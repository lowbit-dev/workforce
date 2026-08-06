// Package cache is a content-addressed local disk cache for files downloaded from URLs.
// Files are keyed by SHA-256 hash. Eviction is governed by MaxSize and TTL.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"lowbit.dev/workforce/worker/sf"
)

// ArtifactCache is a local disk cache for content-addressed files downloaded from URLs.
type ArtifactCache struct {
	dir      string
	maxSize  int64
	ttl      time.Duration
	logger   *slog.Logger
	inFlight sf.Group

	mu      sync.Mutex
	entries map[string]*entry
	total   int64
}

type entry struct {
	path     string
	size     int64
	lastUsed time.Time
}

// New creates (or opens) a cache rooted at dir with the given limits.
// maxSize 0 means no size limit. ttl 0 means no TTL eviction.
func NewArtifactCache(dir string, maxSize int64, ttl time.Duration, logger *slog.Logger) (*ArtifactCache, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("cache: create dir %q: %w", dir, err)
	}

	if logger == nil {
		logger = slog.Default()
	}

	return &ArtifactCache{
		dir:     dir,
		maxSize: maxSize,
		ttl:     ttl,
		logger:  logger,
		entries: make(map[string]*entry),
	}, nil
}

// Fetch returns the local path to the file identified by hash, downloading it from url if
// needed. hash must be a hex-encoded SHA-256 digest. Returns an error if the download or
// integrity check fails.
func (c *ArtifactCache) Fetch(ctx context.Context, hash, url string) (string, error) {
	if path := c.get(hash); path != "" {
		return path, nil
	}

	v, err, _ := c.inFlight.Do(hash, func() (any, error) {
		return c.Obtain(ctx, hash, url)
	})

	return v.(string), err
}

func (c *ArtifactCache) Obtain(ctx context.Context, hash, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("cache: build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("cache: fetch %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("cache: fetch %s: unexpected status %d", url, resp.StatusCode)
	}

	tmp, err := os.CreateTemp(c.dir, "dl-*")
	if err != nil {
		return "", fmt.Errorf("cache: create temp: %w", err)
	}

	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(tmp, h), resp.Body)
	tmp.Close()
	if err != nil {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("cache: download: %w", err)
	}

	if got := hex.EncodeToString(h.Sum(nil)); hash != "" && got != hash {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("cache: integrity check failed (want %s, got %s)", hash, got)
	}

	name := hash
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	dest := filepath.Join(c.dir, name)

	if err := os.Rename(tmp.Name(), dest); err != nil {
		os.Remove(tmp.Name())
		return "", fmt.Errorf("cache: rename: %w", err)
	}
	if err := os.Chmod(dest, 0o700); err != nil {
		return "", fmt.Errorf("cache: chmod: %w", err)
	}

	c.mu.Lock()
	c.entries[hash] = &entry{path: dest, size: size, lastUsed: time.Now()}
	c.total += size
	c.evictLocked()
	c.mu.Unlock()

	return dest, nil
}

func (c *ArtifactCache) get(hash string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[hash]
	if !ok {
		return ""
	}
	if c.ttl > 0 && time.Since(e.lastUsed) > c.ttl {
		c.removeLocked(hash)
		return ""
	}
	e.lastUsed = time.Now()
	return e.path
}

// evictLocked removes the least-recently-used entries until total bytes <= maxSize.
// Must be called with c.mu held.
func (c *ArtifactCache) evictLocked() {
	if c.maxSize <= 0 || c.total <= c.maxSize {
		return
	}
	type kv struct {
		hash     string
		lastUsed time.Time
	}
	items := make([]kv, 0, len(c.entries))
	for h, e := range c.entries {
		items = append(items, kv{h, e.lastUsed})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].lastUsed.Before(items[j].lastUsed)
	})
	for _, item := range items {
		if c.total <= c.maxSize {
			break
		}
		c.removeLocked(item.hash)
	}
}

func (c *ArtifactCache) removeLocked(hash string) {
	e, ok := c.entries[hash]
	if !ok {
		return
	}
	if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
		c.logger.Warn("cache: remove file", "hash", hash, "error", err)
	}
	c.total -= e.size
	delete(c.entries, hash)
}
