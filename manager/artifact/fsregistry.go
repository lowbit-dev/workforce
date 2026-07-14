package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// FsRegistry is a filesystem-backed ArtifactRegistry.
// Binaries are stored at: {dir}/{artifact}/{version}/{os}/{arch}/binary
// Metadata (version info) is stored as a JSON sidecar: {dir}/{artifact}/{version}/meta.json
//
// The Manager's HTTP server serves binaries via GET /artifacts/{task-type}/{version}/{os}/{arch}.
// BaseURL is prepended to construct the download URL returned in Resolve/ResolveVersion.
type FsRegistry struct {
	dir     string // root storage directory
	baseURL string // e.g. "http://manager:8080"

	mu       sync.RWMutex
	versions map[string][]ArtifactVersion // artifact → versions (sorted PublishedAt DESC)
}

// NewFsRegistry creates a new FsRegistry rooted at dir.
// baseURL is the Manager's public base URL used to build artifact download URLs.
func NewFsRegistry(dir, baseURL string) (*FsRegistry, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("fs registry: create dir %q: %w", dir, err)
	}
	r := &FsRegistry{
		dir:      dir,
		baseURL:  strings.TrimRight(baseURL, "/"),
		versions: make(map[string][]ArtifactVersion),
	}
	if err := r.loadAll(); err != nil {
		return nil, err
	}
	return r, nil
}

// Publish stores a new version for the named artifact.
// binaries keys are "os_arch" (e.g. "linux_amd64").
func (r *FsRegistry) Publish(_ context.Context, version ArtifactVersion, binaries map[string]io.Reader) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Detect conflicts before writing any file.
	for key := range binaries {
		os_, arch, err := splitPlatformKey(key)
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		if r.findPlatformLocked(version.Artifact, version.Version, os_, arch) != nil {
			return ErrAlreadyExists
		}
	}

	platforms := make([]ArtifactPlatform, 0, len(binaries))
	for key, reader := range binaries {
		os_, arch, err := splitPlatformKey(key)
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}

		dest := r.binaryPath(version.Artifact, version.Version, os_, arch)
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return fmt.Errorf("publish: mkdir %q: %w", filepath.Dir(dest), err)
		}

		f, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
		if err != nil {
			if os.IsExist(err) {
				return ErrAlreadyExists
			}
			return fmt.Errorf("publish: create binary: %w", err)
		}

		h := sha256.New()
		size, err := io.Copy(io.MultiWriter(f, h), reader)
		f.Close()
		if err != nil {
			os.Remove(dest)
			return fmt.Errorf("publish: write binary: %w", err)
		}

		platforms = append(platforms, ArtifactPlatform{
			Artifact:     version.Artifact,
			Version:      version.Version,
			OS:           os_,
			Arch:         arch,
			Hash:         hex.EncodeToString(h.Sum(nil)),
			Size:         size,
			Dependencies: version.platformDeps(key),
		})
	}

	// Build the full version with all platforms.
	version.Platforms = platforms
	if version.PublishedAt.IsZero() {
		version.PublishedAt = time.Now()
	}

	// Persist metadata.
	if err := r.writeMeta(version); err != nil {
		return err
	}

	// Update in-memory index.
	existing := r.versions[version.Artifact]
	existing = append(existing, version)
	sort.Slice(existing, func(i, j int) bool {
		return existing[i].PublishedAt.After(existing[j].PublishedAt)
	})
	r.versions[version.Artifact] = existing
	return nil
}

// Resolve returns the latest non-pre-release platform build for the given artifact/os/arch.
func (r *FsRegistry) Resolve(_ context.Context, artifact, os_, arch string) (ArtifactPlatform, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, v := range r.versions[artifact] {
		if v.PreRelease {
			continue
		}
		for _, p := range v.Platforms {
			if p.OS == os_ && p.Arch == arch {
				return r.withURL(p), nil
			}
		}
	}
	return ArtifactPlatform{}, ErrNotFound
}

// ResolveVersion returns a specific version including pre-release.
func (r *FsRegistry) ResolveVersion(_ context.Context, artifact, version, os_, arch string) (ArtifactPlatform, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	p := r.findPlatformLocked(artifact, version, os_, arch)
	if p == nil {
		return ArtifactPlatform{}, ErrNotFound
	}
	return r.withURL(*p), nil
}

// ListVersions returns all known versions sorted by PublishedAt descending.
func (r *FsRegistry) ListVersions(_ context.Context, artifact string) ([]ArtifactVersion, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	src := r.versions[artifact]
	out := make([]ArtifactVersion, len(src))
	for i, v := range src {
		cp := v
		// Populate URLs on all platforms.
		for j := range cp.Platforms {
			cp.Platforms[j] = r.withURL(cp.Platforms[j])
		}
		out[i] = cp
	}
	return out, nil
}

// ListPlatforms returns the available platforms for the named artifact.
// If version is non-empty, returns platforms for that exact version.
// If version is empty, returns platforms for the latest non-pre-release version.
// Returns an empty slice (not an error) if no matching version exists.
func (r *FsRegistry) ListPlatforms(_ context.Context, artifact, version string) ([]ArtifactPlatform, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions := r.versions[artifact]
	for _, v := range versions {
		if version != "" {
			if v.Version != version {
				continue
			}
		} else if v.PreRelease {
			continue // skip pre-release when looking for latest
		}
		out := make([]ArtifactPlatform, len(v.Platforms))
		for i, p := range v.Platforms {
			out[i] = r.withURL(p)
		}
		return out, nil
	}
	return nil, nil
}

// DeleteArtifact removes all versions and binaries for the named artifact.
// Returns nil if the artifact does not exist.
func (r *FsRegistry) DeleteArtifact(_ context.Context, artifact string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	dir := filepath.Join(r.dir, artifact)
	if err := os.RemoveAll(dir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs registry: delete artifact %q: %w", artifact, err)
	}
	delete(r.versions, artifact)
	return nil
}

// ServeFile returns the path to the binary file for a given artifact/version/os/arch.
// Used by the Manager's HTTP handler to serve the file directly.
func (r *FsRegistry) ServeFile(artifact, version, os_, arch string) (string, error) {
	path := r.binaryPath(artifact, version, os_, arch)
	if _, err := os.Stat(path); err != nil {
		return "", ErrNotFound
	}
	return path, nil
}

// ---- internal helpers ----

func (r *FsRegistry) binaryPath(artifact, version, os_, arch string) string {
	return filepath.Join(r.dir, artifact, version, os_, arch, "binary")
}

func (r *FsRegistry) metaPath(artifact, version string) string {
	return filepath.Join(r.dir, artifact, version, "meta.json")
}

func (r *FsRegistry) withURL(p ArtifactPlatform) ArtifactPlatform {
	p.URL = fmt.Sprintf("%s/tasks/%s/artifacts/%s/%s/%s",
		r.baseURL, p.Artifact, p.Version, p.OS, p.Arch)
	return p
}

func (r *FsRegistry) findPlatformLocked(artifact, version, os_, arch string) *ArtifactPlatform {
	for i, v := range r.versions[artifact] {
		if v.Version != version {
			continue
		}
		for j, p := range r.versions[artifact][i].Platforms {
			if p.OS == os_ && p.Arch == arch {
				return &r.versions[artifact][i].Platforms[j]
			}
		}
	}
	return nil
}

func (r *FsRegistry) writeMeta(v ArtifactVersion) error {
	path := r.metaPath(v.Artifact, v.Version)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("fs registry: writeMeta mkdir: %w", err)
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Errorf("fs registry: marshal meta: %w", err)
	}
	return os.WriteFile(path, data, 0o600)
}

// loadAll scans the registry directory and populates the in-memory index.
func (r *FsRegistry) loadAll() error {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("fs registry: read dir: %w", err)
	}

	for _, artifactEntry := range entries {
		if !artifactEntry.IsDir() {
			continue
		}
		artifact := artifactEntry.Name()

		versionEntries, err := os.ReadDir(filepath.Join(r.dir, artifact))
		if err != nil {
			return fmt.Errorf("fs registry: read artifact dir %q: %w", artifact, err)
		}

		for _, ve := range versionEntries {
			if !ve.IsDir() {
				continue
			}
			version := ve.Name()
			meta, err := r.readMeta(artifact, version)
			if err != nil {
				continue // skip corrupt entries
			}
			r.versions[artifact] = append(r.versions[artifact], *meta)
		}

		sort.Slice(r.versions[artifact], func(i, j int) bool {
			return r.versions[artifact][i].PublishedAt.After(r.versions[artifact][j].PublishedAt)
		})
	}
	return nil
}

func (r *FsRegistry) readMeta(artifact, version string) (*ArtifactVersion, error) {
	data, err := os.ReadFile(r.metaPath(artifact, version))
	if err != nil {
		return nil, err
	}
	var v ArtifactVersion
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func splitPlatformKey(key string) (os_, arch string, err error) {
	idx := strings.Index(key, "_")
	if idx < 1 || idx == len(key)-1 {
		return "", "", fmt.Errorf("invalid platform key %q: expected os_arch format", key)
	}
	return key[:idx], key[idx+1:], nil
}
