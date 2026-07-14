package artifact

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// SeedEntry describes a single artifact version to publish from local files.
// Binaries maps "os_arch" keys (e.g. "darwin_arm64") to local file paths.
// Dependencies maps the same "os_arch" keys to lists of host binary names required in $PATH.
type SeedEntry struct {
	Name         string
	Version      string
	PreRelease   bool
	Binaries     map[string]string   // "os_arch" → local file path
	Dependencies map[string][]string // "os_arch" → host binaries required in $PATH (optional)
}

// Seed publishes a set of local binaries into registry.
// It is intended for local development setups where uploading via HTTP is unnecessary friction.
//
// Each entry in seeds is passed to registry.Publish. Entries where the exact
// (artifact, version, os, arch) tuple already exists are silently skipped —
// so Seed is safe to call on every startup without version-bumping.
//
// Returns the first non-ErrAlreadyExists error encountered, or nil if all
// entries were published or already present.
func Seed(ctx context.Context, registry ArtifactRegistry, seeds []SeedEntry) error {
	for _, entry := range seeds {
		if err := seedOne(ctx, registry, entry); err != nil {
			return err
		}
	}
	return nil
}

func seedOne(ctx context.Context, registry ArtifactRegistry, entry SeedEntry) error {
	// Open all files first so we can bail before touching the registry on any open error.
	files := make([]*os.File, 0, len(entry.Binaries))
	binaries := make(map[string]io.Reader, len(entry.Binaries))

	for key, path := range entry.Binaries {
		f, err := os.Open(path)
		if err != nil {
			for _, opened := range files {
				opened.Close()
			}
			return fmt.Errorf("seed %q %s: open %q: %w", entry.Name, entry.Version, path, err)
		}
		files = append(files, f)
		binaries[key] = f
	}

	defer func() {
		for _, f := range files {
			f.Close()
		}
	}()

	version := ArtifactVersion{
		Artifact:     entry.Name,
		Version:      entry.Version,
		PreRelease:   entry.PreRelease,
		PublishedAt:  time.Now(),
		PlatformDeps: entry.Dependencies,
	}

	err := registry.Publish(ctx, version, binaries)
	if errors.Is(err, ErrAlreadyExists) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("seed %q %s: %w", entry.Name, entry.Version, err)
	}
	return nil
}
