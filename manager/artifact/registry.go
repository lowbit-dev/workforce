// Package artifact defines the ArtifactRegistry interface and related types used
// internally by the Manager. The Worker never calls registry methods directly —
// it only receives the resolved ArtifactInfo inside a TYPE_PROPOSE_JOB packet.
package artifact

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound is returned by Resolve and ResolveVersion when no matching artifact exists.
var ErrNotFound = errors.New("artifact not found")

// ErrAlreadyExists is returned by Publish when the exact (artifact, version, os, arch) tuple
// already exists. Published versions are immutable.
var ErrAlreadyExists = errors.New("artifact version already exists")

// ArtifactPlatform is the metadata and download URL for one OS/arch build of an artifact.
type ArtifactPlatform struct {
	Artifact     string
	Version      string
	OS           string
	Arch         string
	Hash         string // SHA-256 hex of the binary
	Size         int64
	Dependencies []string // host binaries required in $PATH for this platform binary
	// URL is populated at serve time; workers use this to download the binary.
	// The FsRegistry sets this when returning from Resolve/ResolveVersion.
	URL string
}

// ArtifactVersion groups all platform builds for one version of a task binary.
type ArtifactVersion struct {
	Artifact    string
	Version     string
	PreRelease  bool      // if true, Resolve (latest) skips this version
	PublishedAt time.Time // used for "latest" resolution; highest PublishedAt among non-pre-release wins
	Platforms   []ArtifactPlatform
	// PlatformDeps carries per-platform dependency lists for use during Publish.
	// Keys are "os_arch" (e.g. "linux_amd64"). Values are host binary names required in $PATH.
	// This field is not stored in meta.json; dependencies are stored on each ArtifactPlatform instead.
	PlatformDeps map[string][]string `json:"-"`
}

// platformDeps returns the dependency list for the given os_arch key, or nil if none.
func (v ArtifactVersion) platformDeps(key string) []string {
	if v.PlatformDeps == nil {
		return nil
	}
	return v.PlatformDeps[key]
}

// FileServer is an optional interface that ArtifactRegistry implementations may satisfy
// to allow the Manager to serve binary files directly via http.ServeFile rather than
// redirecting to an external URL.
type FileServer interface {
	// ServeFile returns the absolute filesystem path to the binary for the given
	// artifact/version/os/arch. Returns ErrNotFound if no such binary exists.
	ServeFile(artifact, version, os, arch string) (string, error)
}

// ArtifactRegistry stores and resolves versioned, per-platform task binaries.
type ArtifactRegistry interface {
	// Publish stores a new version for the named artifact.
	// Returns ErrAlreadyExists if the exact (artifact, version, os, arch) tuple already exists.
	// binaries map key is "os_arch" (e.g. "linux_amd64"); split on "_" to extract OS and Arch.
	Publish(ctx context.Context, version ArtifactVersion, binaries map[string]io.Reader) error

	// Resolve returns the latest non-pre-release platform build for the given artifact/os/arch.
	// "Latest" is the version with the highest PublishedAt where PreRelease == false.
	// Returns ErrNotFound if no released version exists.
	Resolve(ctx context.Context, artifact, os, arch string) (ArtifactPlatform, error)

	// ResolveVersion returns a specific version, including pre-release versions.
	// Returns ErrNotFound if the exact (artifact, version, os, arch) tuple does not exist.
	ResolveVersion(ctx context.Context, artifact, version, os, arch string) (ArtifactPlatform, error)

	// ListVersions returns all known versions for the named artifact, sorted by PublishedAt descending.
	ListVersions(ctx context.Context, artifact string) ([]ArtifactVersion, error)

	// ListPlatforms returns the set of available platforms for the named artifact.
	// If version is non-empty, it returns platforms for that exact version.
	// If version is empty, it returns the platforms available in the latest non-pre-release version.
	// Returns an empty slice (not an error) if no matching versions exist.
	ListPlatforms(ctx context.Context, artifact, version string) ([]ArtifactPlatform, error)

	// DeleteArtifact removes all versions and platform binaries for the named artifact.
	// Returns nil if the artifact does not exist.
	DeleteArtifact(ctx context.Context, artifact string) error
}
