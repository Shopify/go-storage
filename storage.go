// Package storage provides types and functionality for abstracting storage systems (Local, in memory, S3, Google Cloud
// storage) into a common interface.
package storage

import (
	"fmt"
	"io"
	"strings"
	"time"

	"golang.org/x/net/context"
)

// File contains the metadata required to define a file (for reading).
type File struct {
	io.ReadCloser           // Underlying data.
	Name          string    // Name of the file (likely basename).
	ModTime       time.Time // Modified time of the file.
	Size          int64     // Size of the file.
}

// isNotExister is an interface used to define the behaviour of errors resulting
// from operations which report missing files/paths.
type isNotExister interface {
	isNotExist() bool
}

// IsNotExist returns a boolean indicating whether the error is known to report that
// a path.
func IsNotExist(err error) bool {
	e, ok := err.(isNotExister)
	return ok && e.isNotExist()
}

// notExistError is returned from FS.Open implementations when a requested
// path does not exist.
type notExistError struct {
	Path string
}

func (e *notExistError) isNotExist() bool { return true }

// Error implements error
func (e *notExistError) Error() string {
	return fmt.Sprintf("storage %v: file does not exist", e.Path)
}

// FS is an interface which defines a virtual filesystem.
type FS interface {
	Walker

	// Open opens an existing file in the filesystem.
	Open(ctx context.Context, path string) (*File, error)

	// Create makes a new file in the filesystem.  Callers must close the
	// returned WriteCloser and check the error to be sure that the file
	// was successfully written.
	Create(ctx context.Context, path string) (io.WriteCloser, error)

	// Delete removes a file from the filesystem.
	Delete(ctx context.Context, path string) error
}

// FSFromURL takes a file system path and returns a FSWalker
// corresponding to a supported storage system (CloudStorage,
// S3, or Local if no platform-specific prefix is used).
func FSFromURL(path string) FS {
	if strings.HasPrefix(path, "gs://") {
		return &CloudStorage{Bucket: strings.TrimPrefix(path, "gs://")}
	}
	if strings.HasPrefix(path, "s3://") {
		return &S3{Bucket: strings.TrimPrefix(path, "s3://")}
	}
	return Local(path)
}

// PrefixFS creates a FS which wraps fs and prefixes all paths with prefix.
func PrefixFS(fs FS, prefix string) FS {
	return prefixFS{
		prefix: prefix,
		fs:     fs,
	}
}

type prefixFS struct {
	prefix string
	fs     FS
}

func (pfs prefixFS) addPrefix(path string) string {
	return fmt.Sprintf("%v%v", pfs.prefix, path)
}

// Open implements FS.
func (pfs prefixFS) Open(ctx context.Context, path string) (*File, error) {
	return pfs.fs.Open(ctx, pfs.addPrefix(path))
}

// Create implements FS.
func (pfs prefixFS) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return pfs.fs.Create(ctx, pfs.addPrefix(path))
}

// Delete implements FS.
func (pfs prefixFS) Delete(ctx context.Context, path string) error {
	return pfs.fs.Delete(ctx, pfs.addPrefix(path))
}

// Walk implements FS.
func (pfs prefixFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	return pfs.fs.Walk(ctx, pfs.addPrefix(path), func(path string) error {
		path = strings.TrimPrefix(path, pfs.prefix)
		return fn(path)
	})
}
