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
// a path does not exist.
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
	return fmt.Sprintf("storage %v: path does not exist", e.Path)
}

// FS is an interface which defines a virtual filesystem.
type FS interface {
	Walker

	// Open opens an existing file at path in the filesystem.  Callers must close the
	// File when done to release all underlying resources.
	Open(ctx context.Context, path string) (*File, error)

	// Create makes a new file at path in the filesystem.  Callers must close the
	// returned WriteCloser and check the error to be sure that the file
	// was successfully written.
	Create(ctx context.Context, path string) (io.WriteCloser, error)

	// Delete removes a path from the filesystem.
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

// Prefix creates a FS which wraps fs and prefixes all paths with prefix.
func Prefix(fs FS, prefix string) FS {
	return pfx{
		fs:     fs,
		prefix: prefix,
	}
}

type pfx struct {
	fs     FS
	prefix string
}

func (p pfx) addPrefix(path string) string {
	return fmt.Sprintf("%v%v", p.prefix, path)
}

// Open implements FS.
func (p pfx) Open(ctx context.Context, path string) (*File, error) {
	return p.fs.Open(ctx, p.addPrefix(path))
}

// Create implements FS.
func (p pfx) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return p.fs.Create(ctx, p.addPrefix(path))
}

// Delete implements FS.
func (p pfx) Delete(ctx context.Context, path string) error {
	return p.fs.Delete(ctx, p.addPrefix(path))
}

// Walk transverses all paths underneath path, calling fn on each visited path.
func (p pfx) Walk(ctx context.Context, path string, fn WalkFn) error {
	return p.fs.Walk(ctx, p.addPrefix(path), func(path string) error {
		path = strings.TrimPrefix(path, p.prefix)
		return fn(path)
	})
}
