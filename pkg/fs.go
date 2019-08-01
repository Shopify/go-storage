// Package storage provides types and functionality for abstracting storage systems
// (local, in memory, S3, Google Cloud storage) into a common interface.
package storage

import (
	"context"
	"io"
	"time"

	"github.com/google/go-cloud/blob"
)

// File contains the metadata required to define a file (for reading).
type File struct {
	io.ReadCloser // Underlying data.
	Attributes
}

// Attributes represents the metadata of a File
// Inspired from github.com/google/go-cloud/blob.Attributes
type Attributes struct {
	// ContentType is the MIME type of the blob object. It will not be empty.
	ContentType string
	// Metadata holds key/value pairs associated with the blob.
	// Keys are guaranteed to be in lowercase, even if the backend provider
	// has case-sensitive keys (although note that Metadata written via
	// this package will always be lowercased). If there are duplicate
	// case-insensitive keys (e.g., "foo" and "FOO"), only one value
	// will be kept, and it is undefined which one.
	Metadata map[string]string
	// ModTime is the time the blob object was last modified.
	ModTime time.Time
	// Size is the size of the object in bytes.
	Size int64
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

	// URL resolves a path to an addressable URL
	URL(ctx context.Context, path string, options *SignedURLOptions) (string, error)
}

type SignedURLOptions blob.SignedURLOptions
