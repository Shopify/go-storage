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
	io.ReadCloser           // Underlying data.
	Name          string    // Name of the file (likely basename).
	ModTime       time.Time // Modified time of the file.
	Size          int64     // Size of the file.
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
