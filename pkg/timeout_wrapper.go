package storage

import (
	"context"
	"io"
	"time"
)

// NewTimeoutWrapper creates a FS which wraps fs and adds a timeout to most operations:
// read: Open, Attributes, URL
// write: Create, Delete
//
// Note that the Open and Create methods are only for resolving the object, NOT actually reading or writing the contents.
// These operations should be fairly quick, on the same order as Attribute and Delete, respectively.
//
// This depends on the underlying implementation to honour context's errors.
// It is at least supported on the CloudStorageFS.
//
// Walk is not covered, since its duration is highly unpredictable.
func NewTimeoutWrapper(fs FS, read time.Duration, write time.Duration) FS {
	return &timeoutWrapper{
		fs:    fs,
		read:  read,
		write: write,
	}
}

type timeoutWrapper struct {
	fs    FS
	read  time.Duration
	write time.Duration
}

// Open implements FS.
func (t *timeoutWrapper) Open(ctx context.Context, path string, options *ReaderOptions) (*File, error) {
	ctx, cancel := context.WithTimeout(ctx, t.read)
	defer cancel()
	return t.fs.Open(ctx, path, options)
}

// Attributes() implements FS.
func (t *timeoutWrapper) Attributes(ctx context.Context, path string, options *ReaderOptions) (*Attributes, error) {
	ctx, cancel := context.WithTimeout(ctx, t.read)
	defer cancel()
	return t.fs.Attributes(ctx, path, options)
}

// Create implements FS.
func (t *timeoutWrapper) Create(ctx context.Context, path string, options *WriterOptions) (io.WriteCloser, error) {
	ctx, cancel := context.WithTimeout(ctx, t.write)
	defer cancel()
	return t.fs.Create(ctx, path, options)
}

// Delete implements FS.
func (t *timeoutWrapper) Delete(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, t.write)
	defer cancel()
	return t.fs.Delete(ctx, path)
}

// Walk transverses all paths underneath path, calling fn on each visited path.
func (t *timeoutWrapper) Walk(ctx context.Context, path string, fn WalkFn) error {
	return t.fs.Walk(ctx, path, fn)
}

func (t *timeoutWrapper) URL(ctx context.Context, path string, options *SignedURLOptions) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, t.read)
	defer cancel()
	return t.fs.URL(ctx, path, options)
}
