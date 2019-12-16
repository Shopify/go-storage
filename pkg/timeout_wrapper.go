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

// timeoutCall watches the context to be sure it's not Done yet,
// but does NOT modify the context being passed to the underlying call.
// This is important, because the context needs to continue to be alive while the returned object (File, Writer, etc)
// is being used by the caller.
func timeoutCall(ctx context.Context, timeout time.Duration, call func() error) (err error) {
	done := make(chan error)
	go func() {
		done <- call()
	}()

	select {
	case <-time.After(timeout):
		return context.DeadlineExceeded
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

// Open implements FS.
func (t *timeoutWrapper) Open(ctx context.Context, path string, options *ReaderOptions) (file *File, err error) {
	return file, timeoutCall(ctx, t.read, func() error {
		file, err = t.fs.Open(ctx, path, options)
		return err
	})
}

// Attributes() implements FS.
func (t *timeoutWrapper) Attributes(ctx context.Context, path string, options *ReaderOptions) (attrs *Attributes, err error) {
	return attrs, timeoutCall(ctx, t.read, func() error {
		attrs, err = t.fs.Attributes(ctx, path, options)
		return err
	})
}

// Create implements FS.
func (t *timeoutWrapper) Create(ctx context.Context, path string, options *WriterOptions) (w io.WriteCloser, err error) {
	return w, timeoutCall(ctx, t.write, func() error {
		w, err = t.fs.Create(ctx, path, options)
		return err
	})
}

// Delete implements FS.
func (t *timeoutWrapper) Delete(ctx context.Context, path string) error {
	return timeoutCall(ctx, t.write, func() error {
		return t.fs.Delete(ctx, path)
	})
}

// Walk transverses all paths underneath path, calling fn on each visited path.
func (t *timeoutWrapper) Walk(ctx context.Context, path string, fn WalkFn) error {
	return t.fs.Walk(ctx, path, fn)
}

func (t *timeoutWrapper) URL(ctx context.Context, path string, options *SignedURLOptions) (url string, err error) {
	return url, timeoutCall(ctx, t.write, func() error {
		url, err = t.fs.URL(ctx, path, options)
		return err
	})
}
