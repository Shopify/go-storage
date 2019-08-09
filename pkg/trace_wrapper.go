package storage

import (
	"context"
	"io"

	"golang.org/x/net/trace"
)

// NewTraceWrapper creates a new FS which wraps an FS and records calls using
// golang.org/x/net/trace.
func NewTraceWrapper(fs FS, name string) FS {
	return &traceWrapper{
		fs:   fs,
		name: name,
	}
}

// traceWrapper is a FS implementation which wraps an FS and records
// calls using golang.org/x/net/trace.
type traceWrapper struct {
	fs FS

	name string
}

// Open implements FS.  All calls to Open are logged via golang.org/x/net/trace.
func (t *traceWrapper) Open(ctx context.Context, path string, options *ReaderOptions) (f *File, err error) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("%v: open: %v", t.name, path)
		defer func() {
			if err != nil {
				tr.LazyPrintf("%v: error: %v", t.name, err)
				tr.SetError()
			}
		}()
	}
	return t.fs.Open(ctx, path, options)
}

// Attributes() implements FS.  All calls to Attributes() are logged via golang.org/x/net/trace.
func (t *traceWrapper) Attributes(ctx context.Context, path string, options *ReaderOptions) (f *Attributes, err error) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("%v: attrs: %v", t.name, path)
		defer func() {
			if err != nil {
				tr.LazyPrintf("%v: error: %v", t.name, err)
				tr.SetError()
			}
		}()
	}
	return t.fs.Attributes(ctx, path, options)
}

// Create implements FS.  All calls to Create are logged via golang.org/x/net/trace.
func (t *traceWrapper) Create(ctx context.Context, path string, options *WriterOptions) (wc io.WriteCloser, err error) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("%v: create: %v", t.name, path)
		defer func() {
			if err != nil {
				tr.LazyPrintf("%v: error: %v", t.name, err)
				tr.SetError()
			}
		}()
	}
	return t.fs.Create(ctx, path, options)
}

// Delete implements FS.  All calls to Delete are logged via golang.org/x/net/trace.
func (t *traceWrapper) Delete(ctx context.Context, path string) (err error) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("%v: delete: %v", t.name, path)
		defer func() {
			if err != nil {
				tr.LazyPrintf("%v: error: %v", t.name, err)
				tr.SetError()
			}
		}()
	}
	return t.fs.Delete(ctx, path)
}

// Walk implements FS.  Nothing is traced at this time.
func (t *traceWrapper) Walk(ctx context.Context, path string, fn WalkFn) error {
	return t.fs.Walk(ctx, path, fn)
}

func (t *traceWrapper) URL(ctx context.Context, path string, options *SignedURLOptions) (_ string, err error) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("%v: url: %v", t.name, path)
		defer func() {
			if err != nil {
				tr.LazyPrintf("%v: error: %v", t.name, err)
				tr.SetError()
			}
		}()
	}
	return t.fs.URL(ctx, path, options)
}
