package storage

import (
	"context"
	"io"

	"golang.org/x/net/trace"
)

// NewTraceFS creates a new FS which wraps an FS and records calls using
// golang.org/x/net/trace.
func NewTraceFS(fs FS, name string) FS {
	return &traceFS{
		fs:   fs,
		name: name,
	}
}

// traceFS is a FS implementation which wraps an FS and records
// calls using golang.org/x/net/trace.
type traceFS struct {
	fs FS

	name string
}

// Open implements FS.  All calls to Open are logged via golang.org/x/net/trace.
func (t *traceFS) Open(ctx context.Context, path string) (f *File, err error) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("%v: open: %v", t.name, path)
		defer func() {
			if err != nil {
				tr.LazyPrintf("%v: error: %v", t.name, err)
				tr.SetError()
			}
		}()
	}
	return t.fs.Open(ctx, path)
}

// Create implements FS.  All calls to Create are logged via golang.org/x/net/trace.
func (t *traceFS) Create(ctx context.Context, path string) (wc io.WriteCloser, err error) {
	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("%v: create: %v", t.name, path)
		defer func() {
			if err != nil {
				tr.LazyPrintf("%v: error: %v", t.name, err)
				tr.SetError()
			}
		}()
	}
	return t.fs.Create(ctx, path)
}

// Delete implements FS.  All calls to Delete are logged via golang.org/x/net/trace.
func (t *traceFS) Delete(ctx context.Context, path string) (err error) {
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
func (t *traceFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	return t.fs.Walk(ctx, path, fn)
}
