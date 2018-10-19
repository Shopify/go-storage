package storage

import (
	"context"
	"expvar"
	"io"
)

// NewErrCountFS creates an FS which records stats based on usage.
func NewErrCountFS(fs FS, name string, err error) FS {
	status := expvar.NewMap(name)
	status.Set("open.total", new(expvar.Int))
	status.Set("open.count", new(expvar.Int))

	status.Set("create.total", new(expvar.Int))
	status.Set("create.count", new(expvar.Int))

	status.Set("delete.total", new(expvar.Int))
	status.Set("delete.count", new(expvar.Int))

	return &errCountFS{
		fs:     fs,
		status: status,
		err:    err,
	}
}

// errCountFS is an FS which records error counts for an FS.
type errCountFS struct {
	fs FS

	err    error
	status *expvar.Map
}

// Open implements FS.  All errors from Open are counted.
func (s *errCountFS) Open(ctx context.Context, path string) (*File, error) {
	f, err := s.fs.Open(ctx, path)
	if err == s.err {
		s.status.Add("open.count", 1)
	}
	s.status.Add("open.total", 1)
	return f, err
}

// Create implements FS.  All errors from Create are counted.
func (s *errCountFS) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	wc, err := s.fs.Create(ctx, path)
	if err == s.err {
		s.status.Add("create.count", 1)
	}
	s.status.Add("create.total", 1)
	return wc, err
}

// Delete implements FS.  All errors from Delete are counted.
func (s *errCountFS) Delete(ctx context.Context, path string) error {
	err := s.fs.Delete(ctx, path)
	if err == s.err {
		s.status.Add("delete.count", 1)
	}
	s.status.Add("delete.total", 1)
	return err
}

// Walk implements FS.  No stats are recorded at this time.
func (s *errCountFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	return s.fs.Walk(ctx, path, fn)
}
