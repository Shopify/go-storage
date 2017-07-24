package storage

import (
	"expvar"
	"io"
	"log"

	"golang.org/x/net/context"
	"golang.org/x/net/trace"
)

// NewLogFS creates a new FS which logs all calls to FS.
func NewLogFS(fs FS, name string, l *log.Logger) FS {
	return &LogFS{
		fs:     fs,
		name:   name,
		logger: l,
	}
}

// LogFS is an FS implementation which logs all filesystem calls.
type LogFS struct {
	fs FS

	name   string
	logger *log.Logger
}

// Open implements FS.  All calls to Open are logged and errors are logged seperately.
func (l *LogFS) Open(ctx context.Context, path string) (*File, error) {
	l.logger.Printf("%v: open: %v", l.name, path)
	f, err := l.fs.Open(ctx, path)
	if err != nil {
		l.logger.Printf("%v: open error: %v: %v", l.name, path, err)
	}
	return f, err
}

// Create implements FS.  All calls to Create are logged and errors are logged seperately.
func (l *LogFS) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	l.logger.Printf("%v: create: %v", l.name, path)
	wc, err := l.fs.Create(ctx, path)
	if err != nil {
		l.logger.Printf("%v: create error: %v: %v", l.name, path, err)
	}
	return wc, err
}

// Delete implements FS.  All calls to Delete are logged and errors are logged seperately.
func (l *LogFS) Delete(ctx context.Context, path string) error {
	l.logger.Printf("%v: delete: %v", l.name, path)
	err := l.fs.Delete(ctx, path)
	if err != nil {
		l.logger.Printf("%v: delete error: %v: %v", l.name, path, err)
	}
	return err
}

// Walk implements FS.  No logs are written at this time.
func (l *LogFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	return l.fs.Walk(ctx, path, fn)
}

// NewTraceFS creates a new FS which wraps an FS and records calls using
// golang.org/x/net/trace.
func NewTraceFS(fs FS, name string) FS {
	return &TraceFS{
		fs:   fs,
		name: name,
	}
}

// TraceFS is a FS implementation which wraps an FS and records
// calls using golang.org/x/net/trace.
type TraceFS struct {
	fs FS

	name string
}

// Open implements FS.  All calls to Open are logged via golang.org/x/net/trace.
func (t *TraceFS) Open(ctx context.Context, path string) (f *File, err error) {
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
func (t *TraceFS) Create(ctx context.Context, path string) (wc io.WriteCloser, err error) {
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
func (t *TraceFS) Delete(ctx context.Context, path string) (err error) {
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
func (t *TraceFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	return t.fs.Walk(ctx, path, fn)
}

// NewErrCountFS creates an FS which records stats based on usage.
func NewErrCountFS(fs FS, name string, err error) FS {
	status := expvar.NewMap(name)
	status.Set("open.total", new(expvar.Int))
	status.Set("open.count", new(expvar.Int))

	status.Set("create.total", new(expvar.Int))
	status.Set("create.count", new(expvar.Int))

	status.Set("delete.total", new(expvar.Int))
	status.Set("delete.count", new(expvar.Int))

	return &ErrCountFS{
		fs:     fs,
		status: status,
		err:    err,
	}
}

// ErrCountFS is an FS which records error counts for an FS.
type ErrCountFS struct {
	fs FS

	err    error
	status *expvar.Map
}

// Open implements FS.  All errors from Open are counted.
func (s ErrCountFS) Open(ctx context.Context, path string) (*File, error) {
	f, err := s.fs.Open(ctx, path)
	if err == s.err {
		s.status.Add("open.count", 1)
	}
	s.status.Add("open.total", 1)
	return f, err
}

// Create implements FS.  All errors from Create are counted.
func (s ErrCountFS) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	wc, err := s.fs.Create(ctx, path)
	if err == s.err {
		s.status.Add("create.count", 1)
	}
	s.status.Add("create.total", 1)
	return wc, err
}

// Delete implements FS.  All errors from Delete are counted.
func (s ErrCountFS) Delete(ctx context.Context, path string) error {
	err := s.fs.Delete(ctx, path)
	if err == s.err {
		s.status.Add("delete.count", 1)
	}
	s.status.Add("delete.total", 1)
	return err
}

// Walk implements FS.  No stats are recorded at this time.
func (s ErrCountFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	return s.fs.Walk(ctx, path, fn)
}
