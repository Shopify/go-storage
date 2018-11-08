package storage

import (
	"context"
	"io"
	"log"
)

// NewLoggerWrapper creates a new FS which logs all calls to FS.
func NewLoggerWrapper(fs FS, name string, l *log.Logger) FS {
	return &loggerWrapper{
		fs:     fs,
		name:   name,
		logger: l,
	}
}

// loggerWrapper is an FS implementation which logs all filesystem calls.
type loggerWrapper struct {
	fs FS

	name   string
	logger *log.Logger
}

// Open implements FS.  All calls to Open are logged and errors are logged separately.
func (l *loggerWrapper) Open(ctx context.Context, path string) (*File, error) {
	l.logger.Printf("%v: open: %v", l.name, path)
	f, err := l.fs.Open(ctx, path)
	if err != nil {
		l.logger.Printf("%v: open error: %v: %v", l.name, path, err)
	}
	return f, err
}

// Create implements FS.  All calls to Create are logged and errors are logged separately.
func (l *loggerWrapper) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	l.logger.Printf("%v: create: %v", l.name, path)
	wc, err := l.fs.Create(ctx, path)
	if err != nil {
		l.logger.Printf("%v: create error: %v: %v", l.name, path, err)
	}
	return wc, err
}

// Delete implements FS.  All calls to Delete are logged and errors are logged separately.
func (l *loggerWrapper) Delete(ctx context.Context, path string) error {
	l.logger.Printf("%v: delete: %v", l.name, path)
	err := l.fs.Delete(ctx, path)
	if err != nil {
		l.logger.Printf("%v: delete error: %v: %v", l.name, path, err)
	}
	return err
}

// Walk implements FS.  No logs are written at this time.
func (l *loggerWrapper) Walk(ctx context.Context, path string, fn WalkFn) error {
	return l.fs.Walk(ctx, path, fn)
}

func (l *loggerWrapper) URL(ctx context.Context, path string, options *SignedURLOptions) (string, error) {
	l.logger.Printf("%v: URL: %v", l.name, path)
	url, err := l.fs.URL(ctx, path, options)
	if err != nil {
		l.logger.Printf("%v: URL error: %v: %v", l.name, path, err)
	}
	return url, err

}
