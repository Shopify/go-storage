package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// NewPrefixWrapper creates a FS which wraps fs and prefixes all paths with prefix.
func NewPrefixWrapper(fs FS, prefix string) FS {
	return &prefixWrapper{
		fs:     fs,
		prefix: prefix,
	}
}

type prefixWrapper struct {
	fs     FS
	prefix string
}

func (p *prefixWrapper) addPrefix(path string) string {
	return fmt.Sprintf("%v%v", p.prefix, path)
}

// Open implements FS.
func (p *prefixWrapper) Open(ctx context.Context, path string) (*File, error) {
	return p.fs.Open(ctx, p.addPrefix(path))
}

// Create implements FS.
func (p *prefixWrapper) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return p.fs.Create(ctx, p.addPrefix(path))
}

// Delete implements FS.
func (p *prefixWrapper) Delete(ctx context.Context, path string) error {
	return p.fs.Delete(ctx, p.addPrefix(path))
}

// Walk transverses all paths underneath path, calling fn on each visited path.
func (p *prefixWrapper) Walk(ctx context.Context, path string, fn WalkFn) error {
	return p.fs.Walk(ctx, p.addPrefix(path), func(path string) error {
		path = strings.TrimPrefix(path, p.prefix)
		return fn(path)
	})
}
