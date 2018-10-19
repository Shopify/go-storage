package storage

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// NewPrefixFS creates a FS which wraps fs and prefixes all paths with prefix.
func NewPrefixFS(fs FS, prefix string) FS {
	return &pfx{
		fs:     fs,
		prefix: prefix,
	}
}

type pfx struct {
	fs     FS
	prefix string
}

func (p *pfx) addPrefix(path string) string {
	return fmt.Sprintf("%v%v", p.prefix, path)
}

// Open implements FS.
func (p *pfx) Open(ctx context.Context, path string) (*File, error) {
	return p.fs.Open(ctx, p.addPrefix(path))
}

// Create implements FS.
func (p *pfx) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return p.fs.Create(ctx, p.addPrefix(path))
}

// Delete implements FS.
func (p *pfx) Delete(ctx context.Context, path string) error {
	return p.fs.Delete(ctx, p.addPrefix(path))
}

// Walk transverses all paths underneath path, calling fn on each visited path.
func (p *pfx) Walk(ctx context.Context, path string, fn WalkFn) error {
	return p.fs.Walk(ctx, p.addPrefix(path), func(path string) error {
		path = strings.TrimPrefix(path, p.prefix)
		return fn(path)
	})
}
