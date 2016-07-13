package storage

import (
	"io"

	"golang.org/x/net/context"
)

// Cache creates an FS implementation which caches files opened from src into cache.
func Cache(src, cache FS) FS {
	return &cachedFS{
		src:   src,
		cache: cache,
	}
}

type cachedFS struct {
	src, cache FS
}

// Open implements FS.
func (c *cachedFS) Open(ctx context.Context, path string) (*File, error) {
	f, err := c.cache.Open(ctx, path)
	if err == nil {
		return f, nil
	}

	if !IsNotExist(err) {
		return nil, err
	}

	sf, err1 := c.src.Open(ctx, path)
	if err1 != nil {
		return nil, err1
	}
	defer sf.Close()

	wc, err := c.cache.Create(ctx, path)
	if err != nil {
		return nil, err
	}

	if _, err := io.Copy(wc, sf); err != nil {
		wc.Close()
		return nil, err
	}

	if err := wc.Close(); err != nil {
		return nil, err
	}

	ff, err := c.cache.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return ff, nil
}

// Delete implements FS.
func (c *cachedFS) Delete(ctx context.Context, path string) error {
	err := c.cache.Delete(ctx, path)
	if err != nil && !IsNotExist(err) {
		return err
	}
	return c.src.Delete(ctx, path)
}

// Create implements FS.
func (c *cachedFS) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return c.src.Create(ctx, path)
}

// Walk implements FS.
func (c *cachedFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	return c.src.Walk(ctx, path, fn)
}
