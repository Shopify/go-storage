package storage

import (
	"context"
	"io"
)

// NewCacheWrapper creates an FS implementation which caches files opened from src into cache.
func NewCacheWrapper(src, cache FS) FS {
	return &cacheWrapper{
		src:   src,
		cache: cache,
	}
}

type cacheWrapper struct {
	src   FS
	cache FS
}

// Open implements FS.
func (c *cacheWrapper) Open(ctx context.Context, path string) (*File, error) {
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
func (c *cacheWrapper) Delete(ctx context.Context, path string) error {
	err := c.cache.Delete(ctx, path)
	if err != nil && !IsNotExist(err) {
		return err
	}
	return c.src.Delete(ctx, path)
}

// Create implements FS.
func (c *cacheWrapper) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	err := c.cache.Delete(ctx, path)
	if err != nil && !IsNotExist(err) {
		return nil, err
	}
	return c.src.Create(ctx, path)
}

// Walk implements FS.
func (c *cacheWrapper) Walk(ctx context.Context, path string, fn WalkFn) error {
	return c.src.Walk(ctx, path, fn)
}

func (c *cacheWrapper) URL(ctx context.Context, path string, options *URLOptions) (string, error) {
	// Pass-through
	return c.src.URL(ctx, path, options)
}
