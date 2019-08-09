package storage

import (
	"context"
	"io"
	"time"
)

type CacheOptions struct {
	// MaxAge is the maximum time allowed since the underlying File's ModTime
	// This means that if the cache is older than MaxAge, the Cache will fetch from the src again.
	// If the expired File is still present on the src (i.e. not updated), it will be ignored.
	MaxAge time.Duration

	// DefaultExpired makes the cache treat a File as expired if its ModTime cannot be checked.
	// By default, it is false, which means the cache will treat zero-ModTime files as valid.
	// Only useful if MaxAge is set.
	DefaultExpired bool
}

// NewCacheWrapper creates an FS implementation which caches files opened from src into cache.
func NewCacheWrapper(src, cache FS, options *CacheOptions) FS {
	if options == nil {
		options = &CacheOptions{}
	}

	return &cacheWrapper{
		src:     src,
		cache:   cache,
		options: options,
	}
}

type cacheWrapper struct {
	src     FS
	cache   FS
	options *CacheOptions
}

func (c *cacheWrapper) isExpired(file *File) bool {
	if c.options.MaxAge == 0 {
		// No expiration behavior
		return false
	}

	if file.ModTime.IsZero() {
		// Unable to check the File's ModTime
		return c.options.DefaultExpired
	}

	return time.Since(file.ModTime) > c.options.MaxAge
}

// Open implements FS.
func (c *cacheWrapper) Open(ctx context.Context, path string, options *ReaderOptions) (*File, error) {
	f, err := c.cache.Open(ctx, path, options)
	if err == nil {
		if c.isExpired(f) {
			err = &expiredError{Path: path}
		} else {
			return f, nil
		}
	}

	if !IsNotExist(err) {
		return nil, err
	}

	sf, err := c.src.Open(ctx, path, options)
	if err != nil {
		return nil, err
	}
	defer sf.Close()

	if c.isExpired(sf) {
		// Cleanup expire entry, but don't care for deletion errors
		c.cache.Delete(ctx, path)

		return nil, &expiredError{Path: path}
	}

	cacheAttrs := sf.Attributes
	wc, err := c.cache.Create(ctx, path, &WriterOptions{
		Attributes: cacheAttrs,
	})
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

	ff, err := c.cache.Open(ctx, path, options)
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
func (c *cacheWrapper) Create(ctx context.Context, path string, options *WriterOptions) (io.WriteCloser, error) {
	err := c.cache.Delete(ctx, path)
	if err != nil && !IsNotExist(err) {
		return nil, err
	}
	return c.src.Create(ctx, path, options)
}

// Walk implements FS.
func (c *cacheWrapper) Walk(ctx context.Context, path string, fn WalkFn) error {
	return c.src.Walk(ctx, path, fn)
}

func (c *cacheWrapper) URL(ctx context.Context, path string, options *SignedURLOptions) (string, error) {
	// Pass-through
	return c.src.URL(ctx, path, options)
}
