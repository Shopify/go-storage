package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Shopify/go-storage/pkg"
)

func withCache(options *storage.CacheOptions, cb func(fs storage.FS, src storage.FS, cache storage.FS)) {
	withLocal(func(local storage.FS) {
		withMem(func(mem storage.FS) {
			fs := storage.NewCacheWrapper(local, mem, options)
			cb(fs, local, mem)
		})
	})
}

func TestCacheWrapper_Open(t *testing.T) {
	withCache(nil, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testOpenNotExists(t, fs, "foo")
	})
}

func TestCacheWrapper_Create(t *testing.T) {
	withCache(nil, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testCreate(t, fs, "foo", "")
		testOpenExists(t, src, "foo", "")
		testOpenExists(t, cache, "foo", "")

		testCreate(t, fs, "foo", "bar")
		testOpenExists(t, src, "foo", "bar")
		testOpenExists(t, cache, "foo", "bar")
	})
}

func TestCacheWrapper_Delete(t *testing.T) {
	withCache(nil, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testDelete(t, fs, "foo")
	})
}

func TestCacheWrapper_CacheOptions_MaxAge(t *testing.T) {
	options := &storage.CacheOptions{
		MaxAge: 500 * time.Millisecond,
	}

	withCache(options, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testCreate(t, fs, "foo", "")

		ctx := context.Background()
		f, err := fs.Open(ctx, "foo")
		assert.NoError(t, err)
		assert.NotZero(t, f)
		assert.NotZero(t, f.ModTime)
		assert.False(t, time.Since(f.ModTime) > options.MaxAge, "file should not be expired")

		<-time.After(options.MaxAge)

		_, err = fs.Open(ctx, "foo")
		assert.Errorf(t, err, "storage foo: path exists, but is expired")

		f, err = cache.Open(ctx, "foo")
		assert.Errorf(t, err, "storage foo: path does not exist")

		// Wrapper still reports expired
		_, err = fs.Open(ctx, "foo")
		assert.Errorf(t, err, "storage foo: path exists, but is expired")
	})
}
