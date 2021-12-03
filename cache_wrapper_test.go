package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Shopify/go-storage"
	"github.com/Shopify/go-storage/internal/testutils"
)

func withCache(options *storage.CacheOptions, cb func(fs storage.FS, src storage.FS, cache storage.FS)) {
	withLocal(func(local storage.FS) {
		withMem(func(mem storage.FS) {
			fs := storage.NewCacheWrapper(local, mem, options)
			cb(fs, local, mem)
		})
	})
}

func withFileCache(options *storage.CacheOptions, cb func(fs storage.FS, src storage.FS, cache storage.FS)) {
	withLocal(func(local storage.FS) {
		withLocal(func(localCache storage.FS) {
			fs := storage.NewCacheWrapper(local, localCache, options)
			cb(fs, local, localCache)
		})
	})
}

func TestCacheWrapper_Open(t *testing.T) {
	withCache(nil, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testutils.OpenNotExists(t, fs, "foo")
	})
}

func TestCacheWrapper_Create(t *testing.T) {
	withCache(nil, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testutils.Create(t, fs, "foo", "")
		testutils.OpenExists(t, src, "foo", "")
		testutils.OpenExists(t, cache, "foo", "")

		testutils.Create(t, fs, "foo", "bar")
		testutils.OpenExists(t, src, "foo", "bar")
		testutils.OpenExists(t, cache, "foo", "bar")
	})
}

func TestCacheWrapper_Create_fileCache(t *testing.T) {
	withFileCache(nil, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testutils.Create(t, fs, "foo", "")
		testutils.OpenExists(t, src, "foo", "")
		testutils.OpenExists(t, cache, "foo", "")

		testutils.Create(t, fs, "foo", "bar")
		testutils.OpenExists(t, src, "foo", "bar")
		testutils.OpenExists(t, cache, "foo", "bar")
	})
}

func TestCacheWrapper_Delete(t *testing.T) {
	withCache(nil, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testutils.Delete(t, fs, "foo")
	})
}

func TestCacheWrapper_CacheOptions_MaxAge(t *testing.T) {
	options := &storage.CacheOptions{
		MaxAge: 500 * time.Millisecond,
	}

	withCache(options, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testutils.Create(t, fs, "foo", "")

		ctx := context.Background()
		f, err := fs.Open(ctx, "foo", nil)
		assert.NoError(t, err)
		assert.NotZero(t, f)
		assert.NotZero(t, f.CreationTime)
		assert.True(t, time.Since(f.CreationTime) < options.MaxAge, "file should not be expired")

		<-time.After(options.MaxAge)

		f2, err := fs.Open(ctx, "foo", nil)
		assert.NoError(t, err)
		assert.NotZero(t, f2.CreationTime)
		assert.True(t, f2.CreationTime.After(f.CreationTime)) // New cache
	})
}

func TestCacheWrapper_CacheOptions_NoData(t *testing.T) {
	options := &storage.CacheOptions{
		NoData: true,
	}

	withCache(options, func(fs storage.FS, src storage.FS, cache storage.FS) {
		testutils.Create(t, fs, "foo", "bar")

		testutils.OpenExists(t, cache, "foo", "") // No content actually stored
	})
}
