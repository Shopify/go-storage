package storage_test

import (
	"testing"

	"github.com/Shopify/go-storage/pkg"
)

func withCache(cb func(fs storage.FS, src storage.FS, cache storage.FS)) {
	withLocal(func(local storage.FS) {
		withMem(func(mem storage.FS) {
			fs := storage.NewCacheFS(local, mem)
			cb(fs, local, mem)
		})
	})
}

func TestCacheOpen(t *testing.T) {
	withCache(func(fs storage.FS, src storage.FS, cache storage.FS) {
		testOpenNotExists(t, fs, "foo")
	})
}

func TestCacheCreate(t *testing.T) {
	withCache(func(fs storage.FS, src storage.FS, cache storage.FS) {
		testCreate(t, fs, "foo", "")
		testOpenExists(t, src, "foo", "")
		testOpenExists(t, cache, "foo", "")

		testCreate(t, fs, "foo", "bar")
		testOpenExists(t, src, "foo", "bar")
		testOpenExists(t, cache, "foo", "bar")
	})
}

func TestCacheDelete(t *testing.T) {
	withCache(func(fs storage.FS, src storage.FS, cache storage.FS) {
		testDelete(t, fs, "foo")
	})
}
