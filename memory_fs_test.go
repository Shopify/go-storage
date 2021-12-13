package storage_test

import (
	"testing"

	"github.com/Shopify/go-storage"
	"github.com/Shopify/go-storage/internal/testutils"
)

func withMem(cb func(storage.FS)) {
	cb(storage.NewMemoryFS())
}

func TestMemOpen(t *testing.T) {
	withMem(func(fs storage.FS) {
		testutils.OpenNotExists(t, fs, "foo")
	})
}

func TestMemCreate(t *testing.T) {
	withMem(func(fs storage.FS) {
		testutils.Create(t, fs, "foo", "")
		testutils.Create(t, fs, "foo", "bar")
	})
}

func TestMemDelete(t *testing.T) {
	withMem(func(fs storage.FS) {
		testutils.Delete(t, fs, "foo")
	})
}
