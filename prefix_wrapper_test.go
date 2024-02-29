package storage_test

import (
	"testing"

	"github.com/Shopify/go-storage"
	"github.com/Shopify/go-storage/internal/testutils"
)

const prefix = "testPrefix/"

func withPrefix(cb func(fs storage.FS, src storage.FS)) {
	src := storage.NewMemoryFS()
	fs := storage.NewPrefixWrapper(src, prefix)
	cb(fs, src)
}

func TestPrefixOpen(t *testing.T) {
	withPrefix(func(fs storage.FS, _ storage.FS) {
		testutils.OpenNotExists(t, fs, "foo")
	})
}

func TestPrefixCreate(t *testing.T) {
	withPrefix(func(fs storage.FS, src storage.FS) {
		testutils.Create(t, fs, "foo", "")
		testutils.OpenExists(t, src, "testPrefix/foo", "")

		testutils.Create(t, fs, "foo", "bar")
		testutils.OpenExists(t, src, "testPrefix/foo", "bar")
	})
}

func TestPrefixDelete(t *testing.T) {
	withPrefix(func(fs storage.FS, src storage.FS) {
		testutils.Create(t, src, "testPrefix/foo", "bar")
		testutils.OpenExists(t, fs, "foo", "bar")

		testutils.Delete(t, fs, "foo")
		testutils.OpenNotExists(t, fs, "foo")
	})
}
