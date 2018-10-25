package storage_test

import (
	"testing"

	"github.com/Shopify/go-storage/pkg"
)

const prefix = "testPrefix/"

func withPrefix(cb func(fs storage.FS, src storage.FS)) {
	src := storage.NewMemoryFS()
	fs := storage.NewPrefixWrapper(src, prefix)
	cb(fs, src)
}

func TestPrefixOpen(t *testing.T) {
	withPrefix(func(fs storage.FS, src storage.FS) {
		testOpenNotExists(t, fs, "foo")
	})
}

func TestPrefixCreate(t *testing.T) {
	withPrefix(func(fs storage.FS, src storage.FS) {
		testCreate(t, fs, "foo", "")
		testOpenExists(t, src, "testPrefix/foo", "")

		testCreate(t, fs, "foo", "bar")
		testOpenExists(t, src, "testPrefix/foo", "bar")
	})
}

func TestPrefixDelete(t *testing.T) {
	withPrefix(func(fs storage.FS, src storage.FS) {
		testCreate(t, src, "testPrefix/foo", "bar")
		testOpenExists(t, fs, "foo", "bar")

		testDelete(t, fs, "foo")
		testOpenNotExists(t, fs, "foo")
	})
}
