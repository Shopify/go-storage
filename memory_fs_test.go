package storage_test

import (
	"testing"

	"github.com/Shopify/go-storage"
)

func withMem(cb func(storage.FS)) {
	cb(storage.NewMemoryFS())
}

func TestMemOpen(t *testing.T) {
	withMem(func(fs storage.FS) {
		testOpenNotExists(t, fs, "foo")
	})
}

func TestMemCreate(t *testing.T) {
	withMem(func(fs storage.FS) {
		testCreate(t, fs, "foo", "")
		testCreate(t, fs, "foo", "bar")
	})
}

func TestMemDelete(t *testing.T) {
	withMem(func(fs storage.FS) {
		testDelete(t, fs, "foo")
	})
}
