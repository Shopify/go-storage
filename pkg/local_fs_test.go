package storage_test

import (
	"os"
	"testing"

	"github.com/Shopify/go-storage/pkg"
)

func withLocal(cb func(storage.FS)) {
	dir := os.TempDir()
	defer os.RemoveAll(dir)

	fs := storage.NewLocalFS(dir)
	cb(fs)
}

func TestLocalOpen(t *testing.T) {
	withLocal(func(fs storage.FS) {
		testOpenNotExists(t, fs, "foo")
	})
}

func TestLocalCreate(t *testing.T) {
	withLocal(func(fs storage.FS) {
		testCreate(t, fs, "foo", "")
		testCreate(t, fs, "foo", "bar")
	})
}

func TestLocalDelete(t *testing.T) {
	withLocal(func(fs storage.FS) {
		testDelete(t, fs, "foo")
	})
}
