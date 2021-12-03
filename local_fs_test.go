package storage_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/Shopify/go-storage"
	"github.com/Shopify/go-storage/internal/testutils"
)

func withLocal(cb func(storage.FS)) {
	dir, err := ioutil.TempDir("", "go-storage-local-test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	fs := storage.NewLocalFS(dir)
	cb(fs)
}

func TestLocalOpen(t *testing.T) {
	withLocal(func(fs storage.FS) {
		testutils.OpenNotExists(t, fs, "foo")
	})
}

func TestLocalCreate(t *testing.T) {
	withLocal(func(fs storage.FS) {
		testutils.Create(t, fs, "foo", "")
		testutils.Create(t, fs, "foo", "bar")
	})
}

func TestLocalDelete(t *testing.T) {
	withLocal(func(fs storage.FS) {
		testutils.Delete(t, fs, "foo")
	})
}
