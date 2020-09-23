package storage_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/Shopify/go-storage"
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
