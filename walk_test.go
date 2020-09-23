package storage

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testing.T) {
	withTestTree(t, func(dir string) {
		ctx := context.Background()
		fs := NewLocalFS(dir)

		list, err := List(ctx, fs, "")
		assert.NoError(t, err)
		// Note how directories are not listed and that output is sorted
		assert.Equal(t, []string{"/baz", "/foo/bar"}, list)

		list, err = List(ctx, fs, "foo")
		assert.NoError(t, err)
		assert.Equal(t, []string{"/foo/bar"}, list)

		// Error if subpath does not exist
		list, err = List(ctx, fs, "non-existent")
		assert.Errorf(t, err, "lstat %s/non-existent: no such file or directory", dir)
		assert.Equal(t, []string(nil), list)

		// Error if root directory does not exist
		list, err = List(ctx, NewLocalFS(filepath.Join(dir, "non-existent")), "")
		assert.Errorf(t, err, "lstat %s/non-existent: no such file or directory", dir)
		assert.Equal(t, []string(nil), list)
	})
}

func TestWalkN(t *testing.T) {
	withTestTree(t, func(dir string) {
		var list []string
		c := make(chan string)
		done := make(chan struct{})
		go func() {
			for path := range c {
				list = append(list, path)
			}
			close(done)
		}()

		ctx := context.Background()
		fs := NewLocalFS(dir)
		// 5 workers for 2 items
		err := WalkN(ctx, fs, "", 5, func(path string) error {
			c <- path
			return nil
		})
		close(c)

		<-done
		assert.NoError(t, err)
		// Note how directories are not listed and that output is not necessarily sorted
		assert.ElementsMatch(t, []string{"/baz", "/foo/bar"}, list)
	})
}

func withTestTree(t *testing.T, cb func(dir string)) {
	dir, err := ioutil.TempDir("", "go-storage-walk-test")
	assert.NoError(t, err)
	defer os.RemoveAll(dir)

	os.Mkdir(filepath.Join(dir, "foo"), 0755)
	os.Create(filepath.Join(dir, "foo", "bar"))
	os.Create(filepath.Join(dir, "baz"))
	cb(dir)
}
