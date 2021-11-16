package storage_test

import (
	"context"
	"io"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Shopify/go-storage"
)

func testOpenExists(t *testing.T, fs storage.FS, path string, content string) {
	ctx := context.Background()

	f, err := fs.Open(ctx, path, nil)
	assert.NoError(t, err)

	b, err := ioutil.ReadAll(f)
	assert.NoError(t, err)

	got := string(b)
	assert.Equal(t, content, got)

	attrs, err := fs.Attributes(ctx, path, nil)
	assert.NoError(t, err)
	assert.Equal(t, f.Attributes.Metadata, attrs.Metadata)
	assert.Equal(t, f.Attributes.Size, attrs.Size)
	assert.Equal(t, f.Attributes.ContentType, attrs.ContentType)
	assert.Equal(t, f.Attributes.ContentEncoding, attrs.ContentEncoding)

	err = f.Close()
	assert.NoError(t, err)
}

func testOpenNotExists(t *testing.T, fs storage.FS, path string) {
	ctx := context.Background()
	_, err := fs.Open(ctx, path, nil)
	assert.Errorf(t, err, "storage %s: path does not exist", path)

	_, err = fs.Attributes(ctx, path, nil)
	assert.Errorf(t, err, "storage %s: path does not exist", path)
}

func testCreate(t *testing.T, fs storage.FS, path string, content string) {
	ctx := context.Background()

	wc, err := fs.Create(ctx, path, nil)
	assert.NoError(t, err)

	_, err = io.WriteString(wc, content)
	assert.NoError(t, err)

	err = wc.Close()
	assert.NoError(t, err)

	testOpenExists(t, fs, path, content)
}

func testDelete(t *testing.T, fs storage.FS, path string) {
	ctx := context.Background()

	testCreate(t, fs, path, "foo")

	err := fs.Delete(ctx, path)
	assert.NoError(t, err)

	testOpenNotExists(t, fs, path)
}

func testRemoveAll(t testing.TB, fs storage.FS) {
	err := fs.Walk(context.Background(), "", func(path string) error {
		return fs.Delete(context.Background(), path)
	})
	assert.NoError(t, err)
}
