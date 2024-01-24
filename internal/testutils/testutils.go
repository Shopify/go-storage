package testutils

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Shopify/go-storage"
)

func OpenExists(t *testing.T, fs storage.FS, path string, content string) {
	t.Helper()
	ctx := context.Background()

	f, err := fs.Open(ctx, path, nil)
	assert.NoError(t, err)

	b, err := io.ReadAll(f)
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

func OpenNotExists(t *testing.T, fs storage.FS, path string) {
	t.Helper()
	ctx := context.Background()

	_, err := fs.Open(ctx, path, nil)
	assert.Errorf(t, err, "storage %s: path does not exist", path)

	_, err = fs.Attributes(ctx, path, nil)
	assert.Errorf(t, err, "storage %s: path does not exist", path)
}

func Create(t *testing.T, fs storage.FS, path string, content string) {
	t.Helper()
	ctx := context.Background()

	wc, err := fs.Create(ctx, path, nil)
	assert.NoError(t, err)

	_, err = io.WriteString(wc, content)
	assert.NoError(t, err)

	err = wc.Close()
	assert.NoError(t, err)

	OpenExists(t, fs, path, content)
}

func Delete(t *testing.T, fs storage.FS, path string) {
	t.Helper()
	ctx := context.Background()

	Create(t, fs, path, "foo")

	err := fs.Delete(ctx, path)
	assert.NoError(t, err)

	OpenNotExists(t, fs, path)
}

func RemoveAll(tb testing.TB, fs storage.FS) {
	tb.Helper()
	ctx := context.Background()

	err := fs.Walk(ctx, "", func(path string) error {
		return fs.Delete(ctx, path)
	})
	assert.NoError(tb, err)
}
