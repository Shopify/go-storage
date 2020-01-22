package storage_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"os"
	"testing"

	"github.com/Shopify/go-storage/pkg"
)

func Test_mockFS_Attributes(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewMockFS()
	path := "foo"
	options := &storage.ReaderOptions{}
	attrs := &storage.Attributes{}

	fs.On("Attributes", ctx, path, options).Return(attrs, nil)
	attrs2, err := fs.Attributes(ctx, path, options)
	assert.NoError(t, err)
	assert.Equal(t, attrs, attrs2)
	fs.AssertExpectations(t)
}

func Test_mockFS_Create(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewMockFS()
	path := "foo"
	options := &storage.WriterOptions{}
	w := &os.File{}

	fs.On("Create", ctx, path, options).Return(w, nil)
	w2, err := fs.Create(ctx, path, options)
	assert.NoError(t, err)
	assert.Equal(t, w, w2)
	fs.AssertExpectations(t)
}

func Test_mockFS_Delete(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewMockFS()
	path := "foo"

	fs.On("Delete", ctx, path).Return(nil)
	err := fs.Delete(ctx, path)
	assert.NoError(t, err)
	fs.AssertExpectations(t)
}

func Test_mockFS_Open(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewMockFS()
	path := "foo"
	options := &storage.ReaderOptions{}
	file := &storage.File{}

	fs.On("Open", ctx, path, options).Return(file, nil)
	file2, err := fs.Open(ctx, path, options)
	assert.NoError(t, err)
	assert.Equal(t, file, file2)
	fs.AssertExpectations(t)
}

func Test_mockFS_URL(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewMockFS()
	path := "foo"
	options := &storage.SignedURLOptions{}
	url := "url"

	fs.On("URL", ctx, path, options).Return(url, nil)
	url2, err := fs.URL(ctx, path, options)
	assert.NoError(t, err)
	assert.Equal(t, url, url2)
	fs.AssertExpectations(t)
}

func Test_mockFS_Walk(t *testing.T) {
	ctx := context.Background()
	fs := storage.NewMockFS()
	path := "foo"
	fn := func(path string) error { return nil }

	fs.On("Walk", ctx, path, mock.Anything).Return(nil)
	err := fs.Walk(ctx, path, fn)
	assert.NoError(t, err)
	fs.AssertExpectations(t)
}
