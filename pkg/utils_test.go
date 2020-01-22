package storage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Shopify/go-storage/pkg"
)

func TestExists(t *testing.T) {
	ctx := context.Background()

	withMem(func(fs storage.FS) {
		assert.False(t, storage.Exists(ctx, fs, "foo"))
		testCreate(t, fs, "foo", "bar")
		assert.True(t, storage.Exists(ctx, fs, "foo"))
	})
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	withMem(func(fs storage.FS) {
		testCreate(t, fs, "foo", "bar")

		data, err := storage.Read(ctx, fs, "foo", nil)
		assert.NoError(t, err)
		assert.Equal(t, []byte("bar"), data)
	})
}

func TestWrite(t *testing.T) {
	ctx := context.Background()
	withMem(func(fs storage.FS) {
		assert.NoError(t, storage.Write(ctx, fs, "foo", []byte("bar"), nil))
		testOpenExists(t, fs, "foo", "bar")
	})
}
