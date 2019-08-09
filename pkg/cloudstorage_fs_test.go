package storage_test

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Shopify/go-storage/pkg"
)

func Test_cloudStorageFS_URL(t *testing.T) {
	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testCreate(t, fs, path, contents)

		url, err := fs.URL(context.Background(), path, nil)
		assert.NoError(t, err)
		assert.NotEmpty(t, url)

		resp, err := http.DefaultClient.Get(url)
		assert.NoError(t, err)
		assert.Equal(t, resp.StatusCode, http.StatusOK)

		data, err := ioutil.ReadAll(resp.Body)
		assert.NoError(t, err)
		assert.Equal(t, contents, string(data))
	})
}

func Test_cloudStorageFS_Open(t *testing.T) {
	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testCreate(t, fs, path, contents)
		testOpenExists(t, fs, path, contents)
	})
}

func Test_cloudStorageFS_Create(t *testing.T) {
	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testCreate(t, fs, path, contents)
	})
}

func Test_cloudStorageFS_Delete(t *testing.T) {
	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testCreate(t, fs, path, contents)
		testDelete(t, fs, path)
	})
}

func withCloudStorageFS(t *testing.T, cb func(fs storage.FS)) {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		t.Skip("skipping cloud storage tests, GOOGLE_APPLICATION_CREDENTIALS is empty")
	}
	bucket := os.Getenv("GOOGLE_CLOUDSTORAGE_TEST_BUCKET")
	if bucket == "" {
		t.Skip("skipping cloud storage tests, GOOGLE_CLOUDSTORAGE_TEST_BUCKET is empty")
	}

	fs := storage.NewCloudStorageFS(bucket, nil)

	randomBytes := make([]byte, 16)
	_, err := rand.Read(randomBytes)
	assert.NoError(t, err)
	prefix := fmt.Sprintf("test-go-storage/%x/", sha1.New().Sum(randomBytes))

	fs = storage.NewPrefixWrapper(fs, prefix)
	defer testRemoveAll(t, fs)

	cb(fs)
}
