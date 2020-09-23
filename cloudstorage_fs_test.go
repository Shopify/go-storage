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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Shopify/go-storage"
)

func BenchmarkCloudStorageFS(b *testing.B) {
	ctx := context.Background()

	withCloudStorageFS(b, func(fs storage.FS) {
		b.Run("create", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				path := fmt.Sprintf("benchmark-%d", time.Now().Nanosecond())

				w, err := fs.Create(ctx, path, nil)
				require.NoError(b, err)

				_, err = w.Write([]byte("test"))
				require.NoError(b, err)

				err = w.Close()
				require.NoError(b, err)

				b.StopTimer()
				err = fs.Delete(ctx, path)
				require.NoError(b, err)
				b.StartTimer()
			}
		})

		b.Run("read", func(b *testing.B) {
			path := fmt.Sprintf("benchmark-%d", time.Now().Nanosecond())

			w, err := fs.Create(ctx, path, nil)
			require.NoError(b, err)
			_, err = w.Write([]byte("test"))
			require.NoError(b, err)
			err = w.Close()
			require.NoError(b, err)

			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				r, err := fs.Open(ctx, path, nil)
				require.NoError(b, err)

				_, err = ioutil.ReadAll(r)
				require.NoError(b, err)

				err = r.Close()
				require.NoError(b, err)
			}

			b.StopTimer()
			err = fs.Delete(ctx, path)
			require.NoError(b, err)
		})
	})
}

func Test_cloudStorageFS_URL(t *testing.T) {
	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testCreate(t, fs, path, contents)

		url, err := fs.URL(context.Background(), path, nil)
		require.NoError(t, err)
		require.NotEmpty(t, url)

		resp, err := http.DefaultClient.Get(url)
		require.NoError(t, err)
		require.Equal(t, resp.StatusCode, http.StatusOK)

		data, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, contents, string(data))
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

func withCloudStorageFS(t testing.TB, cb func(fs storage.FS)) {
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
	require.NoError(t, err)
	prefix := fmt.Sprintf("test-go-storage/%x/", sha1.New().Sum(randomBytes))

	fs = storage.NewPrefixWrapper(fs, prefix)
	defer testRemoveAll(t, fs)

	cb(fs)
}

func Test_ResolveCloudStorageScope(t *testing.T) {
	tests := map[storage.Scope]storage.Scope{
		storage.ScopeRead:                         storage.ScopeRead,
		storage.ScopeWrite:                        storage.ScopeRW,
		storage.ScopeDelete:                       storage.ScopeRWD,
		storage.ScopeWrite | storage.ScopeSignURL: storage.ScopeRW | storage.ScopeSignURL,
	}
	for input, output := range tests {
		t.Run(input.String(), func(t *testing.T) {
			require.Equal(t, output, storage.ResolveCloudStorageScope(input))
		})
	}
}
