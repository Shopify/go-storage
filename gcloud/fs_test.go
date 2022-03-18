package gcloud_test

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
	"github.com/Shopify/go-storage/gcloud"
	"github.com/Shopify/go-storage/internal/testutils"
)

func BenchmarkFS(b *testing.B) {
	ctx := context.Background()

	withFS(b, func(fs storage.FS) {
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

func Test_fs_URL(t *testing.T) {
	withFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testutils.Create(t, fs, path, contents)

		url, err := fs.URL(context.Background(), path, nil)
		require.NoError(t, err)
		require.NotEmpty(t, url)

		resp, err := http.DefaultClient.Get(url)
		require.NoError(t, err)
		require.Equal(t, resp.StatusCode, http.StatusOK)

		defer resp.Body.Close()
		data, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, contents, string(data))
	})
}

func Test_fs_Open(t *testing.T) {
	withFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testutils.Create(t, fs, path, contents)
		testutils.OpenExists(t, fs, path, contents)
	})
}

func Test_fs_Create(t *testing.T) {
	withFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testutils.Create(t, fs, path, contents)
	})
}

func Test_fs_Delete(t *testing.T) {
	withFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testutils.Create(t, fs, path, contents)
		testutils.Delete(t, fs, path)
	})
}

func withFS(tb testing.TB, cb func(fs storage.FS)) {
	tb.Helper()

	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		tb.Skip("skipping cloud storage tests, GOOGLE_APPLICATION_CREDENTIALS is empty")
	}
	bucket := os.Getenv("STORAGE_GCLOUD_TEST_BUCKET")
	if bucket == "" {
		tb.Skip("skipping cloud storage tests, STORAGE_GCLOUD_TEST_BUCKET is empty")
	}

	fs := gcloud.NewFS(bucket, nil)

	randomBytes := make([]byte, 16)
	_, err := rand.Read(randomBytes)
	require.NoError(tb, err)
	prefix := fmt.Sprintf("test-go-storage/%x/", sha1.New().Sum(randomBytes))

	fs = storage.NewPrefixWrapper(fs, prefix)
	defer testutils.RemoveAll(tb, fs)

	cb(fs)
}

func Test_ResolveScope(t *testing.T) {
	tests := map[storage.Scope]storage.Scope{
		storage.ScopeRead:                         storage.ScopeRead,
		storage.ScopeWrite:                        storage.ScopeRW,
		storage.ScopeDelete:                       storage.ScopeRWD,
		storage.ScopeWrite | storage.ScopeSignURL: storage.ScopeRW | storage.ScopeSignURL,
	}
	for input, output := range tests {
		t.Run(input.String(), func(t *testing.T) {
			require.Equal(t, output, gcloud.ResolveScope(input))
		})
	}
}
