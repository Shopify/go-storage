package storage_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/Shopify/go-storage"
	"github.com/Shopify/go-storage/internal/testutils"
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

				_, err = io.ReadAll(r)
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
		testutils.Create(t, fs, path, contents)

		url, err := fs.URL(context.Background(), path, nil)
		require.NoError(t, err)
		require.NotEmpty(t, url)

		resp, err := http.DefaultClient.Get(url)
		require.NoError(t, err)
		require.Equal(t, resp.StatusCode, http.StatusOK)

		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, contents, string(data))
	})
}

func Test_cloudStorageFS_Open(t *testing.T) {
	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testutils.Create(t, fs, path, contents)
		testutils.OpenExists(t, fs, path, contents)
	})
}

func Test_cloudStorageFS_Create(t *testing.T) {
	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testutils.Create(t, fs, path, contents)
	})
}

func Test_cloudStorageFS_Delete(t *testing.T) {
	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"
		contents := "test"
		testutils.Create(t, fs, path, contents)
		testutils.Delete(t, fs, path)
	})
}

func Test_cloudStorageFS_Content_Encoding(t *testing.T) {
	ctx := context.Background()

	withCloudStorageFS(t, func(fs storage.FS) {
		path := "foo"

		contentRaw := []byte("test")
		contentCompressed := gzipCompress(contentRaw)

		// Write compressed content

		wc, err := fs.Create(ctx, path, &storage.WriterOptions{
			Attributes: storage.Attributes{
				// Don't let the client infer the CT as application/x-gzip
				// https://github.com/googleapis/google-cloud-go/issues/1743#issuecomment-581639160
				ContentType: "text/plain",

				ContentEncoding: "gzip",
			},
		})
		require.NoError(t, err)

		_, err = wc.Write(contentCompressed)
		require.NoError(t, err)

		err = wc.Close()
		require.NoError(t, err)

		// Read content - auto-transcoding on: uncompressed according to Content-Encoding.

		f, err := fs.Open(ctx, path, &storage.ReaderOptions{ReadCompressed: false})
		require.NoError(t, err)

		require.Equal(t, "", f.Attributes.ContentEncoding) // Payload is decoded, ContentEncoding must be unset

		readContent, err := io.ReadAll(f)
		require.NoError(t, err)

		require.Equal(t, contentRaw, readContent)

		// Read content - auto-transcoding off: ignoring Content-Encoding.

		f, err = fs.Open(ctx, path, &storage.ReaderOptions{ReadCompressed: true})
		require.NoError(t, err)

		require.Equal(t, "gzip", f.Attributes.ContentEncoding)

		readContent, err = io.ReadAll(f)
		require.NoError(t, err)

		require.Equal(t, contentCompressed, readContent)
	})
}

func withCloudStorageFS(tb testing.TB, cb func(fs storage.FS)) {
	tb.Helper()

	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		tb.Skip("skipping cloud storage tests, GOOGLE_APPLICATION_CREDENTIALS is empty")
	}
	bucket := os.Getenv("GOOGLE_CLOUDSTORAGE_TEST_BUCKET")
	if bucket == "" {
		tb.Skip("skipping cloud storage tests, GOOGLE_CLOUDSTORAGE_TEST_BUCKET is empty")
	}

	fs := storage.NewCloudStorageFS(bucket, nil)

	randomBytes := make([]byte, 16)
	_, err := rand.Read(randomBytes)
	require.NoError(tb, err)
	prefix := fmt.Sprintf("test-go-storage/%x/", sha1.New().Sum(randomBytes))

	fs = storage.NewPrefixWrapper(fs, prefix)
	defer testutils.RemoveAll(tb, fs)

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

func gzipCompress(b []byte) []byte {
	var buf bytes.Buffer

	compressor := gzip.NewWriter(&buf)

	_, err := compressor.Write(b)
	if err != nil {
		panic(err)
	}

	err = compressor.Close()
	if err != nil {
		panic(err)
	}

	return buf.Bytes()
}
