package storage

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/gcsblob"
	"github.com/google/go-cloud/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func NewCloudStorageFS(bucket string) FS {
	return &cloudStorageFS{bucket: bucket}
}

// cloudStorageFS implements FS and uses Google Cloud Storage as the underlying
// file storage.
type cloudStorageFS struct {
	bucket string // bucket is the name of the bucket to use as the underlying storage.
}

// Open implements FS.
func (c *cloudStorageFS) Open(ctx context.Context, path string) (*File, error) {
	b, err := c.blobBucketHandle(ctx)
	if err != nil {
		return nil, err
	}

	f, err := b.NewReader(ctx, path)
	if err != nil {
		if blob.IsNotExist(err) {
			return nil, &notExistError{
				Path: path,
			}
		}
		return nil, err
	}

	return &File{
		ReadCloser: f,
		Name:       path,
		Size:       f.Size(),
		ModTime:    f.ModTime(),
	}, nil
}

// Create implements FS.
func (c *cloudStorageFS) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	b, err := c.blobBucketHandle(ctx)
	if err != nil {
		return nil, err
	}
	return b.NewWriter(ctx, path, nil)
}

// Delete implements FS.
func (c *cloudStorageFS) Delete(ctx context.Context, path string) error {
	b, err := c.blobBucketHandle(ctx)
	if err != nil {
		return err
	}
	return b.Delete(ctx, path)
}

// Walk implements FS.
func (c *cloudStorageFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	bh, err := c.bucketHandle(ctx, storage.ScopeReadOnly)
	if err != nil {
		return err
	}

	it := bh.Objects(ctx, &storage.Query{
		Prefix: path,
	})

	for {
		r, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// TODO(dhowden): Properly handle this error.
			return err
		}

		if err = fn(r.Name); err != nil {
			return err
		}
	}
	return nil
}

func (c *cloudStorageFS) blobBucketHandle(ctx context.Context) (*blob.Bucket, error) {
	dc, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		return nil, err
	}
	cl, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(dc))
	if err != nil {
		return nil, err
	}
	return gcsblob.OpenBucket(ctx, c.bucket, cl)
}

func (c *cloudStorageFS) bucketHandle(ctx context.Context, scope string) (*storage.BucketHandle, error) {
	ts, err := google.DefaultTokenSource(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to retrieve default token source: %v", err)
	}

	client, err := storage.NewClient(ctx, option.WithTokenSource(ts))
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to get client: %v", err)
	}

	return client.Bucket(c.bucket), nil
}
