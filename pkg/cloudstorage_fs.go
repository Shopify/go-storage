package storage

import (
	"context"
	"io"

	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/gcsblob"
	"github.com/google/go-cloud/gcp"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
)

// NewCloudStorageFS creates a Google Cloud Storage FS
// credentials can be nil to use the default GOOGLE_APPLICATION_CREDENTIALS
func NewCloudStorageFS(bucket string, credentials *google.Credentials) FS {
	return &cloudStorageFS{
		bucket:      bucket,
		credentials: credentials,
	}
}

// cloudStorageFS implements FS and uses Google Cloud Storage as the underlying
// file storage.
type cloudStorageFS struct {
	// bucket is the name of the bucket to use as the underlying storage.
	bucket      string
	credentials *google.Credentials
}

func (c *cloudStorageFS) URL(ctx context.Context, path string, options *SignedURLOptions) (string, error) {
	b, err := c.blobBucketHandle(ctx, storage.DevstorageReadOnlyScope)
	if err != nil {
		return "", err
	}

	var blobOptions *blob.SignedURLOptions
	if options != nil {
		o := blob.SignedURLOptions(*options)
		blobOptions = &o
	}

	return b.SignedURL(ctx, path, blobOptions)
}

// Open implements FS.
func (c *cloudStorageFS) Open(ctx context.Context, path string) (*File, error) {
	b, err := c.blobBucketHandle(ctx, storage.DevstorageReadOnlyScope)
	if err != nil {
		return nil, err
	}

	f, err := b.NewReader(ctx, path, nil)
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
	b, err := c.blobBucketHandle(ctx, storage.DevstorageReadWriteScope)
	if err != nil {
		return nil, err
	}
	return b.NewWriter(ctx, path, nil)
}

// Delete implements FS.
func (c *cloudStorageFS) Delete(ctx context.Context, path string) error {
	b, err := c.blobBucketHandle(ctx, storage.DevstorageFullControlScope)
	if err != nil {
		return err
	}
	return b.Delete(ctx, path)
}

// Walk implements FS.
func (c *cloudStorageFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	bh, err := c.blobBucketHandle(ctx, storage.DevstorageReadOnlyScope)
	if err != nil {
		return err
	}

	it := bh.List(&blob.ListOptions{
		Prefix: path,
	})

	for {
		r, err := it.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			// TODO(dhowden): Properly handle this error.
			return err
		}

		if err = fn(r.Key); err != nil {
			return err
		}
	}
	return nil
}

func (c *cloudStorageFS) findCredentials(ctx context.Context, scope string, extraScopes ...string) (*google.Credentials, error) {
	if c.credentials != nil {
		return c.credentials, nil
	}
	return google.FindDefaultCredentials(ctx, append(extraScopes, scope)...)
}

func (c *cloudStorageFS) blobBucketHandle(ctx context.Context, scope string, extraScopes ...string) (*blob.Bucket, error) {
	creds, err := c.findCredentials(ctx, scope, extraScopes...)
	if err != nil {
		return nil, errors.Wrap(err, "cloud storage: unable to retrieve default token source")
	}

	client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, err
	}

	var options *gcsblob.Options
	if creds != nil {
		config, err := google.JWTConfigFromJSON(creds.JSON, append(extraScopes, scope)...)
		if err != nil {
			return nil, errors.Wrap(err, "cloud storage: parse credentials")
		}
		options = &gcsblob.Options{
			PrivateKey:     config.PrivateKey,
			GoogleAccessID: config.Email,
		}
	}

	return gcsblob.OpenBucket(ctx, c.bucket, client, options)
}
