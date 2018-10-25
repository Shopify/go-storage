package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/gcsblob"
	"github.com/google/go-cloud/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const defaultCloudStorageSignedURLExpiration = 1 * time.Minute

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

func (c *cloudStorageFS) URL(ctx context.Context, path string, options *URLOptions) (string, error) {
	creds, err := c.findCredentials(ctx, storage.ScopeReadOnly)
	if err != nil {
		return "", fmt.Errorf("cloud storage: unable to retrieve default token source: %v", err)
	}
	config, err := google.JWTConfigFromJSON(creds.JSON, storage.ScopeReadOnly)
	if err != nil {
		return "", fmt.Errorf("cloud storage: parse credentials: %v", err)
	}

	var expires time.Time
	if options != nil && options.Expiration != 0 {
		expires = time.Now().Add(options.Expiration)
	} else {
		expires = time.Now().Add(defaultCloudStorageSignedURLExpiration)
	}

	return storage.SignedURL(c.bucket, path, &storage.SignedURLOptions{
		GoogleAccessID: config.Email,
		PrivateKey:     config.PrivateKey,
		Expires:        expires,
		Method:         "GET",
	})
}

// Open implements FS.
func (c *cloudStorageFS) Open(ctx context.Context, path string) (*File, error) {
	b, err := c.blobBucketHandle(ctx, storage.ScopeReadOnly)
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
	b, err := c.blobBucketHandle(ctx, storage.ScopeReadWrite)
	if err != nil {
		return nil, err
	}
	return b.NewWriter(ctx, path, nil)
}

// Delete implements FS.
func (c *cloudStorageFS) Delete(ctx context.Context, path string) error {
	b, err := c.blobBucketHandle(ctx, storage.ScopeReadWrite)
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

func (c *cloudStorageFS) findCredentials(ctx context.Context, scope string, extraScopes ...string) (*google.Credentials, error) {
	if c.credentials != nil {
		return c.credentials, nil
	}
	return google.FindDefaultCredentials(ctx, append(extraScopes, scope)...)
}

func (c *cloudStorageFS) blobBucketHandle(ctx context.Context, scope string, extraScopes ...string) (*blob.Bucket, error) {
	creds, err := c.findCredentials(ctx, scope, extraScopes...)
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to retrieve default token source: %v", err)
	}

	client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, err
	}

	return gcsblob.OpenBucket(ctx, c.bucket, client)
}

func (c *cloudStorageFS) bucketHandle(ctx context.Context, scope string, extraScopes ...string) (*storage.BucketHandle, error) {
	creds, err := c.findCredentials(ctx, scope, extraScopes...)
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to retrieve default token source: %v", err)
	}

	client, err := storage.NewClient(ctx, option.WithTokenSource(gcp.CredentialsTokenSource(creds)))
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to get client: %v", err)
	}

	return client.Bucket(c.bucket), nil
}
