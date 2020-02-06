package storage

import (
	"context"
	"gocloud.dev/gcerrors"
	"io"

	"github.com/pkg/errors"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
)

var ErrCredentialsMissing = errors.New("credentials missing")

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
	b, err := c.bucketHandleForSigning(ctx, storage.DevstorageReadOnlyScope)
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
func (c *cloudStorageFS) Open(ctx context.Context, path string, options *ReaderOptions) (*File, error) {
	b, err := c.bucketHandle(ctx, storage.DevstorageReadOnlyScope)
	if err != nil {
		return nil, err
	}

	f, err := b.NewReader(ctx, path, nil)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, &notExistError{
				Path: path,
			}
		}
		return nil, err
	}

	return &File{
		ReadCloser: f,
		Attributes: Attributes{
			ContentType: f.ContentType(),
			Size:        f.Size(),
			ModTime:     f.ModTime(),
		},
	}, nil
}

// Attributes implements FS.
func (c *cloudStorageFS) Attributes(ctx context.Context, path string, options *ReaderOptions) (*Attributes, error) {
	b, err := c.bucketHandle(ctx, storage.DevstorageReadOnlyScope)
	if err != nil {
		return nil, err
	}

	a, err := b.Attributes(ctx, path)
	if err != nil {
		return nil, err
	}

	return &Attributes{
		ContentType: a.ContentType,
		Metadata:    a.Metadata,
		ModTime:     a.ModTime,
		Size:        a.Size,
	}, nil
}

// Create implements FS.
func (c *cloudStorageFS) Create(ctx context.Context, path string, options *WriterOptions) (io.WriteCloser, error) {
	b, err := c.bucketHandle(ctx, storage.DevstorageReadWriteScope)
	if err != nil {
		return nil, err
	}
	var blobOpts *blob.WriterOptions
	if options != nil {
		blobOpts = &blob.WriterOptions{
			Metadata:    options.Attributes.Metadata,
			ContentType: options.Attributes.ContentType,
			BufferSize:  options.BufferSize,
		}
	}
	return b.NewWriter(ctx, path, blobOpts)
}

// Delete implements FS.
func (c *cloudStorageFS) Delete(ctx context.Context, path string) error {
	b, err := c.bucketHandle(ctx, storage.DevstorageFullControlScope)
	if err != nil {
		return err
	}
	return b.Delete(ctx, path)
}

// Walk implements FS.
func (c *cloudStorageFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	bh, err := c.bucketHandle(ctx, storage.DevstorageReadOnlyScope)
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

func (c *cloudStorageFS) client(ctx context.Context, scope string, extraScopes ...string) (*gcp.HTTPClient, *google.Credentials, error) {
	creds, err := c.findCredentials(ctx, scope, extraScopes...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "cloud storage: unable to retrieve default token source")
	}

	client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, nil, errors.Wrap(err, "cloud storage: unable to build http client")
	}

	return client, creds, nil
}

func (c *cloudStorageFS) bucketHandle(ctx context.Context, scope string, extraScopes ...string) (*blob.Bucket, error) {
	client, _, err := c.client(ctx, scope, extraScopes...)
	if err != nil {
		return nil, err
	}

	return gcsblob.OpenBucket(ctx, client, c.bucket, nil)
}

func (c *cloudStorageFS) bucketHandleForSigning(ctx context.Context, scope string, extraScopes ...string) (*blob.Bucket, error) {
	client, creds, err := c.client(ctx, scope, extraScopes...)
	if err != nil {
		return nil, err
	}

	if creds == nil {
		return nil, ErrCredentialsMissing
	}

	config, err := google.JWTConfigFromJSON(creds.JSON, append(extraScopes, scope)...)
	if err != nil {
		return nil, errors.Wrap(err, "cloud storage: parse credentials")
	}
	options := &gcsblob.Options{
		PrivateKey:     config.PrivateKey,
		GoogleAccessID: config.Email,
	}

	return gcsblob.OpenBucket(ctx, client, c.bucket, options)
}
