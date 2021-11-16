package storage

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/pkg/errors"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcerrors"
	"gocloud.dev/gcp"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/storage/v1"
)

var ErrCredentialsMissing = errors.New("credentials missing")

// NewCloudStorageFS creates a Google Cloud Storage FS
// credentials can be nil to use the default GOOGLE_APPLICATION_CREDENTIALS
func NewCloudStorageFS(bucket string, credentials *google.Credentials) FS {
	return &cloudStorageFS{
		bucketName:  bucket,
		credentials: credentials,
	}
}

// cloudStorageFS implements FS and uses Google Cloud Storage as the underlying
// file storage.
type cloudStorageFS struct {
	l            sync.RWMutex
	bucket       *blob.Bucket
	bucketName   string
	bucketScopes Scope
	credentials  *google.Credentials
}

func (c *cloudStorageFS) URL(ctx context.Context, path string, options *SignedURLOptions) (string, error) {
	b, err := c.bucketHandle(ctx, ScopeSignURL)
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
	b, err := c.bucketHandle(ctx, ScopeRead)
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
	b, err := c.bucketHandle(ctx, ScopeRead)
	if err != nil {
		return nil, err
	}

	a, err := b.Attributes(ctx, path)
	if err != nil {
		return nil, err
	}

	return &Attributes{
		ContentType:     a.ContentType,
		ContentEncoding: a.ContentEncoding,
		Metadata:        a.Metadata,
		ModTime:         a.ModTime,
		Size:            a.Size,
	}, nil
}

// Create implements FS.
func (c *cloudStorageFS) Create(ctx context.Context, path string, options *WriterOptions) (io.WriteCloser, error) {
	b, err := c.bucketHandle(ctx, ScopeWrite)
	if err != nil {
		return nil, err
	}
	var blobOpts *blob.WriterOptions
	if options != nil {
		blobOpts = &blob.WriterOptions{
			Metadata:        options.Attributes.Metadata,
			ContentType:     options.Attributes.ContentType,
			ContentEncoding: options.Attributes.ContentEncoding,
			BufferSize:      options.BufferSize,
		}
	}
	return b.NewWriter(ctx, path, blobOpts)
}

// Delete implements FS.
func (c *cloudStorageFS) Delete(ctx context.Context, path string) error {
	b, err := c.bucketHandle(ctx, ScopeDelete)
	if err != nil {
		return err
	}
	return b.Delete(ctx, path)
}

// Walk implements FS.
func (c *cloudStorageFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	bh, err := c.bucketHandle(ctx, ScopeRead)
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

func cloudStorageScope(scope Scope) string {
	switch {
	case scope.Has(ScopeDelete):
		return storage.DevstorageFullControlScope
	case scope.Has(ScopeWrite):
		return storage.DevstorageReadWriteScope
	case scope.Has(ScopeRead), scope.Has(ScopeSignURL):
		return storage.DevstorageReadOnlyScope
	default:
		panic(fmt.Sprintf("unknown scope: '%s'", scope))
	}
}

func ResolveCloudStorageScope(scope Scope) Scope {
	switch cloudStorageScope(scope) {
	case storage.DevstorageFullControlScope:
		return ScopeRWD | scope
	case storage.DevstorageReadWriteScope:
		return ScopeRW | scope
	case storage.DevstorageReadOnlyScope:
		return ScopeRead | scope
	default:
		panic(fmt.Sprintf("unknown scope: '%s'", scope))
	}
}

func (c *cloudStorageFS) findCredentials(ctx context.Context, scope Scope) (*google.Credentials, error) {
	if c.credentials != nil {
		return c.credentials, nil
	}
	return google.FindDefaultCredentials(ctx, cloudStorageScope(scope))
}

func (c *cloudStorageFS) client(ctx context.Context, scope Scope) (*gcp.HTTPClient, *google.Credentials, error) {
	creds, err := c.findCredentials(ctx, scope)
	if err != nil {
		return nil, nil, errors.Wrap(err, "cloud storage: unable to retrieve default token source")
	}

	client, err := gcp.NewHTTPClient(gcp.DefaultTransport(), gcp.CredentialsTokenSource(creds))
	if err != nil {
		return nil, nil, errors.Wrap(err, "cloud storage: unable to build http client")
	}

	return client, creds, nil
}

func (c *cloudStorageFS) bucketOptions(creds *google.Credentials, scope Scope) (*gcsblob.Options, error) {
	if !scope.Has(ScopeSignURL) {
		return nil, nil
	}

	if creds == nil {
		return nil, ErrCredentialsMissing
	}

	config, err := google.JWTConfigFromJSON(creds.JSON, cloudStorageScope(scope))
	if err != nil {
		return nil, errors.Wrap(err, "cloud storage: parse credentials")
	}
	return &gcsblob.Options{
		PrivateKey:     config.PrivateKey,
		GoogleAccessID: config.Email,
	}, nil
}

func (c *cloudStorageFS) openBucket(ctx context.Context, scope Scope) (*blob.Bucket, error) {
	client, creds, err := c.client(ctx, scope)
	if err != nil {
		return nil, err
	}

	options, err := c.bucketOptions(creds, scope)
	if err != nil {
		return nil, err
	}

	return gcsblob.OpenBucket(ctx, client, c.bucketName, options)
}

func (c *cloudStorageFS) bucketHandle(ctx context.Context, scope Scope) (*blob.Bucket, error) {
	c.l.RLock()
	scope |= c.bucketScopes // Expand requested scope to encompass existing scopes
	if bucket := c.bucket; bucket != nil && c.bucketScopes.Has(scope) {
		c.l.RUnlock()
		return bucket, nil
	}
	c.l.RUnlock()

	c.l.Lock()
	defer c.l.Unlock()
	if c.bucket != nil && c.bucketScopes.Has(scope) { // Race condition
		return c.bucket, nil
	}

	// Expand the requested scope to include the scopes that GCS would provide
	// e.g. Requesting Write actually provides ReadWrite.
	// Also include any scope that was previously used.
	scope = ResolveCloudStorageScope(c.bucketScopes | scope)

	bucket, err := c.openBucket(ctx, scope)
	if err != nil {
		return nil, err
	}

	c.bucket = bucket
	c.bucketScopes = scope
	return bucket, nil
}
