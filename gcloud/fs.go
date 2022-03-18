package gcloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	gstorage "cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/Shopify/go-storage"
)

// NewFS creates a Google Cloud Storage FS
// credentials can be nil to use the default GOOGLE_APPLICATION_CREDENTIALS
func NewFS(bucket string, credentials *google.Credentials) storage.FS {
	return &fs{
		bucketName:  bucket,
		credentials: credentials,
	}
}

// fs implements storage.FS and uses Google Cloud Storage as the underlying
// file storage.
type fs struct {
	bucketName  string
	credentials *google.Credentials

	bucketLock   sync.RWMutex
	bucket       *gstorage.BucketHandle
	bucketScopes storage.Scope
}

func (f *fs) URL(ctx context.Context, path string, options *storage.SignedURLOptions) (string, error) {
	if options == nil {
		options = &storage.SignedURLOptions{}
	}
	options.ApplyDefaults()

	b, err := f.bucketHandle(ctx, storage.ScopeSignURL)
	if err != nil {
		return "", err
	}

	return b.SignedURL(path, &gstorage.SignedURLOptions{
		Method:  options.Method,
		Expires: time.Now().Add(options.Expiry),
	})
}

// Open implements FS.
func (f *fs) Open(ctx context.Context, path string, options *storage.ReaderOptions) (*storage.File, error) {
	b, err := f.bucketHandle(ctx, storage.ScopeRead)
	if err != nil {
		return nil, err
	}

	o, err := b.Object(path).NewReader(ctx)
	if err != nil {
		if errors.Is(err, gstorage.ErrObjectNotExist) {
			return nil, &storage.NotExistError{
				Path: path,
			}
		}

		return nil, err
	}

	return &storage.File{
		ReadCloser: o,
		Attributes: storage.Attributes{
			ContentType:     o.Attrs.ContentType,
			ContentEncoding: o.Attrs.ContentEncoding,
			ModTime:         o.Attrs.LastModified,
			Size:            o.Attrs.Size,
		},
	}, nil
}

// Attributes implements FS.
func (f *fs) Attributes(ctx context.Context, path string, options *storage.ReaderOptions) (*storage.Attributes, error) {
	b, err := f.bucketHandle(ctx, storage.ScopeRead)
	if err != nil {
		return nil, err
	}

	a, err := b.Object(path).Attrs(ctx)
	if err != nil {
		return nil, err
	}

	return &storage.Attributes{
		ContentType:     a.ContentType,
		ContentEncoding: a.ContentEncoding,
		Metadata:        a.Metadata,
		ModTime:         a.Updated,
		CreationTime:    a.Created,
		Size:            a.Size,
	}, nil
}

// Create implements FS.
func (f *fs) Create(ctx context.Context, path string, options *storage.WriterOptions) (io.WriteCloser, error) {
	b, err := f.bucketHandle(ctx, storage.ScopeWrite)
	if err != nil {
		return nil, err
	}

	w := b.Object(path).NewWriter(ctx)

	if options != nil {
		w.Metadata = options.Attributes.Metadata
		w.ContentType = options.Attributes.ContentType
		w.ContentEncoding = options.Attributes.ContentEncoding
		w.ChunkSize = options.BufferSize
	}
	w.ChunkSize = f.chunkSize(w.ChunkSize)

	return w, nil
}

func (f *fs) chunkSize(size int) int {
	if size == 0 {
		return googleapi.DefaultUploadChunkSize
	} else if size > 0 {
		return size
	}

	return 0 // disable buffering
}

// Delete implements FS.
func (f *fs) Delete(ctx context.Context, path string) error {
	b, err := f.bucketHandle(ctx, storage.ScopeDelete)
	if err != nil {
		return err
	}

	return b.Object(path).Delete(ctx)
}

// Walk implements FS.
func (f *fs) Walk(ctx context.Context, path string, fn storage.WalkFn) error {
	bh, err := f.bucketHandle(ctx, storage.ScopeRead)
	if err != nil {
		return err
	}

	it := bh.Objects(ctx, &gstorage.Query{
		Prefix: path,
	})

	for {
		r, err := it.Next()
		if errors.Is(err, iterator.Done) {
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

func cloudStorageScope(scope storage.Scope) string {
	switch {
	case scope.Has(storage.ScopeDelete):
		return gstorage.ScopeFullControl
	case scope.Has(storage.ScopeWrite):
		return gstorage.ScopeReadWrite
	case scope.Has(storage.ScopeRead), scope.Has(storage.ScopeSignURL):
		return gstorage.ScopeReadOnly
	default:
		panic(fmt.Sprintf("unknown scope: '%s'", scope))
	}
}

func ResolveScope(scope storage.Scope) storage.Scope {
	switch cloudStorageScope(scope) {
	case gstorage.ScopeFullControl:
		return storage.ScopeRWD | scope
	case gstorage.ScopeReadWrite:
		return storage.ScopeRW | scope
	case gstorage.ScopeReadOnly:
		return storage.ScopeRead | scope
	default:
		panic(fmt.Sprintf("unknown scope: '%s'", scope))
	}
}

func (f *fs) findCredentials(ctx context.Context, scope string) (*google.Credentials, error) {
	if f.credentials != nil {
		return f.credentials, nil
	}

	return google.FindDefaultCredentials(ctx, scope)
}

func (f *fs) client(ctx context.Context, scope storage.Scope) (*gstorage.Client, error) {
	creds, err := f.findCredentials(ctx, cloudStorageScope(scope))
	if err != nil {
		return nil, fmt.Errorf("finding credentials: %w", err)
	}

	var options []option.ClientOption
	options = append(options, option.WithCredentials(creds))
	options = append(options, option.WithScopes(cloudStorageScope(scope)))

	client, err := gstorage.NewClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("building client: %w", err)
	}

	return client, nil
}

func (f *fs) bucketHandle(ctx context.Context, scope storage.Scope) (*gstorage.BucketHandle, error) {
	f.bucketLock.RLock()
	scope |= f.bucketScopes // Expand requested scope to encompass existing scopes
	if bucket := f.bucket; bucket != nil && f.bucketScopes.Has(scope) {
		f.bucketLock.RUnlock()

		return bucket, nil
	}
	f.bucketLock.RUnlock()

	f.bucketLock.Lock()
	defer f.bucketLock.Unlock()
	if f.bucket != nil && f.bucketScopes.Has(scope) { // Race condition
		return f.bucket, nil
	}

	// Expand the requested scope to include the scopes that GCS would provide
	// e.g. Requesting Write actually provides ReadWrite.
	// Also include any scope that was previously used.
	scope = ResolveScope(f.bucketScopes | scope)

	client, err := f.client(ctx, scope)
	if err != nil {
		return nil, err
	}

	f.bucket = client.Bucket(f.bucketName)
	f.bucketScopes = scope

	return f.bucket, nil
}
