package storage

import (
	"fmt"
	"io"

	"google.golang.org/cloud"
	"google.golang.org/cloud/storage"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
)

// CloudStorage implements FS and uses Google Cloud Storage as the underlying
// file storage.
type CloudStorage struct {
	Bucket string
}

// Open implements FS.
func (c *CloudStorage) Open(ctx context.Context, path string) (*File, error) {
	bh, err := c.bucketHandle(ctx, storage.ScopeReadOnly)
	if err != nil {
		return nil, err
	}

	obj := bh.Object(path)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return nil, &notExistError{
				Path: path,
			}
		}
		return nil, fmt.Errorf("cloud storage: error fetching object attributes: %v", err)
	}

	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloud storage: error fetching object reader for '%v': %v", path, err)
	}

	return &File{
		ReadCloser: r,
		Name:       attrs.Name,
		ModTime:    attrs.Updated,
		Size:       attrs.Size,
	}, nil
}

// Create implements FS.
func (c *CloudStorage) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	bh, err := c.bucketHandle(ctx, storage.ScopeReadWrite)
	if err != nil {
		return nil, err
	}

	return bh.Object(path).NewWriter(ctx), nil
}

// Delete implements FS.
func (c *CloudStorage) Delete(ctx context.Context, path string) error {
	bh, err := c.bucketHandle(ctx, storage.ScopeReadWrite)
	if err != nil {
		return err
	}
	return bh.Object(path).Delete(ctx)
}

// Walk implements FS.
func (c *CloudStorage) Walk(ctx context.Context, path string, fn WalkFn) error {
	q := &storage.Query{
		Prefix: path,
	}

	bh, err := c.bucketHandle(ctx, storage.ScopeReadOnly)
	if err != nil {
		return err
	}

	for q != nil {
		l, err := bh.List(ctx, q)
		if err != nil {
			return err
		}

		for _, r := range l.Results {
			if err = fn(r.Name); err != nil {
				return err
			}
		}

		q = l.Next
	}
	return nil
}

func (c *CloudStorage) bucketHandle(ctx context.Context, scope string) (*storage.BucketHandle, error) {
	ts, err := google.DefaultTokenSource(ctx, scope)
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to retrieve default token source: %v", err)
	}

	client, err := storage.NewClient(ctx, cloud.WithTokenSource(ts))
	if err != nil {
		return nil, fmt.Errorf("cloud storage: unable to get client: %v", err)
	}

	return client.Bucket(c.Bucket), nil
}
