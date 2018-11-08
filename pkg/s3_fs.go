package storage

import (
	"context"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/s3blob"
	"github.com/pkg/errors"
)

func NewS3FS(bucket string) FS {
	return &s3FS{bucket: bucket}
}

// s3FS is an implementation of FS which uses AWS s3FS as the underlying storage layer.
type s3FS struct {
	bucket string // bucket is the name of the bucket to use as the underlying storage.
}

// Open implements FS.
func (s *s3FS) Open(ctx context.Context, path string) (*File, error) {
	b, err := s.bucketHandles(ctx)
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
		return nil, errors.Wrap(err, "s3: unable to fetch object")
	}

	return &File{
		ReadCloser: f,
		Name:       path,
		Size:       f.Size(),
		ModTime:    f.ModTime(),
	}, nil
}

// Create implements FS.
func (s *s3FS) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	b, err := s.bucketHandles(ctx)
	if err != nil {
		return nil, err
	}
	return b.NewWriter(ctx, path, nil)
}

// Delete implements FS.
func (s *s3FS) Delete(ctx context.Context, path string) error {
	b, err := s.bucketHandles(ctx)
	if err != nil {
		return err
	}
	return b.Delete(ctx, path)
}

// Walk implements FS.
func (s *s3FS) Walk(ctx context.Context, path string, fn WalkFn) error {
	bh, err := s.bucketHandles(ctx)
	if err != nil {
		return err
	}

	it, err := bh.List(ctx, &blob.ListOptions{
		Prefix: path,
	})
	if err != nil {
		return err
	}

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

const bucketRegionHint = endpoints.UsEast1RegionID

func (s *s3FS) bucketHandles(ctx context.Context) (*blob.Bucket, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, errors.Wrap(err, "s3: unable to create session")
	}

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#GetBucketRegion
	region := aws.StringValue(sess.Config.Region)
	if len(region) == 0 {
		region, err = s3manager.GetBucketRegion(ctx, sess, s.bucket, bucketRegionHint)
		if err != nil {
			return nil, errors.Wrap(err, "s3: unable to find bucket region")
		}
	}

	c := aws.NewConfig().WithRegion(region)
	sess = sess.Copy(c)

	b, err := s3blob.OpenBucket(ctx, s.bucket, sess, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "s3: could not open %q", s.bucket)
	}

	return b, nil
}

func (s *s3FS) URL(ctx context.Context, path string, options *SignedURLOptions) (string, error) {
	return "", ErrNotImplemented
}