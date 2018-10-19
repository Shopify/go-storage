package storage

import (
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/go-cloud/blob"
	"github.com/google/go-cloud/blob/s3blob"
	"golang.org/x/net/context"
)

// s3FS is an implementation of FS which uses AWS s3FS as the underlying storage layer.
type s3FS struct {
	bucket string // bucket is the name of the bucket to use as the underlying storage.
}

func NewS3FS(bucket string) FS {
	return &s3FS{bucket: bucket}
}

// Open implements FS.
func (s *s3FS) Open(ctx context.Context, path string) (*File, error) {
	b, _, err := s.bucketHandles(ctx)
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
		return nil, fmt.Errorf("s3: unable to fetch object: %v", err)
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
	b, _, err := s.bucketHandles(ctx)
	if err != nil {
		return nil, err
	}
	return b.NewWriter(ctx, path, nil)
}

// Delete implements FS.
func (s *s3FS) Delete(ctx context.Context, path string) error {
	b, _, err := s.bucketHandles(ctx)
	if err != nil {
		return err
	}
	return b.Delete(ctx, path)
}

// Walk implements FS.
func (s *s3FS) Walk(ctx context.Context, path string, fn WalkFn) error {
	_, s3c, err := s.bucketHandles(ctx)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	err = s3c.ListObjectsPagesWithContext(ctx, &s3.ListObjectsInput{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(path),
	}, func(page *s3.ListObjectsOutput, last bool) bool {
		for _, obj := range page.Contents {
			if err := fn(*obj.Key); err != nil {
				errCh <- err
				return false
			}
		}
		return last
	})
	if err != nil {
		return fmt.Errorf("s3: unable to walk: %v", err)
	}

	close(errCh)
	return <-errCh
}

const bucketRegionHint = endpoints.UsEast1RegionID

func (s *s3FS) bucketHandles(ctx context.Context) (*blob.Bucket, *s3.S3, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, nil, fmt.Errorf("s3: unable to create session: %v", err)
	}

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#GetBucketRegion
	region := aws.StringValue(sess.Config.Region)
	if len(region) == 0 {
		region, err = s3manager.GetBucketRegion(ctx, sess, s.bucket, bucketRegionHint)
		if err != nil {
			return nil, nil, fmt.Errorf("s3: unable to find bucket region: %v", err)
		}
	}

	c := aws.NewConfig().WithRegion(region)
	sess = sess.Copy(c)

	b, err := s3blob.OpenBucket(ctx, sess, s.bucket)
	if err != nil {
		return nil, nil, fmt.Errorf("s3: could not open %q: %v", s.bucket, err)
	}
	s3c := s3.New(sess, c)

	return b, s3c, nil
}
