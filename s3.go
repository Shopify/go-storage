package storage

import (
	"fmt"
	"io"

	"golang.org/x/net/context"
)

// S3 is an implementation of FS which uses AWS S3 as the underlying storage layer.
type S3 struct {
	Bucket string // Bucket is the name of the bucket to use as the underlying storage.
}

// Open implements FS.
func (s *S3) Open(ctx context.Context, path string) (*File, error) {
	return nil, fmt.Errorf("Open not implemented for S3")
}

// Create implements FS.
func (s *S3) Create(ctx context.Context, path string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("Create not implemented for S3")
}

// Delete implements FS.
func (s *S3) Delete(ctx context.Context, path string) error {
	return fmt.Errorf("Delete not implemented for S3")
}

// Walk implements FS.
func (s *S3) Walk(ctx context.Context, path string, fn WalkFn) error {
	return fmt.Errorf("Walk not implemented for S3")
}
