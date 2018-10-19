package storage

import (
	"strings"
)

// FromURL takes a file system path and returns a FSWalker
// corresponding to a supported storage system (cloudStorageFS,
// s3FS, or localFS if no platform-specific prefix is used).
func FromURL(path string) FS {
	if strings.HasPrefix(path, "gs://") {
		return NewCloudStorageFS(strings.TrimPrefix(path, "gs://"))
	}
	if strings.HasPrefix(path, "s3://") {
		return NewS3FS(strings.TrimPrefix(path, "s3://"))
	}
	return NewLocalFS(path)
}
