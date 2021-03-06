package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFromURL(t *testing.T) {
	cs := FromURL("gs://foo/bar")
	assert.Implements(t, (*FS)(nil), cs)
	assert.IsType(t, (*cloudStorageFS)(nil), cs)
	assert.Equal(t, "foo/bar", cs.(*cloudStorageFS).bucketName)

	s3 := FromURL("s3://foo/bar")
	assert.Implements(t, (*FS)(nil), s3)
	assert.IsType(t, (*s3FS)(nil), s3)
	assert.Equal(t, "foo/bar", s3.(*s3FS).bucket)

	l := FromURL("/foo/bar")
	assert.Implements(t, (*FS)(nil), l)
	assert.IsType(t, (*localFS)(nil), l)
	assert.Equal(t, "/foo/bar", string(*l.(*localFS)))

	l = FromURL("file:///foo/bar")
	assert.Implements(t, (*FS)(nil), l)
	assert.IsType(t, (*localFS)(nil), l)
	assert.Equal(t, "/foo/bar", string(*l.(*localFS)))
}
