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

	l := FromURL("/foo/bar")
	assert.Implements(t, (*FS)(nil), l)
	assert.IsType(t, (*localFS)(nil), l)
	assert.Equal(t, "/foo/bar", string(*l.(*localFS)))

	l = FromURL("file:///foo/bar")
	assert.Implements(t, (*FS)(nil), l)
	assert.IsType(t, (*localFS)(nil), l)
	assert.Equal(t, "/foo/bar", string(*l.(*localFS)))
}
