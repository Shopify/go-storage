package storage

import (
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/net/context"
)

// DefaultLocalCreatePathMode is the default os.FileMode used when creating directories
// during a Local.Create call.
const DefaultLocalCreatePathMode = os.FileMode(0755)

// LocalCreatePathMode is the os.FileMode used when creating directories via Local.Create
var LocalCreatePathMode = DefaultLocalCreatePathMode

// Local is a local FS and Walker implementation.
type Local string

func (l Local) fullPath(path string) string {
	return filepath.Join(string(l), path)
}

func (l Local) wrapError(path string, err error) error {
	if os.IsNotExist(err) {
		return &notExistError{
			Path: path,
		}
	}
	return err
}

// Open implements FS.
func (l Local) Open(_ context.Context, path string) (*File, error) {
	path = l.fullPath(path)
	stat, err := os.Stat(path)
	if err != nil {
		return nil, l.wrapError(path, err)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, l.wrapError(path, err)
	}

	return &File{
		ReadCloser: f,
		Name:       stat.Name(),
		ModTime:    stat.ModTime(),
		Size:       stat.Size(),
	}, nil
}

// Create implements FS.  If the path contains any directories which do not already exist
// then Create will try to make them, returning an error if it fails.
func (l Local) Create(_ context.Context, path string) (io.WriteCloser, error) {
	dir := l.fullPath(filepath.Dir(path))
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, LocalCreatePathMode); err != nil {
			return nil, err
		}
	}

	f, err := os.Create(l.fullPath(path))
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Delete implements FS.
func (l Local) Delete(_ context.Context, path string) error {
	return os.Remove(l.fullPath(path))
}

// Walk implements Walker.
func (l Local) Walk(_ context.Context, path string, fn WalkFn) error {
	return filepath.Walk(l.fullPath(path), func(path string, f os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !f.IsDir() {
			path = strings.TrimPrefix(path, string(l))
			return fn(path)
		}
		return nil
	})
}
