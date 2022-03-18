package storage

import (
	"errors"
	"fmt"
)

var ErrNotImplemented = errors.New("not implemented")

// IsNotExist returns a boolean indicating whether the error is known to report that
// a path does not exist.
func IsNotExist(err error) bool {
	var e *NotExistError

	return errors.As(err, &e)
}

// NotExistError is returned from FS.Open implementations when a requested
// path does not exist.
type NotExistError struct {
	Path string
}

// Error implements error
func (e *NotExistError) Error() string {
	return fmt.Sprintf("storage %v: path does not exist", e.Path)
}
