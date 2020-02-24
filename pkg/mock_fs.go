package storage

import (
	"context"
	"io"

	"github.com/stretchr/testify/mock"
)

// NewMockFS creates an FS where each method can be mocked.
// To be used in tests.
func NewMockFS() *mockFS {
	return &mockFS{}
}

type mockFS struct {
	mock.Mock
}

func (m *mockFS) Walk(ctx context.Context, path string, fn WalkFn) error {
	args := m.Called(ctx, path, fn)
	return args.Error(0)
}

func (m *mockFS) Open(ctx context.Context, path string, options *ReaderOptions) (*File, error) {
	args := m.Called(ctx, path, options)
	file := args.Get(0)
	err := args.Error(1)
	if file == nil {
		return nil, err
	}
	return file.(*File), err
}

func (m *mockFS) Attributes(ctx context.Context, path string, options *ReaderOptions) (*Attributes, error) {
	args := m.Called(ctx, path, options)
	attrs := args.Get(0)
	err := args.Error(1)
	if attrs == nil {
		return nil, err
	}
	return attrs.(*Attributes), err
}

func (m *mockFS) Create(ctx context.Context, path string, options *WriterOptions) (io.WriteCloser, error) {
	args := m.Called(ctx, path, options)
	w := args.Get(0)
	err := args.Error(1)
	if w == nil {
		return nil, err
	}
	return w.(io.WriteCloser), err
}

func (m *mockFS) Delete(ctx context.Context, path string) error {
	args := m.Called(ctx, path)
	return args.Error(0)
}

func (m *mockFS) URL(ctx context.Context, path string, options *SignedURLOptions) (string, error) {
	args := m.Called(ctx, path, options)
	url := args.String(0)
	err := args.Error(1)
	return url, err
}
