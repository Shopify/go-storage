package storage_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/Shopify/go-storage"
	"github.com/Shopify/go-storage/internal/testutils"
)

const slowDelay = 400 * time.Millisecond

func withSlowWrapper(read time.Duration, write time.Duration, cb func(storage.FS)) {
	cb(storage.NewSlowWrapper(storage.NewMemoryFS(), read, write))
}

func TestNewSlowWrapper(t *testing.T) {
	withSlowWrapper(slowDelay, slowDelay, func(fs storage.FS) {
		start := time.Now()

		// testCreate will do a Create, Open, and Attributes, so 3 calls
		testutils.Create(t, fs, "foo", "bar")
		assert.WithinDuration(t, start.Add(slowDelay*3), time.Now(), slowDelay)
	})
}
