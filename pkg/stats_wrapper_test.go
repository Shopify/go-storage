package storage_test

import (
	"crypto/sha1"
	"expvar"
	"fmt"
	"github.com/Shopify/go-storage/pkg"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewStatsWrapper(t *testing.T) {
	withLocal(func(localFS storage.FS) {
		randomBytes := make([]byte, 16)
		testStats := fmt.Sprintf("test-go-storage-%x", sha1.New().Sum(randomBytes))
		fs := storage.NewStatsWrapper(localFS, testStats)

		testDelete(t, fs, "foo")

		stats := expvar.Get(testStats).(*expvar.Map)
		assert.Equal(t, int64(2), stats.Get(storage.StatOpenTotal).(*expvar.Int).Value())
		assert.Equal(t, int64(1), stats.Get(storage.StatOpenErrors).(*expvar.Int).Value())
		assert.Equal(t, int64(1), stats.Get(storage.StatCreateTotal).(*expvar.Int).Value())
		assert.Equal(t, int64(0), stats.Get(storage.StatCreateErrors).(*expvar.Int).Value())
		assert.Equal(t, int64(1), stats.Get(storage.StatDeleteTotal).(*expvar.Int).Value())
		assert.Equal(t, int64(0), stats.Get(storage.StatDeleteErrors).(*expvar.Int).Value())
	})
}
