package sqlite

import (
	"testing"

	"github.com/go-zoox/kv/test"
)

func createClient() *SQLite {
	client, err := New(&SQLiteConfig{
		Path:   "/tmp/test.db",
		Prefix: "go-zoox-test:",
	})
	if err != nil {
		panic(err)
	}

	return client
}

func TestKV(t *testing.T) {
	test.RunTestCases(t, createClient(), []string{"maxAge"})
}
