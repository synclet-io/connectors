package main

import (
	"context"
	"io"
	"strings"
	"time"
)

// object is one listed object of a store.
type object struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// objectStore lists and opens the objects of one bucket. The S3
// implementation is s3Store; a sibling connector for another storage
// (GCS, Azure Blob, SFTP) implements this interface and keeps the rest of
// the connector as is.
type objectStore interface {
	// List returns every object whose key starts with prefix.
	List(ctx context.Context, prefix string) ([]object, error)
	// Open returns the content of the object with the given key.
	Open(ctx context.Context, key string) (io.ReadCloser, error)
}

// globPrefix returns the literal part of a glob before its first meta
// character, the prefix that lists every key the glob can match.
func globPrefix(glob string) string {
	if i := strings.IndexAny(glob, `*?[{\`); i >= 0 {
		return glob[:i]
	}
	return glob
}
