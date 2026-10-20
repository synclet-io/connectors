package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
)

const deleteConcurrency = 16

// objectStore is the bucket the writer uploads to.
type objectStore interface {
	put(ctx context.Context, key string, body io.Reader) error
	list(ctx context.Context, prefix string) ([]string, error)
	delete(ctx context.Context, keys []string) error
}

// gcsStore is an objectStore on one Cloud Storage bucket; uploads are
// resumable, in chunks of the client's default size. Every request is
// retried on transient errors: an object name is written once per sync, so
// repeating an upload or a delete is safe.
type gcsStore struct {
	bucket *storage.BucketHandle
	name   string
}

func newGCSStore(client *storage.Client, bucket string) *gcsStore {
	return &gcsStore{bucket: client.Bucket(bucket).Retryer(storage.WithPolicy(storage.RetryAlways)), name: bucket}
}

func (s *gcsStore) put(ctx context.Context, key string, body io.Reader) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w := s.bucket.Object(key).NewWriter(ctx)
	if _, err := io.Copy(w, body); err != nil {
		cancel()
		_ = w.Close()
		return fmt.Errorf("uploading gs://%s/%s: %w", s.name, key, err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("uploading gs://%s/%s: %w", s.name, key, err)
	}
	return nil
}

func (s *gcsStore) list(ctx context.Context, prefix string) ([]string, error) {
	q := &storage.Query{Prefix: prefix}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, err
	}
	var keys []string
	it := s.bucket.Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return keys, nil
		}
		if err != nil {
			return nil, fmt.Errorf("listing gs://%s/%s: %w", s.name, prefix, err)
		}
		keys = append(keys, attrs.Name)
	}
}

// delete removes keys concurrently; a key that is already gone is not an
// error.
func (s *gcsStore) delete(ctx context.Context, keys []string) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(deleteConcurrency)
	for _, k := range keys {
		g.Go(func() error {
			err := s.bucket.Object(k).Delete(ctx)
			if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
				return fmt.Errorf("deleting gs://%s/%s: %w", s.name, k, err)
			}
			return nil
		})
	}
	return g.Wait()
}
