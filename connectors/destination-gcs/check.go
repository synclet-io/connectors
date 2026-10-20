package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"go.uber.org/multierr"
	"golang.org/x/oauth2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// Check verifies the bucket exists and that the credentials may write,
// delete and list objects under bucket_path, by writing and deleting a
// probe object.
func (d *GCSDestination) Check(ctx context.Context, cfg Config) (rerr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	client, err := d.newClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(client))
	return checkBucket(ctx, client.Bucket(cfg.Bucket), cfg)
}

// checkBucket needs only object permissions (roles/storage.objectAdmin on
// the bucket is enough), so it does not read the bucket's metadata.
func checkBucket(ctx context.Context, bucket *storage.BucketHandle, cfg Config) error {
	probe := "_synclet_check_" + uuid.NewString()
	if p := cfg.keyPrefix(); p != "" {
		probe = p + "/" + probe
	}
	w := bucket.Object(probe).NewWriter(ctx)
	_, err := w.Write([]byte("synclet"))
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return describeError(err, cfg.Bucket, "no write permission", probe)
	}
	if err := bucket.Object(probe).Delete(ctx); err != nil {
		return describeError(err, cfg.Bucket, "no delete permission", probe)
	}
	prefix := cfg.keyPrefix()
	if prefix != "" {
		prefix += "/"
	}
	it := bucket.Objects(ctx, &storage.Query{Prefix: prefix})
	it.PageInfo().MaxSize = 1
	if _, err := it.Next(); err != nil && !errors.Is(err, iterator.Done) {
		return describeError(err, cfg.Bucket, "no list permission", prefix)
	}
	return nil
}

// describeError names the cause of a failed request: invalid credentials, a
// missing bucket, or denied, which is reported as denied.
func describeError(err error, bucket, denied, key string) error {
	var re *oauth2.RetrieveError
	if errors.As(err, &re) {
		return fmt.Errorf("invalid credentials: %w", err)
	}
	if errors.Is(err, storage.ErrBucketNotExist) {
		return fmt.Errorf("bucket %q does not exist", bucket)
	}
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		switch ge.Code {
		case http.StatusUnauthorized:
			return fmt.Errorf("invalid credentials: %s", ge.Message)
		case http.StatusNotFound:
			return fmt.Errorf("bucket %q does not exist", bucket)
		case http.StatusForbidden:
			return fmt.Errorf("%s on gs://%s/%s: %s", denied, bucket, key, ge.Message)
		}
	}
	return fmt.Errorf("gs://%s/%s: %w", bucket, key, err)
}
