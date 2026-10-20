package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	apioption "google.golang.org/api/option"
)

// gcsStore is the objectStore of one Cloud Storage bucket.
type gcsStore struct {
	client *storage.Client
	bucket *storage.BucketHandle
	creds  *auth.Credentials
}

// resolveCredentials returns the configured credentials, scoped to read-only
// Cloud Storage access. A service account key's tokens come from tokenURL.
// Tokens are fetched lazily, on the first request or on fetchToken.
func resolveCredentials(cr Credentials, tokenURL string) (*auth.Credentials, error) {
	opts := &credentials.DetectOptions{Scopes: []string{storage.ScopeReadOnly}}
	switch cr.AuthType {
	case authService:
		key, _, err := checkGoogleKey([]byte(cr.ServiceAccountInfo), tokenURL, serviceAccountType)
		if err != nil {
			return nil, fmt.Errorf("service_account_info is not a valid service account key: %w", err)
		}
		creds, err := credentials.NewCredentialsFromJSON(credentials.ServiceAccount, key, opts)
		if err != nil {
			return nil, fmt.Errorf("service_account_info is not a valid service account key: %w", err)
		}
		return creds, nil
	case authApplicationDefault:
		creds, err := credentials.DetectDefault(opts)
		if err != nil {
			return nil, fmt.Errorf("no application default credentials found: %w", err)
		}
		return creds, nil
	}
	return nil, fmt.Errorf("credentials auth_type %q is not one of %v", cr.AuthType, authTypes)
}

// newGCSStore builds a client for cfg's bucket that fetches a service account
// key's tokens from tokenURL. extra goes after the credentials, so tests can
// point the client at a fake server.
func newGCSStore(ctx context.Context, cfg Config, tokenURL string, extra ...apioption.ClientOption) (*gcsStore, error) {
	creds, err := resolveCredentials(cfg.Credentials, tokenURL)
	if err != nil {
		return nil, err
	}
	opts := append([]apioption.ClientOption{apioption.WithAuthCredentials(creds)}, extra...)
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create Cloud Storage client: %w", err)
	}
	return &gcsStore{client: client, bucket: client.Bucket(cfg.Bucket), creds: creds}, nil
}

func (s *gcsStore) fetchToken(ctx context.Context) error {
	_, err := s.creds.Token(ctx)
	return err
}

// List returns every object under prefix.
func (s *gcsStore) List(ctx context.Context, prefix string) ([]object, error) {
	var out []object
	err := s.walk(ctx, prefix, func(o object) bool {
		out = append(out, o)
		return true
	})
	return out, err
}

// walk calls fn for each object under prefix in name order until fn returns
// false.
func (s *gcsStore) walk(ctx context.Context, prefix string, fn func(object) bool) error {
	q := &storage.Query{Prefix: prefix}
	if err := q.SetAttrSelection([]string{"Name", "Size", "Updated"}); err != nil {
		return err
	}
	it := s.bucket.Objects(ctx, q)
	for {
		a, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
		if !fn(object{Key: a.Name, Size: a.Size, LastModified: a.Updated.UTC()}) {
			return nil
		}
	}
}

// Open streams the object's stored bytes. Objects uploaded with
// Content-Encoding gzip come back compressed, as the compression setting
// expects, instead of transcoded.
func (s *gcsStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.bucket.Object(key).ReadCompressed(true).NewReader(ctx)
}

// readFirstByte fetches the first byte of an object, which proves read
// permission without downloading it. An empty object has no byte range.
func (s *gcsStore) readFirstByte(ctx context.Context, o object) error {
	length := int64(-1)
	if o.Size > 0 {
		length = 1
	}
	r, err := s.bucket.Object(o.Key).ReadCompressed(true).NewRangeReader(ctx, 0, length)
	if err != nil {
		return err
	}
	_, err = io.Copy(io.Discard, r)
	return errors.Join(err, r.Close())
}
