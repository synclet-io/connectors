package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/storage"
	"github.com/bmatcuk/doublestar/v4"
	"google.golang.org/api/googleapi"
)

// Check resolves the credentials and fetches a token, lists each stream's
// prefix and reads the first byte of its first matching object. The error
// names the cause: a malformed or revoked key, no application default
// credentials, a missing bucket, or no list or read permission.
func (src Source) Check(ctx context.Context, cfg Config) (rerr error) {
	if err := validConfig(&cfg); err != nil {
		return err
	}
	store, err := newGCSStore(ctx, cfg, src.tokenEndpoint(), src.clientOptions...)
	if err != nil {
		return err
	}
	defer func() { rerr = errors.Join(rerr, store.client.Close()) }()
	if err := store.fetchToken(ctx); err != nil {
		return describeCredentialError(err)
	}
	for _, s := range cfg.Streams {
		prefix := globPrefix(s.Glob)
		var first *object
		err := store.walk(ctx, prefix, func(o object) bool {
			if ok, _ := doublestar.Match(s.Glob, o.Key); ok {
				first = &o
				return false
			}
			return true
		})
		if err != nil {
			return describeListError(err, cfg.Bucket, prefix)
		}
		if first == nil {
			if s.Schema == "" {
				return fmt.Errorf("stream %q: no object matches glob %q, so its schema cannot be inferred; add files or give the stream a schema", s.Name, s.Glob)
			}
			continue
		}
		if err := store.readFirstByte(ctx, *first); err != nil {
			return describeReadError(err, first.Key)
		}
	}
	return nil
}

// statusCode is the HTTP status of a failed Cloud Storage request, or 0.
func statusCode(err error) int {
	var ge *googleapi.Error
	if errors.As(err, &ge) {
		return ge.Code
	}
	return 0
}

func isTokenError(err error) bool {
	var ae *auth.Error
	return errors.As(err, &ae) || statusCode(err) == http.StatusUnauthorized
}

func describeCredentialError(err error) error {
	if isTokenError(err) {
		return fmt.Errorf("invalid credentials: the token request was refused; check that the service account key is current and not deleted or disabled: %w", err)
	}
	return fmt.Errorf("cannot fetch an access token: %w", err)
}

func describeListError(err error, bucket, prefix string) error {
	code := statusCode(err)
	switch {
	case isTokenError(err):
		return describeCredentialError(err)
	case errors.Is(err, storage.ErrBucketNotExist) || code == http.StatusNotFound:
		return fmt.Errorf("bucket %q does not exist: %w", bucket, err)
	case code == http.StatusForbidden:
		return fmt.Errorf("no list permission on prefix %q of bucket %q (storage.objects.list): %w", prefix, bucket, err)
	}
	return fmt.Errorf("list prefix %q of bucket %q: %w", prefix, bucket, err)
}

func describeReadError(err error, key string) error {
	switch {
	case isTokenError(err):
		return describeCredentialError(err)
	case statusCode(err) == http.StatusForbidden:
		return fmt.Errorf("no read permission on %q (storage.objects.get): %w", key, err)
	}
	return fmt.Errorf("read %q: %w", key, err)
}
