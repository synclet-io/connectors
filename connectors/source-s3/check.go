package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/smithy-go"
	"github.com/bmatcuk/doublestar/v4"
)

// Check resolves the credentials (assuming the role, when configured),
// lists each stream's prefix and reads the first byte of its first matching
// object. The error names the cause: invalid credentials, a role that
// cannot be assumed, a missing bucket, or no list or read permission.
func (Source) Check(ctx context.Context, cfg Config) error {
	if err := validConfig(&cfg); err != nil {
		return err
	}
	store, err := newS3Store(ctx, cfg)
	if err != nil {
		return err
	}
	if err := store.retrieveCredentials(ctx); err != nil {
		if cfg.Credentials.AuthType == authAssumeRole {
			return fmt.Errorf("cannot assume role %q: %w", cfg.Credentials.RoleARN, describeCredentialError(err))
		}
		return fmt.Errorf("cannot resolve credentials: %w", err)
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

func errorCode(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func isCredentialCode(code string) bool {
	switch code {
	case "InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidClientTokenId", "InvalidToken", "ExpiredToken":
		return true
	}
	return false
}

func describeCredentialError(err error) error {
	if code := errorCode(err); isCredentialCode(code) {
		return fmt.Errorf("invalid credentials (%s): check the access key ID and secret: %w", code, err)
	}
	return err
}

func describeListError(err error, bucket, prefix string) error {
	code := errorCode(err)
	switch {
	case isCredentialCode(code):
		return describeCredentialError(err)
	case code == "NoSuchBucket":
		return fmt.Errorf("bucket %q does not exist: %w", bucket, err)
	case code == "AccessDenied":
		return fmt.Errorf("no list permission on prefix %q of bucket %q (s3:ListBucket): %w", prefix, bucket, err)
	case code == "PermanentRedirect" || code == "AuthorizationHeaderMalformed":
		return fmt.Errorf("bucket %q is in another region; set region: %w", bucket, err)
	}
	return fmt.Errorf("list prefix %q of bucket %q: %w", prefix, bucket, err)
}

func describeReadError(err error, key string) error {
	code := errorCode(err)
	switch {
	case isCredentialCode(code):
		return describeCredentialError(err)
	case code == "AccessDenied":
		return fmt.Errorf("no read permission on %q (s3:GetObject): %w", key, err)
	}
	return fmt.Errorf("read %q: %w", key, err)
}
