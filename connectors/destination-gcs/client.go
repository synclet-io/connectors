package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const readWriteScope = "https://www.googleapis.com/auth/devstorage.read_write"

// newGCSClient builds a Cloud Storage client authenticated with the
// configured service account key or Application Default Credentials. It
// fetches the first access token up front, so a key that cannot sign in
// fails as invalid credentials; the client reuses that token.
func newGCSClient(ctx context.Context, cfg Config) (*storage.Client, error) {
	var creds *google.Credentials
	var err error
	switch cfg.Credentials.AuthType {
	case authServiceAccount:
		creds, err = google.CredentialsFromJSONWithType(ctx, []byte(cfg.Credentials.ServiceAccountInfo), google.ServiceAccount, readWriteScope)
		if err != nil {
			return nil, fmt.Errorf("credentials: parsing service_account_info: %w", err)
		}
	case authApplicationDefault:
		creds, err = google.FindDefaultCredentials(ctx, readWriteScope)
		if err != nil {
			return nil, fmt.Errorf("credentials: finding Application Default Credentials: %w", err)
		}
	default:
		return nil, fmt.Errorf("credentials: unsupported auth_type %q", cfg.Credentials.AuthType)
	}
	if _, err := creds.TokenSource.Token(); err != nil {
		return nil, fmt.Errorf("invalid credentials: %w", err)
	}
	client, err := storage.NewClient(ctx, option.WithCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("creating Cloud Storage client: %w", err)
	}
	return client, nil
}
