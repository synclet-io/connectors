package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const bigqueryScope = "https://www.googleapis.com/auth/bigquery"

// credentialsOption returns the client option that authenticates as the
// configured credentials.
func credentialsOption(ctx context.Context, creds CredentialsConfig) (option.ClientOption, error) {
	switch creds.AuthType {
	case authService:
		//nolint:staticcheck // SA1019: service_account_info accepts any Google credential JSON.
		cred, err := google.CredentialsFromJSON(ctx, []byte(creds.ServiceAccountInfo), bigqueryScope)
		if err != nil {
			return nil, fmt.Errorf("service_account_info is not a valid service account key: %w", err)
		}
		return option.WithCredentials(cred), nil
	case authApplicationDefault:
		cred, err := google.FindDefaultCredentials(ctx, bigqueryScope)
		if err != nil {
			return nil, fmt.Errorf("find application default credentials: %w", err)
		}
		return option.WithCredentials(cred), nil
	default:
		return nil, fmt.Errorf("unsupported auth_type %q", creds.AuthType)
	}
}

// client validates cfg and returns a BigQuery client for its project that
// runs jobs in its location. Configuration and credential errors are
// synclet.ConfigError.
func (s Source) client(ctx context.Context, cfg Config) (*bigquery.Client, error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	opts := s.clientOpts
	if opts == nil {
		opt, err := credentialsOption(ctx, cfg.Credentials)
		if err != nil {
			return nil, synclet.ConfigError(err)
		}
		opts = []option.ClientOption{opt}
	}
	c, err := bigquery.NewClient(ctx, cfg.ProjectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("create BigQuery client: %w", err)
	}
	c.Location = cfg.Location
	return c, nil
}
