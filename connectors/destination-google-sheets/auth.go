package main

import (
	"context"
	"fmt"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

// createClientOption returns an option.ClientOption for authenticating with Google APIs
// based on the configured auth_type.
func createClientOption(ctx context.Context, creds CredentialsConfig, scope string) (option.ClientOption, error) {
	switch creds.AuthType {
	case "Client":
		conf := &oauth2.Config{
			ClientID:     creds.ClientID,
			ClientSecret: creds.ClientSecret,
			Endpoint:     google.Endpoint,
		}
		token := &oauth2.Token{RefreshToken: creds.RefreshToken}
		ts := conf.TokenSource(ctx, token)
		return option.WithTokenSource(ts), nil

	case "Service":
		key, typ, err := checkGoogleKey([]byte(creds.ServiceAccountInfo), googleTokenURL, serviceAccountType, authorizedUserType)
		if err != nil {
			return nil, fmt.Errorf("service_account_info is not a valid service account key: %w", err)
		}
		cred, err := google.CredentialsFromJSONWithType(ctx, key, google.CredentialsType(typ), scope)
		if err != nil {
			return nil, fmt.Errorf("parsing service account: %w", err)
		}
		return option.WithCredentials(cred), nil

	case "ApplicationDefault":
		cred, err := google.FindDefaultCredentials(ctx, scope)
		if err != nil {
			return nil, fmt.Errorf("finding default credentials: %w", err)
		}
		return option.WithCredentials(cred), nil

	default:
		return nil, fmt.Errorf("unsupported auth_type: %s", creds.AuthType)
	}
}
