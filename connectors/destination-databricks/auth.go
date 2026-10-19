package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// bearer sets a workspace token on requests of the SQL driver and the Files
// API: the personal access token, or an OAuth token of the service principal
// fetched from the workspace's token endpoint and renewed before it expires.
type bearer struct {
	src oauth2.TokenSource
}

func newBearer(ctx context.Context, cfg Config, client *http.Client) *bearer {
	a := cfg.Authentication
	if a.AuthType != authOAuth {
		return &bearer{src: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: a.PersonalAccessToken})}
	}
	cc := clientcredentials.Config{
		ClientID:     a.ClientID,
		ClientSecret: a.Secret,
		TokenURL:     cfg.baseURL() + "/oidc/v1/token",
		Scopes:       []string{"all-apis"},
		AuthStyle:    oauth2.AuthStyleInHeader,
	}
	return &bearer{src: cc.TokenSource(context.WithValue(context.WithoutCancel(ctx), oauth2.HTTPClient, client))}
}

// Authenticate implements the SQL driver's auth.Authenticator.
func (b *bearer) Authenticate(r *http.Request) error {
	tok, err := b.token()
	if err != nil {
		return &tokenError{err: err}
	}
	r.Header.Set("Authorization", "Bearer "+tok)
	return nil
}

func (b *bearer) token() (string, error) {
	tok, err := b.src.Token()
	if err != nil {
		return "", describeTokenError(err)
	}
	return tok.AccessToken, nil
}

// describeTokenError names rejected client credentials; other failures of
// the token endpoint keep their cause.
func describeTokenError(err error) error {
	var re *oauth2.RetrieveError
	if errors.As(err, &re) && re.Response != nil {
		switch {
		case re.ErrorCode == "invalid_client" || re.Response.StatusCode == http.StatusUnauthorized:
			return fmt.Errorf("invalid client credentials: the workspace rejected client_id or secret (HTTP %d %s)", re.Response.StatusCode, re.ErrorCode)
		case re.Response.StatusCode == http.StatusNotFound:
			return fmt.Errorf("OAuth token endpoint not found at %s: check hostname (HTTP 404)", re.Response.Request.URL)
		}
		return fmt.Errorf("fetch OAuth token: HTTP %d %s %s", re.Response.StatusCode, re.ErrorCode, re.ErrorDescription)
	}
	return fmt.Errorf("fetch OAuth token: %w", err)
}
