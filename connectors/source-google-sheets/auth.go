package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"sync"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
)

const (
	sheetsScope = "https://www.googleapis.com/auth/spreadsheets.readonly"
	driveScope  = "https://www.googleapis.com/auth/drive.metadata.readonly"
)

// parseServiceAccountKey reads a service account JSON key and checks that
// its private key is a PEM RSA key, so a malformed key fails before any
// request.
func parseServiceAccountKey(raw []byte) (*jwt.Config, error) {
	invalid := func(err error) error {
		return fmt.Errorf("service_account_info is not a valid service account key: %w", err)
	}
	conf, err := google.JWTConfigFromJSON(raw, sheetsScope, driveScope)
	if err != nil {
		return nil, invalid(err)
	}
	if conf.Email == "" {
		return nil, invalid(errors.New("client_email is empty"))
	}
	block, _ := pem.Decode(conf.PrivateKey)
	if block == nil {
		return nil, invalid(errors.New("private_key is not PEM encoded"))
	}
	if _, err := x509.ParsePKCS8PrivateKey(block.Bytes); err != nil {
		if _, err1 := x509.ParsePKCS1PrivateKey(block.Bytes); err1 != nil {
			return nil, invalid(fmt.Errorf("private_key: %w", err))
		}
	}
	return conf, nil
}

// tokenSource hands out access tokens of the configured credentials,
// caching each until shortly before it expires.
type tokenSource struct {
	// serviceAccount is set for a service account key.
	serviceAccount bool
	authType       string
	// account names who the credentials act as, for error messages.
	account string
	newTS   func() oauth2.TokenSource

	mu sync.Mutex
	ts oauth2.TokenSource
}

// newTokenSource returns the token source of cr. tokenURL replaces the token
// endpoint of the OAuth client and of a service account key when it is not
// empty.
func newTokenSource(ctx context.Context, hc *http.Client, tokenURL string, cr CredentialsConfig) (*tokenSource, error) {
	ctx = context.WithValue(ctx, oauth2.HTTPClient, hc)
	t := &tokenSource{authType: cr.AuthType}
	switch cr.AuthType {
	case authClient:
		t.account = "the Google user of the refresh token"
		endpoint := google.Endpoint
		if tokenURL != "" {
			endpoint.TokenURL = tokenURL
		}
		conf := &oauth2.Config{ClientID: cr.ClientID, ClientSecret: cr.ClientSecret, Endpoint: endpoint}
		t.newTS = func() oauth2.TokenSource {
			return conf.TokenSource(ctx, &oauth2.Token{RefreshToken: cr.RefreshToken})
		}
	case authService:
		raw := []byte(cr.ServiceAccountInfo)
		var kind struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &kind); err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("service_account_info is not a Google credential JSON: %w", err))
		}
		if kind.Type == "service_account" {
			conf, err := parseServiceAccountKey(raw)
			if err != nil {
				return nil, synclet.ConfigError(err)
			}
			if tokenURL != "" {
				conf.TokenURL = tokenURL
			}
			t.serviceAccount = true
			t.account = "service account " + conf.Email
			t.newTS = func() oauth2.TokenSource { return conf.TokenSource(ctx) }
			break
		}
		//nolint:staticcheck // SA1019: service_account_info accepts any Google credential JSON.
		creds, err := google.CredentialsFromJSON(ctx, raw, sheetsScope, driveScope)
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("service_account_info is not a valid Google credential JSON: %w", err))
		}
		t.account = "the Google account of service_account_info"
		t.newTS = func() oauth2.TokenSource { return creds.TokenSource }
	case authApplicationDefault:
		creds, err := google.FindDefaultCredentials(ctx, sheetsScope, driveScope)
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("no Application Default Credentials found: %w", err))
		}
		t.account = "the Application Default Credentials"
		t.newTS = func() oauth2.TokenSource { return creds.TokenSource }
	default:
		return nil, synclet.ConfigError(fmt.Errorf("unsupported auth_type: %q", cr.AuthType))
	}
	t.ts = t.newTS()
	return t, nil
}

// get returns a cached access token, or a new one.
func (t *tokenSource) get() (string, error) {
	t.mu.Lock()
	ts := t.ts
	t.mu.Unlock()
	tok, err := ts.Token()
	if err != nil {
		return "", t.classify(err)
	}
	return tok.AccessToken, nil
}

// invalidate drops the cached token, so the next get fetches a new one.
func (t *tokenSource) invalidate() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ts = t.newTS()
}

// classify turns a token endpoint failure into a configuration error that
// names the credential at fault, a retryable error for a 5xx response or a
// transport failure, or the endpoint's answer.
func (t *tokenSource) classify(err error) error {
	var re *oauth2.RetrieveError
	if !errors.As(err, &re) {
		return retryable{err: fmt.Errorf("token endpoint: %w", err)}
	}
	code, desc := re.ErrorCode, re.ErrorDescription
	if code == "" {
		var body struct {
			Error       string `json:"error"`
			Description string `json:"error_description"`
		}
		if json.Unmarshal(re.Body, &body) == nil {
			code, desc = body.Error, body.Description
		}
	}
	status := 0
	if re.Response != nil {
		status = re.Response.StatusCode
	}
	cause := fmt.Errorf("token endpoint: HTTP %d %s: %s", status, code, desc)
	credential := code == "invalid_grant" || code == "invalid_client" || code == "unauthorized_client"
	switch {
	case credential && t.serviceAccount:
		return synclet.ConfigError(fmt.Errorf("service account key is invalid, disabled or deleted (%w)", cause))
	case credential && t.authType == authService:
		return synclet.ConfigError(fmt.Errorf("the credentials in service_account_info are invalid or revoked (%w)", cause))
	case credential && t.authType == authApplicationDefault:
		return synclet.ConfigError(fmt.Errorf("the Application Default Credentials are invalid or revoked (%w)", cause))
	case code == "invalid_grant":
		return synclet.ConfigError(fmt.Errorf("refresh_token is invalid or revoked (%w)", cause))
	case credential:
		return synclet.ConfigError(fmt.Errorf("client_id or client_secret is invalid (%w)", cause))
	case status >= 500:
		return retryable{err: cause}
	}
	return cause
}
