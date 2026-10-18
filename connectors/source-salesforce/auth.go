package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultLoginURL   = "https://login.salesforce.com"
	defaultSandboxURL = "https://test.salesforce.com"
)

// tokenSource exchanges the refresh token for access tokens. Salesforce
// gives no lifetime for an access token, so one is cached until the API
// rejects it.
type tokenSource struct {
	http         *http.Client
	url          string
	clientID     string
	clientSecret string
	refreshToken string
	isSandbox    bool
	// instance is the configured instance URL; a token for another org is a
	// configuration error.
	instance string
	// rotated is called with the new refresh token when a token response
	// carries one that differs from the token in use.
	rotated func(ctx context.Context, refreshToken string) error

	mu    sync.Mutex
	token string
}

// get returns the cached access token, or a new one.
func (s *tokenSource) get(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" {
		return s.token, nil
	}
	token, err := s.fetch(ctx)
	if err != nil {
		return "", err
	}
	s.token = token
	return token, nil
}

// invalidate drops the cached token if it is still token, so the next get
// fetches a new one.
func (s *tokenSource) invalidate(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == token {
		s.token = ""
	}
}

// fetch posts the refresh token grant. invalid_grant and invalid_client
// become configuration errors naming the credential at fault; a 5xx
// response or a transport error is retryable.
func (s *tokenSource) fetch(ctx context.Context) (_ string, rerr error) {
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {s.clientID},
		"client_secret": {s.clientSecret},
		"refresh_token": {s.refreshToken},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return "", retryable{err: fmt.Errorf("token endpoint: %w", err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", retryable{err: fmt.Errorf("token endpoint: read body: %w", err)}
	}
	var body struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		InstanceURL  string `json:"instance_url"`
		Error        string `json:"error"`
		Description  string `json:"error_description"`
	}
	_ = json.Unmarshal(raw, &body)
	if resp.StatusCode == http.StatusOK && body.AccessToken != "" {
		if err := s.matchInstance(body.InstanceURL); err != nil {
			return "", err
		}
		if body.RefreshToken != "" && body.RefreshToken != s.refreshToken {
			s.refreshToken = body.RefreshToken
			if s.rotated != nil {
				if err := s.rotated(ctx, body.RefreshToken); err != nil {
					return "", err
				}
			}
		}
		return body.AccessToken, nil
	}
	cause := fmt.Errorf("token endpoint: HTTP %d %s: %s", resp.StatusCode, body.Error, body.Description)
	switch {
	case body.Error == "invalid_grant":
		org := "a sandbox org while is_sandbox is false"
		if s.isSandbox {
			org = "a production org while is_sandbox is true"
		}
		return "", synclet.ConfigError(fmt.Errorf("refresh_token is invalid, expired or revoked, or belongs to %s (%w)", org, cause))
	case body.Error == "invalid_client", body.Error == "invalid_client_id", body.Error == "unauthorized_client":
		return "", synclet.ConfigError(fmt.Errorf("client_id or client_secret is invalid (%w)", cause))
	case resp.StatusCode >= 500:
		return "", retryable{err: cause}
	case resp.StatusCode == http.StatusOK:
		return "", errors.New("token endpoint: response without access_token")
	}
	return "", cause
}

// matchInstance fails when the token belongs to an org other than the
// configured instance_url.
func (s *tokenSource) matchInstance(got string) error {
	inst, err := Config{InstanceURL: got}.instance()
	if err != nil || inst != s.instance {
		return synclet.ConfigError(fmt.Errorf("instance_url %s does not match the org of the refresh token (%s)", s.instance, got))
	}
	return nil
}
