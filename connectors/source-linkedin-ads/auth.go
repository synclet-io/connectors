package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultTokenURL = "https://www.linkedin.com/oauth/v2/accessToken"
	// tokenMargin is how long before its expiry an access token is replaced.
	tokenMargin = 60 * time.Second
)

// tokenSource exchanges the refresh token for access tokens and caches each
// until tokenMargin before it expires. LinkedIn refresh tokens do not rotate:
// a refresh_token in the response is the one sent.
type tokenSource struct {
	http         *http.Client
	url          string
	clientID     string
	clientSecret string
	refreshToken string
	now          func() time.Time

	mu      sync.Mutex
	token   string
	expires time.Time
}

// get returns a cached access token, or a new one.
func (s *tokenSource) get(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token != "" && s.now().Before(s.expires) {
		return s.token, nil
	}
	token, ttl, err := s.fetch(ctx)
	if err != nil {
		return "", err
	}
	s.token, s.expires = token, s.now().Add(ttl-tokenMargin)
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

// fetch posts the refresh token grant. A refused refresh token or client
// becomes a configuration error naming the credential at fault; a 429 or 5xx
// response or a transport error is retryable.
func (s *tokenSource) fetch(ctx context.Context) (_ string, _ time.Duration, rerr error) {
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {s.clientID},
		"client_secret": {s.clientSecret},
		"refresh_token": {s.refreshToken},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := s.http.Do(req)
	if err != nil {
		return "", 0, retryable{err: fmt.Errorf("linkedin token endpoint: %w", err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, retryable{err: fmt.Errorf("linkedin token endpoint: read body: %w", err)}
	}
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	decErr := json.Unmarshal(raw, &body)
	if resp.StatusCode == http.StatusOK && decErr == nil && body.AccessToken != "" && body.ExpiresIn > 0 {
		return body.AccessToken, time.Duration(body.ExpiresIn) * time.Second, nil
	}
	detail := body.Error + ": " + body.Description
	if body.Error == "" {
		detail = snippet(raw)
	}
	cause := fmt.Errorf("linkedin token endpoint: HTTP %d %s", resp.StatusCode, detail)
	switch {
	case body.Error == "invalid_client", body.Error == "unauthorized_client":
		return "", 0, synclet.ConfigError(fmt.Errorf("client_id or client_secret is invalid (%w)", cause))
	case body.Error == "invalid_grant",
		body.Error == "invalid_request" && strings.Contains(strings.ToLower(body.Description), "refresh token"):
		return "", 0, synclet.ConfigError(fmt.Errorf("refresh_token is invalid or expired; authorize the app again for a new one (%w)", cause))
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return "", 0, retryable{err: cause}
	case resp.StatusCode == http.StatusOK:
		return "", 0, fmt.Errorf("linkedin token endpoint: response without access_token and expires_in: %s", snippet(raw))
	}
	return "", 0, cause
}
