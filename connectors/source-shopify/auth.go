package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// tokenMargin is how long before its expiry a token of the client
// credentials grant is fetched again.
const tokenMargin = 5 * time.Minute

// credentials holds the configured access token, or the client
// credentials of a Dev Dashboard app and the token last fetched with them.
type credentials struct {
	token        string
	clientID     string
	clientSecret string
	fetched      string
	expires      time.Time
}

func (cr *credentials) grant() bool { return cr.token == "" }

func (cr *credentials) reset() { cr.fetched, cr.expires = "", time.Time{} }

// get returns the token to send, fetching one with the client credentials
// grant when there is none or it is about to expire.
func (cr *credentials) get(ctx context.Context, c *apiClient) (string, error) {
	if !cr.grant() {
		return cr.token, nil
	}
	if cr.fetched != "" && (cr.expires.IsZero() || c.now().Before(cr.expires.Add(-tokenMargin))) {
		return cr.fetched, nil
	}
	token, ttl, err := c.fetchToken(ctx, cr.clientID, cr.clientSecret)
	if err != nil {
		return "", err
	}
	cr.fetched = token
	cr.expires = time.Time{}
	if ttl > 0 {
		cr.expires = c.now().Add(ttl)
	}
	return token, nil
}

// fetchToken exchanges the client credentials for an access token. The
// secret travels in the form body, never in the URL.
func (c *apiClient) fetchToken(ctx context.Context, id, secret string) (token string, ttl time.Duration, rerr error) {
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {id}, "client_secret": {secret}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.origin+"/admin/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("client credentials grant: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("client credentials grant: read body: %w", err)
	}
	he := &httpError{op: "client credentials grant", status: resp.StatusCode, message: errorText(raw)}
	switch {
	case resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
		return "", 0, synclet.ConfigError(fmt.Errorf("client_id or client_secret is invalid, or the app is not installed on %s: %w", c.domain(), he))
	case resp.StatusCode == http.StatusNotFound:
		return "", 0, synclet.ConfigError(fmt.Errorf("shop %s not found; use the shop's myshopify.com domain: %w", c.domain(), he))
	case resp.StatusCode != http.StatusOK:
		return "", 0, he
	}
	var body struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", 0, fmt.Errorf("client credentials grant: decode response: %w", err)
	}
	if body.AccessToken == "" {
		return "", 0, fmt.Errorf("client credentials grant: response without an access_token")
	}
	return body.AccessToken, time.Duration(body.ExpiresIn) * time.Second, nil
}
