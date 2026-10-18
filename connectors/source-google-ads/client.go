package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// apiVersion is the Google Ads API version of every request. Google
	// releases a major version about every three months and sunsets each
	// about a year later.
	apiVersion        = "v26"
	defaultBaseURL    = "https://googleads.googleapis.com"
	defaultRetryDelay = 2 * time.Second
	maxRetryDelay     = time.Minute
	// attempts is how many times a request is sent while it fails with
	// RESOURCE_TEMPORARILY_EXHAUSTED, a 5xx response or a transport error.
	attempts = 5
)

// apiClient sends authenticated requests to the Google Ads REST API. The
// developer token and access token travel only in headers.
type apiClient struct {
	http       *http.Client
	root       string
	auth       *tokenSource
	devToken   string
	retryDelay time.Duration
	jitter     func(time.Duration) time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func (s Source) apiClient(cfg Config) *apiClient {
	hc := s.client
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}
	root := s.baseURL
	if root == "" {
		root = defaultBaseURL
	}
	tokenURL := s.tokenURL
	if tokenURL == "" {
		tokenURL = defaultTokenURL
	}
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	c := &apiClient{
		http: hc,
		root: strings.TrimRight(root, "/") + "/" + apiVersion + "/",
		auth: &tokenSource{
			http: hc, url: tokenURL, now: now,
			clientID: cfg.ClientID, clientSecret: cfg.ClientSecret, refreshToken: cfg.RefreshToken,
		},
		devToken:   cfg.DeveloperToken,
		retryDelay: s.retryDelay,
		jitter:     s.jitter,
		wait:       s.wait,
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	if c.jitter == nil {
		c.jitter = equalJitter
	}
	if c.wait == nil {
		c.wait = sleep
	}
	return c
}

// apiError is a failed Google Ads API request. codes are the values of the
// errorCode objects of its GoogleAdsFailure, such as USER_PERMISSION_DENIED.
type apiError struct {
	path    string
	status  int
	state   string
	codes   []string
	message string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%s: HTTP %d", e.path, e.status)
	switch {
	case len(e.codes) > 0:
		msg += " " + e.codes[0]
	case e.state != "":
		msg += " " + e.state
	}
	if e.message != "" {
		msg += ": " + e.message
	}
	return msg
}

func (e *apiError) has(code string) bool { return slices.Contains(e.codes, code) }

// errorBody is the error body of the Google Ads REST API.
type errorBody struct {
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
		Details []struct {
			Errors []struct {
				ErrorCode map[string]string `json:"errorCode"`
				Message   string            `json:"message"`
			} `json:"errors"`
		} `json:"details"`
	} `json:"error"`
}

func newAPIError(path string, status int, raw []byte) *apiError {
	ae := &apiError{path: path, status: status}
	var body errorBody
	if json.Unmarshal(raw, &body) != nil || body.Error == nil {
		ae.message = strings.TrimSpace(string(raw))
		return ae
	}
	ae.state, ae.message = body.Error.Status, body.Error.Message
	first := true
	for _, d := range body.Error.Details {
		for _, e := range d.Errors {
			for _, v := range e.ErrorCode {
				ae.codes = append(ae.codes, v)
			}
			if first && e.Message != "" {
				ae.message, first = e.Message, false
			}
		}
	}
	return ae
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// post sends body as JSON to path and decodes the response into out. A
// request rejected with 401 gets a new access token and is sent once more;
// RESOURCE_TEMPORARILY_EXHAUSTED, 5xx and transport errors are sent again
// after a doubling, jittered delay, up to attempts times in all.
func (c *apiClient) post(ctx context.Context, path, login string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	delay := c.retryDelay
	refreshed := false
	for attempt := 1; ; attempt++ {
		err := c.do(ctx, path, login, payload, out)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var ae *apiError
		if errors.As(err, &ae) && ae.status == http.StatusUnauthorized && !refreshed {
			refreshed = true
			attempt--
			continue
		}
		var r retryable
		if err == nil || !errors.As(err, &r) || attempt == attempts {
			return err
		}
		if err := c.wait(ctx, c.jitter(delay)); err != nil {
			return err
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

func (c *apiClient) do(ctx context.Context, path, login string, payload []byte, out any) (rerr error) {
	token, err := c.auth.get(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.root+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("developer-token", c.devToken)
	if login != "" {
		req.Header.Set("login-customer-id", login)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{err: fmt.Errorf("%s: %w", path, err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{err: fmt.Errorf("%s: read body: %w", path, err)}
	}
	if resp.StatusCode == http.StatusOK {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("%s: decode response: %w", path, err)
		}
		return nil
	}
	ae := newAPIError(path, resp.StatusCode, raw)
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		c.auth.invalidate(token)
		return ae
	case ae.has("DEVELOPER_TOKEN_NOT_APPROVED"), ae.has("DEVELOPER_TOKEN_PROHIBITED"):
		return synclet.ConfigError(fmt.Errorf("developer token is not approved for this account (test-only token?) (%w)", ae))
	case ae.has("RESOURCE_EXHAUSTED"):
		return ae
	case ae.has("RESOURCE_TEMPORARILY_EXHAUSTED"), resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return retryable{err: ae}
	}
	return ae
}

// searchPage is one page of a googleAds:search response.
type searchPage struct {
	Results       []map[string]any `json:"results"`
	NextPageToken string           `json:"nextPageToken"`
}

// search calls fn with every result row of a GAQL query of customer cid,
// following nextPageToken. login is the login-customer-id header, empty for
// none. USER_PERMISSION_DENIED becomes a configuration error that names the
// customer.
func (c *apiClient) search(ctx context.Context, cid, login, query string, fn func(map[string]any) error) error {
	path := "customers/" + cid + "/googleAds:search"
	body := map[string]string{"query": query}
	for {
		var p searchPage
		if err := c.post(ctx, path, login, body, &p); err != nil {
			var ae *apiError
			if errors.As(err, &ae) && ae.has("USER_PERMISSION_DENIED") {
				return synclet.ConfigError(fmt.Errorf("customer %s is not accessible; if it is managed through a manager account, set login_customer_id (%w)", cid, err))
			}
			return err
		}
		for _, row := range p.Results {
			if err := fn(row); err != nil {
				return err
			}
		}
		if p.NextPageToken == "" {
			return nil
		}
		body["pageToken"] = p.NextPageToken
	}
}

// fieldTypes asks GoogleAdsFields for the data type of each named field.
// A name GoogleAdsFields does not know is missing from the result.
func (c *apiClient) fieldTypes(ctx context.Context, names []string) (map[string]fieldType, error) {
	quoted := make([]string, len(names))
	for i, n := range slices.Sorted(slices.Values(names)) {
		quoted[i] = "'" + n + "'"
	}
	body := map[string]string{"query": "SELECT name, data_type, is_repeated WHERE name IN (" + strings.Join(quoted, ", ") + ")"}
	types := map[string]fieldType{}
	for {
		var p struct {
			Results []struct {
				Name       string `json:"name"`
				DataType   string `json:"dataType"`
				IsRepeated bool   `json:"isRepeated"`
			} `json:"results"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := c.post(ctx, "googleAdsFields:search", "", body, &p); err != nil {
			return nil, fmt.Errorf("field types: %w", err)
		}
		for _, r := range p.Results {
			if slices.Contains(names, r.Name) {
				types[r.Name] = fieldType{dataType: r.DataType, repeated: r.IsRepeated}
			}
		}
		if p.NextPageToken == "" {
			return types, nil
		}
		body["pageToken"] = p.NextPageToken
	}
}
