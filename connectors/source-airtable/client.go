package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL    = "https://api.airtable.com/v0/"
	defaultRetryDelay = time.Second
	// requestSpacing keeps requests to one base within Airtable's limit of
	// 5 requests per second per base.
	requestSpacing = 200 * time.Millisecond
	// rateLimitWait is how long Airtable blocks a base after a 429; the
	// response carries no Retry-After.
	rateLimitWait = 30 * time.Second
	// rateLimitRetries is how many times a request is repeated after a 429.
	rateLimitRetries = 5
	// retries is how many times a request is repeated after a 5xx response
	// or a transport error.
	retries = 3
)

// apiClient sends authenticated GET requests to the Airtable API. The token
// travels only in the Authorization header, never in a URL.
type apiClient struct {
	http       *http.Client
	baseURL    string
	auth       string
	retryDelay time.Duration
	now        func() time.Time
	wait       func(ctx context.Context, d time.Duration) error
	last       map[string]time.Time
}

func (s Source) apiClient(cfg Config) *apiClient {
	c := &apiClient{
		http:       s.client,
		baseURL:    s.baseURL,
		auth:       "Bearer " + cfg.PersonalAccessToken,
		retryDelay: s.retryDelay,
		now:        s.now,
		wait:       s.wait,
		last:       map[string]time.Time{},
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: time.Minute}
	}
	if c.baseURL == "" {
		c.baseURL = defaultBaseURL
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.wait == nil {
		c.wait = sleep
	}
	return c
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// httpError is a response with an error status that is not a
// configuration error by itself.
type httpError struct {
	status int
	path   string
	detail string
}

func (e *httpError) Error() string { return fmt.Sprintf("GET %s %s", e.path, e.detail) }

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// rateLimited is a 429 response.
type rateLimited struct{ path string }

func (e rateLimited) Error() string { return fmt.Sprintf("GET %s (HTTP 429): rate limited", e.path) }

// get decodes the JSON response of path into out. Requests that share base
// are spaced by requestSpacing; base is the base id, or "" for requests not
// about one base. A 429 is repeated after rateLimitWait up to
// rateLimitRetries times; a 5xx response or a transport error is repeated
// with a doubling delay up to retries times.
func (c *apiClient) get(ctx context.Context, base, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	delay := c.retryDelay
	var failures, limited int
	for {
		if err := c.space(ctx, base); err != nil {
			return err
		}
		err := c.do(ctx, u, out)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var rl rateLimited
		if errors.As(err, &rl) {
			if limited == rateLimitRetries {
				return fmt.Errorf("%w; gave up after %d waits of %s", err, rateLimitRetries, rateLimitWait)
			}
			limited++
			if err := c.wait(ctx, rateLimitWait); err != nil {
				return err
			}
			continue
		}
		var r retryable
		if !errors.As(err, &r) || failures == retries {
			return err
		}
		failures++
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay *= 2
	}
}

// space waits until requestSpacing has passed since the previous request
// for base.
func (c *apiClient) space(ctx context.Context, base string) error {
	if last, ok := c.last[base]; ok {
		if d := last.Add(requestSpacing).Sub(c.now()); d > 0 {
			if err := c.wait(ctx, d); err != nil {
				return err
			}
		}
	}
	c.last[base] = c.now()
	return nil
}

func (c *apiClient) do(ctx context.Context, u string, out any) (rerr error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{err}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{fmt.Errorf("GET %s: read body: %w", req.URL.Path, err)}
	}
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, out); err != nil {
			return fmt.Errorf("GET %s: decode response: %w", req.URL.Path, err)
		}
		return nil
	}
	detail := "(HTTP " + strconv.Itoa(resp.StatusCode) + ")"
	if d := errorDetail(body); d != "" {
		detail += ": " + d
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return rateLimited{path: req.URL.Path}
	case http.StatusUnauthorized:
		return synclet.ConfigError(fmt.Errorf("invalid personal access token %s", detail))
	}
	he := &httpError{status: resp.StatusCode, path: req.URL.Path, detail: detail}
	if resp.StatusCode >= 500 {
		return retryable{he}
	}
	return he
}

// errorDetail returns the message of an Airtable error body, whose "error"
// is a code string or an object with a type and a message.
func errorDetail(body []byte) string {
	var e struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(body, &e) != nil || len(e.Error) == 0 {
		return ""
	}
	var code string
	if json.Unmarshal(e.Error, &code) == nil {
		return code
	}
	var obj struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	if json.Unmarshal(e.Error, &obj) != nil {
		return ""
	}
	switch {
	case obj.Type != "" && obj.Message != "":
		return obj.Type + ": " + obj.Message
	case obj.Type != "":
		return obj.Type
	default:
		return obj.Message
	}
}

// statusOf returns the HTTP status of an httpError in err, or 0.
func statusOf(err error) int {
	var he *httpError
	if errors.As(err, &he) {
		return he.status
	}
	return 0
}
