package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL    = "https://api.hubapi.com/"
	defaultRetryDelay = time.Second
	// defaultRetryAfter is the wait after a 429 without a Retry-After header:
	// the private-app burst window.
	defaultRetryAfter = 10 * time.Second
	// defaultWindow and defaultMax are the private-app burst limit until a
	// response reports its own in the X-HubSpot-RateLimit headers.
	defaultWindow = 10 * time.Second
	defaultMax    = 100
	// searchInterval spaces CRM search requests, which HubSpot limits to
	// five per second per account, apart from the burst limit.
	searchInterval = 250 * time.Millisecond
	// retries is how many times a request is repeated after a 5xx response
	// or a transport error.
	retries = 3
	// rateLimitRetries is how many times a request is repeated after a 429.
	rateLimitRetries = 5
)

// lane is a rate limit a request counts against.
type lane int

const (
	general lane = iota
	search
)

// apiClient sends authenticated requests to the HubSpot API, one at a
// time. The token travels only in the Authorization header, never in a URL.
type apiClient struct {
	http       *http.Client
	baseURL    string
	auth       string
	retryDelay time.Duration
	now        func() time.Time
	wait       func(ctx context.Context, d time.Duration) error
	// interval is the spacing of general requests; next is the earliest
	// time the next request of each lane may be sent.
	interval time.Duration
	next     [2]time.Time
}

func (s Source) apiClient(cfg Config) *apiClient {
	c := &apiClient{
		http:       s.client,
		baseURL:    s.baseURL,
		auth:       "Bearer " + cfg.AccessToken,
		retryDelay: s.retryDelay,
		now:        s.now,
		wait:       s.wait,
		interval:   defaultWindow / defaultMax,
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

// apiError is a response with an error status.
type apiError struct {
	method   string
	path     string
	status   int
	category string
	message  string
	scopes   []string
}

func (e *apiError) Error() string {
	s := fmt.Sprintf("%s %s (HTTP %d)", e.method, e.path, e.status)
	if e.category != "" {
		s += " " + e.category
	}
	if e.message != "" {
		s += ": " + e.message
	}
	return s
}

// scopeError is a 403 MISSING_SCOPES response: the token lacks the scopes
// the request needs.
type scopeError struct{ *apiError }

func (e scopeError) Error() string {
	return fmt.Sprintf("missing scope %s for %s %s", strings.Join(e.scopes, ", "), e.method, e.path)
}

func (e scopeError) Unwrap() error { return e.apiError }

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// rateLimited is a 429 response that is not the daily limit; the request
// may be repeated after wait.
type rateLimited struct {
	err  *apiError
	wait time.Duration
}

func (e rateLimited) Error() string { return e.err.Error() }

func (c *apiClient) get(ctx context.Context, path string, query url.Values, out any) error {
	return c.send(ctx, http.MethodGet, path, query, nil, general, out)
}

// post sends body as JSON. A search request counts against the search
// lane, any other against the general one.
func (c *apiClient) post(ctx context.Context, path string, query url.Values, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("POST %s: encode body: %w", path, err)
	}
	l := general
	if strings.HasSuffix(path, "/search") {
		l = search
	}
	return c.send(ctx, http.MethodPost, path, query, raw, l, out)
}

// send decodes the JSON response of a request into out. A 429 is repeated
// after its Retry-After up to rateLimitRetries times, except the daily
// limit; a 5xx response or a transport error is repeated with a doubling
// delay up to retries times. 401 and 403 are configuration errors.
func (c *apiClient) send(ctx context.Context, method, path string, query url.Values, body []byte, l lane, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	delay := c.retryDelay
	var failures, limited int
	for {
		if err := c.pace(ctx, l); err != nil {
			return err
		}
		err := c.do(ctx, method, u, body, l, out)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var rl rateLimited
		if errors.As(err, &rl) {
			if limited == rateLimitRetries {
				return fmt.Errorf("%w; gave up after %d waits", err, rateLimitRetries)
			}
			limited++
			if err := c.wait(ctx, rl.wait); err != nil {
				return err
			}
			continue
		}
		var r retryable
		if !errors.As(err, &r) || failures == retries {
			return err
		}
		failures++
		if err := c.wait(ctx, delay); err != nil {
			return err
		}
		delay *= 2
	}
}

// pace waits until the next request of lane l may be sent.
func (c *apiClient) pace(ctx context.Context, l lane) error {
	if d := c.next[l].Sub(c.now()); d > 0 {
		return c.wait(ctx, d)
	}
	return nil
}

// observe schedules the next request of lane l. The general spacing is
// the interval of X-HubSpot-RateLimit-Interval-Milliseconds divided by
// X-HubSpot-RateLimit-Max when a response carries them.
func (c *apiClient) observe(l lane, h http.Header) {
	if l == search {
		c.next[search] = c.now().Add(searchInterval)
		return
	}
	ms, errMS := strconv.Atoi(h.Get("X-HubSpot-RateLimit-Interval-Milliseconds"))
	limit, errMax := strconv.Atoi(h.Get("X-HubSpot-RateLimit-Max"))
	if errMS == nil && errMax == nil && ms > 0 && limit > 0 {
		c.interval = time.Duration(ms) * time.Millisecond / time.Duration(limit)
	}
	c.next[general] = c.now().Add(c.interval)
}

func (c *apiClient) do(ctx context.Context, method, u string, body []byte, l lane, out any) (rerr error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{err}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	c.observe(l, resp.Header)
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{fmt.Errorf("%s %s: read body: %w", method, req.URL.Path, err)}
	}
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusMultiStatus {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("%s %s: decode response: %w", method, req.URL.Path, err)
		}
		return nil
	}
	ae, policy := decodeError(method, req.URL.Path, resp.StatusCode, raw)
	switch {
	case resp.StatusCode == http.StatusTooManyRequests && policy == "DAILY":
		return synclet.ConfigError(fmt.Errorf("daily API limit of the HubSpot account reached: %w", ae))
	case resp.StatusCode == http.StatusTooManyRequests:
		return rateLimited{err: ae, wait: retryAfter(resp.Header.Get("Retry-After"))}
	case resp.StatusCode == http.StatusUnauthorized:
		return synclet.ConfigError(fmt.Errorf("invalid private-app token (HTTP 401): %s", ae.message))
	case resp.StatusCode == http.StatusForbidden && ae.category == "MISSING_SCOPES":
		return synclet.ConfigError(scopeError{ae})
	case resp.StatusCode == http.StatusForbidden:
		return synclet.ConfigError(ae)
	case resp.StatusCode >= 500:
		return retryable{ae}
	}
	return ae
}

// decodeError reads a HubSpot error body: its message, category, the
// scopes a 403 names and the policyName of a 429.
func decodeError(method, path string, status int, raw []byte) (*apiError, string) {
	ae := &apiError{method: method, path: path, status: status}
	var body struct {
		Message    string `json:"message"`
		Category   string `json:"category"`
		PolicyName string `json:"policyName"`
		Errors     []struct {
			Context map[string][]string `json:"context"`
		} `json:"errors"`
	}
	if json.Unmarshal(raw, &body) != nil {
		ae.message = strings.TrimSpace(string(raw))
		return ae, ""
	}
	ae.message, ae.category = body.Message, body.Category
	for _, e := range body.Errors {
		ae.scopes = append(ae.scopes, e.Context["requiredGranularScopes"]...)
	}
	return ae, body.PolicyName
}

// retryAfter parses a Retry-After header given in seconds.
func retryAfter(h string) time.Duration {
	n, err := strconv.Atoi(h)
	if err != nil || n < 0 {
		return defaultRetryAfter
	}
	return time.Duration(n) * time.Second
}
