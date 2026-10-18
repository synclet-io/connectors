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
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL = "https://api.notion.com/v1/"
	// notionVersion is the pinned Notion API version: databases hold data
	// sources, and rows are pages whose parent is a data source.
	notionVersion     = "2025-09-03"
	defaultRetryDelay = time.Second
	// requestSpacing keeps the integration under Notion's average of 3
	// requests per second.
	requestSpacing = 350 * time.Millisecond
	// defaultRetryAfter is the wait after a 429 without a usable
	// Retry-After header.
	defaultRetryAfter = time.Second
	// rateLimitRetries is how many times a request is repeated after a 429.
	rateLimitRetries = 5
	// retries is how many times a request is repeated after a 5xx response
	// or a transport error.
	retries = 3
	// pageSize is the page_size of every listing, the most Notion allows.
	pageSize = 100
)

// apiClient sends authenticated requests to the Notion API, one at a time
// and spaced by requestSpacing. The token travels only in the Authorization
// header, never in a URL.
type apiClient struct {
	http       *http.Client
	baseURL    string
	auth       string
	retryDelay time.Duration
	now        func() time.Time
	wait       func(ctx context.Context, d time.Duration) error
	last       time.Time
}

func (s Source) apiClient(cfg Config) *apiClient {
	c := &apiClient{
		http:       s.client,
		baseURL:    s.baseURL,
		auth:       "Bearer " + cfg.AccessToken,
		retryDelay: s.retryDelay,
		now:        s.now,
		wait:       s.wait,
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
	method string
	path   string
	code   string
	detail string
}

func (e *httpError) Error() string { return fmt.Sprintf("%s %s %s", e.method, e.path, e.detail) }

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// rateLimited is a 429 response and the wait its Retry-After asks for.
type rateLimited struct {
	method string
	path   string
	wait   time.Duration
}

func (e rateLimited) Error() string {
	return fmt.Sprintf("%s %s (HTTP 429): rate limited", e.method, e.path)
}

// get decodes the JSON response of GET path?query into out.
func (c *apiClient) get(ctx context.Context, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	return c.send(ctx, http.MethodGet, u, nil, out)
}

// post sends body as JSON to path and decodes the JSON response into out.
func (c *apiClient) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("POST %s: encode body: %w", path, err)
	}
	return c.send(ctx, http.MethodPost, c.baseURL+path, b, out)
}

// send repeats a request after a 429 for its Retry-After up to
// rateLimitRetries times, and after a 5xx response or a transport error
// with a doubling delay up to retries times.
func (c *apiClient) send(ctx context.Context, method, u string, body []byte, out any) error {
	delay := c.retryDelay
	var failures, limited int
	for {
		if err := c.space(ctx); err != nil {
			return err
		}
		err := c.do(ctx, method, u, body, out)
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

// space waits until requestSpacing has passed since the previous request.
func (c *apiClient) space(ctx context.Context) error {
	if !c.last.IsZero() {
		if d := c.last.Add(requestSpacing).Sub(c.now()); d > 0 {
			if err := c.wait(ctx, d); err != nil {
				return err
			}
		}
	}
	c.last = c.now()
	return nil
}

func (c *apiClient) do(ctx context.Context, method, u string, body []byte, out any) (rerr error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Notion-Version", notionVersion)
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
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{fmt.Errorf("%s %s: read body: %w", method, req.URL.Path, err)}
	}
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("%s %s: decode response: %w", method, req.URL.Path, err)
		}
		return nil
	}
	code, message := errorBody(raw)
	detail := "(HTTP " + strconv.Itoa(resp.StatusCode) + ")"
	switch {
	case code != "" && message != "":
		detail += ": " + code + ": " + message
	case code != "" || message != "":
		detail += ": " + code + message
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return rateLimited{method: method, path: req.URL.Path, wait: retryAfter(resp.Header.Get("Retry-After"))}
	case http.StatusUnauthorized:
		return synclet.ConfigError(fmt.Errorf("integration token is invalid %s", detail))
	case http.StatusForbidden:
		return synclet.ConfigError(fmt.Errorf("the integration lacks a capability for %s %s %s", method, req.URL.Path, detail))
	}
	he := &httpError{status: resp.StatusCode, method: method, path: req.URL.Path, code: code, detail: detail}
	if resp.StatusCode >= 500 {
		return retryable{he}
	}
	return he
}

// errorBody returns the code and message of a Notion error object,
// {"object":"error","status":...,"code":...,"message":...}.
func errorBody(raw []byte) (code, message string) {
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &e) != nil {
		return "", ""
	}
	return e.Code, e.Message
}

// retryAfter parses a Retry-After header given in seconds.
func retryAfter(h string) time.Duration {
	n, err := strconv.Atoi(h)
	if err != nil || n < 0 {
		return defaultRetryAfter
	}
	return time.Duration(n) * time.Second
}

// listPage is one page of a Notion list response.
type listPage struct {
	Results    []json.RawMessage `json:"results"`
	HasMore    bool              `json:"has_more"`
	NextCursor *string           `json:"next_cursor"`
}

// next returns the cursor of the following page, "" after the last one, or
// an error when has_more is set without a cursor or with a cursor already
// in seen, which would read the same pages again. It adds the cursor to
// seen.
func (p listPage) next(what string, seen map[string]bool) (string, error) {
	if !p.HasMore {
		return "", nil
	}
	if p.NextCursor == nil || *p.NextCursor == "" {
		return "", fmt.Errorf("%s: has_more is true but next_cursor is empty", what)
	}
	cur := *p.NextCursor
	if seen[cur] {
		return "", fmt.Errorf("%s: next_cursor %q was returned before", what, cur)
	}
	seen[cur] = true
	return cur, nil
}

// getAll pages through GET path by start_cursor and calls fn for every
// result, in order. fn returns false to stop.
func (c *apiClient) getAll(ctx context.Context, path string, fn func(json.RawMessage) (bool, error)) error {
	query := url.Values{"page_size": {strconv.Itoa(pageSize)}}
	seen := map[string]bool{}
	for {
		var p listPage
		if err := c.get(ctx, path, query, &p); err != nil {
			return err
		}
		for _, r := range p.Results {
			more, err := fn(r)
			if err != nil || !more {
				return err
			}
		}
		cur, err := p.next("GET "+path, seen)
		if err != nil || cur == "" {
			return err
		}
		query.Set("start_cursor", cur)
	}
}

// postAll pages through POST path by a start_cursor in the body and calls
// fn for every result, in order. fn returns false to stop.
func (c *apiClient) postAll(ctx context.Context, path string, body map[string]any, fn func(json.RawMessage) (bool, error)) error {
	body["page_size"] = pageSize
	delete(body, "start_cursor")
	seen := map[string]bool{}
	for {
		var p listPage
		if err := c.post(ctx, path, body, &p); err != nil {
			return err
		}
		for _, r := range p.Results {
			more, err := fn(r)
			if err != nil || !more {
				return err
			}
		}
		cur, err := p.next("POST "+path, seen)
		if err != nil || cur == "" {
			return err
		}
		body["start_cursor"] = cur
	}
}
