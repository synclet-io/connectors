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
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL    = "https://api.revenuecat.com/v2/"
	defaultRetryDelay = time.Second
	// defaultRetryAfter is the wait after a 429 that names no wait in its
	// Retry-After header or backoff_ms field.
	defaultRetryAfter = time.Minute
	// retries is how many times a request is repeated after a 5xx response,
	// a retryable error body or a transport error.
	retries = 3
	// rateLimitRetries is how many times a request is repeated after a 429.
	rateLimitRetries = 5
	// pageSize is the limit of list requests, the most the API returns.
	pageSize = "100"
)

// domain is a RevenueCat rate limiting domain; all endpoints of a domain
// share its requests-per-minute limit.
type domain int

const (
	configDomain domain = iota
	customerDomain
	metricsDomain
)

// defaultLimits are the documented requests per minute of each domain.
var defaultLimits = map[domain]int{
	configDomain:   60,
	customerDomain: 480,
	metricsDomain:  25,
}

// pacer spaces the requests of one domain: next is the earliest time the
// next request may be sent.
type pacer struct {
	limit int
	next  time.Time
}

// apiClient sends authenticated GET requests to the RevenueCat REST API
// v2. The key travels only in the Authorization header, never in a URL.
// It is used by one goroutine at a time.
type apiClient struct {
	http       *http.Client
	baseURL    string
	auth       string
	retryDelay time.Duration
	now        func() time.Time
	wait       func(ctx context.Context, d time.Duration) error
	pacers     map[domain]*pacer
}

func (s Source) apiClient(cfg Config) *apiClient {
	c := &apiClient{
		http:       s.client,
		baseURL:    s.baseURL,
		auth:       "Bearer " + cfg.APIKey,
		retryDelay: s.retryDelay,
		now:        s.now,
		wait:       s.wait,
		pacers:     map[domain]*pacer{},
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
	for d, l := range defaultLimits {
		c.pacers[d] = &pacer{limit: l}
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

// forbidden is a 403: the key lacks a permission the request needs.
type forbidden struct {
	path   string
	detail string
}

func (e *forbidden) Error() string { return fmt.Sprintf("GET %s %s", e.path, e.detail) }

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// rateLimited is a 429 response; the request may be repeated after wait.
type rateLimited struct {
	path   string
	detail string
	wait   time.Duration
}

func (e rateLimited) Error() string { return fmt.Sprintf("GET %s %s", e.path, e.detail) }

// errorBody is the body of a RevenueCat error response.
type errorBody struct {
	Type      string `json:"type"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	BackoffMS *int64 `json:"backoff_ms"`
}

// get decodes the JSON response of path into out. Every request first waits
// for its domain's pacing. A 429 is repeated after its Retry-After up to
// rateLimitRetries times; a 5xx response, a retryable error body or a
// transport error is repeated with a doubling delay up to retries times.
func (c *apiClient) get(ctx context.Context, d domain, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	delay := c.retryDelay
	var failures, limited int
	for {
		if err := c.pace(ctx, d); err != nil {
			return err
		}
		err := c.do(ctx, d, u, out)
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

// pace waits until the next request of domain d may be sent.
func (c *apiClient) pace(ctx context.Context, d domain) error {
	if w := c.pacers[d].next.Sub(c.now()); w > 0 {
		return c.wait(ctx, w)
	}
	return nil
}

// observe schedules the next request of domain d one interval of its limit
// after now. RevenueCat-Rate-Limit-Current-Limit replaces the limit; a
// RevenueCat-Rate-Limit-Current-Usage at or above it adds one more interval.
func (c *apiClient) observe(d domain, h http.Header) {
	p := c.pacers[d]
	if l, err := strconv.Atoi(h.Get("RevenueCat-Rate-Limit-Current-Limit")); err == nil && l > 0 {
		p.limit = l
	}
	interval := time.Minute / time.Duration(p.limit)
	p.next = c.now().Add(interval)
	if u, err := strconv.Atoi(h.Get("RevenueCat-Rate-Limit-Current-Usage")); err == nil && u >= p.limit {
		p.next = p.next.Add(interval)
	}
}

func (c *apiClient) do(ctx context.Context, d domain, u string, out any) (rerr error) {
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
	c.observe(d, resp.Header)
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
	var eb errorBody
	_ = json.Unmarshal(body, &eb)
	detail := "(HTTP " + strconv.Itoa(resp.StatusCode) + ")"
	switch {
	case eb.Type != "" && eb.Message != "":
		detail += ": " + eb.Type + ": " + eb.Message
	case eb.Type != "" || eb.Message != "":
		detail += ": " + eb.Type + eb.Message
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return rateLimited{path: req.URL.Path, detail: detail, wait: retryAfter(resp.Header.Get("Retry-After"), eb.BackoffMS)}
	case http.StatusUnauthorized:
		return synclet.ConfigError(authError(eb.Message, detail))
	case http.StatusForbidden:
		return &forbidden{path: req.URL.Path, detail: detail}
	}
	he := &httpError{status: resp.StatusCode, path: req.URL.Path, detail: detail}
	if resp.StatusCode >= 500 || eb.Retryable {
		return retryable{he}
	}
	return he
}

// authError describes a 401. RevenueCat answers a v1 key with a message
// about the API version; any other 401 is an invalid key.
func authError(message, detail string) error {
	m := strings.ToLower(message)
	if strings.Contains(m, "v1") || strings.Contains(m, "version") {
		return fmt.Errorf("API v1 key used for REST API v2 %s: create a secret API key of version V2 in the project settings", detail)
	}
	return fmt.Errorf("invalid API key %s; API v1 keys do not work with REST API v2", detail)
}

// retryAfter returns the wait of a 429: its Retry-After header in seconds,
// else the body's backoff_ms, else defaultRetryAfter.
func retryAfter(h string, backoffMS *int64) time.Duration {
	if n, err := strconv.Atoi(h); err == nil && n >= 0 {
		return time.Duration(n) * time.Second
	}
	if backoffMS != nil && *backoffMS >= 0 {
		return time.Duration(*backoffMS) * time.Millisecond
	}
	return defaultRetryAfter
}

// item is one entry of a list, its fields as the API sent them.
type item = map[string]json.RawMessage

// list requests path page by page with limit 100, following the
// starting_after of each page's next_page, and calls fn for every item. A
// next_page without starting_after, or one that repeats an earlier value,
// fails the list instead of looping or skipping a page.
func (c *apiClient) list(ctx context.Context, d domain, path string, query url.Values, fn func(item) error) error {
	q := url.Values{}
	for k, v := range query {
		q[k] = v
	}
	q.Set("limit", pageSize)
	seen := map[string]bool{}
	for {
		var page struct {
			Items    *[]item `json:"items"`
			NextPage *string `json:"next_page"`
		}
		if err := c.get(ctx, d, path, q, &page); err != nil {
			return err
		}
		if page.Items == nil {
			return fmt.Errorf("GET %s: response has no items", path)
		}
		for _, it := range *page.Items {
			if err := fn(it); err != nil {
				return err
			}
		}
		if page.NextPage == nil || *page.NextPage == "" {
			return nil
		}
		next, err := url.Parse(*page.NextPage)
		if err != nil {
			return fmt.Errorf("GET %s: next_page %q: %w", path, *page.NextPage, err)
		}
		after := next.Query().Get("starting_after")
		if after == "" {
			return fmt.Errorf("GET %s: next_page %q has no starting_after", path, *page.NextPage)
		}
		if seen[after] {
			return fmt.Errorf("GET %s: next_page repeats starting_after %q", path, after)
		}
		seen[after] = true
		q.Set("starting_after", after)
	}
}
