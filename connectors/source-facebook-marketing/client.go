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
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// apiVersion is the Graph API version of every request. Meta releases a
	// version every few months and retires each about two years later.
	apiVersion        = "v26.0"
	defaultBaseURL    = "https://graph.facebook.com"
	defaultRetryDelay = 5 * time.Second
	maxRetryDelay     = 5 * time.Minute
	maxThrottleWait   = time.Hour
	// retries is how many times a request is repeated after throttling, a 5xx
	// response, a transient Graph error or a transport error.
	retries = 5
	// pageLimit is the page size of every list request.
	pageLimit = 500
)

// throttleCodes are the Graph API error codes of rate limiting. Meta sends
// them with HTTP 400, and sometimes 200, rather than 429.
var throttleCodes = map[int]bool{4: true, 17: true, 32: true, 613: true}

func isThrottleCode(code int) bool {
	return throttleCodes[code] || (code >= 80000 && code <= 80014)
}

// transientCodes are Graph API error codes Meta documents as temporary.
var transientCodes = map[int]bool{1: true, 2: true}

// apiClient sends authenticated requests to the Graph API. The access token
// travels only in the Authorization header, never in a URL or a form.
type apiClient struct {
	http       *http.Client
	root       string
	token      string
	headers    []Header
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func (s Source) apiClient(cfg Config) *apiClient {
	root := cfg.GraphAPIBaseURL
	if root == "" {
		root = s.baseURL
	}
	if root == "" {
		root = defaultBaseURL
	}
	c := &apiClient{
		http:       s.client,
		root:       strings.TrimRight(root, "/") + "/" + apiVersion + "/",
		token:      cfg.AccessToken,
		headers:    cfg.GraphAPIHeaders,
		retryDelay: s.retryDelay,
		wait:       s.wait,
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 5 * time.Minute}
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	if c.wait == nil {
		c.wait = sleep
	}
	return c
}

// graphError is the error object of a Graph API error body.
type graphError struct {
	Code    int    `json:"code"`
	Subcode int    `json:"error_subcode"`
	Type    string `json:"type"`
	Message string `json:"message"`
}

// apiError is a failed Graph API request.
type apiError struct {
	method string
	path   string
	status int
	graph  graphError
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%s %s: HTTP %d", e.method, e.path, e.status)
	if e.graph.Code != 0 {
		msg += fmt.Sprintf(" code %d", e.graph.Code)
		if e.graph.Subcode != 0 {
			msg += fmt.Sprintf("/%d", e.graph.Subcode)
		}
	}
	if e.graph.Message != "" {
		msg += ": " + e.graph.Message
	}
	return msg
}

// retryable marks a failure that may pass when the request is repeated after
// wait, or after the client's doubling delay when wait is zero.
type retryable struct {
	err  error
	wait time.Duration
}

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

func (c *apiClient) get(ctx context.Context, path string, query url.Values, out any) error {
	return c.request(ctx, http.MethodGet, path, query, out)
}

func (c *apiClient) post(ctx context.Context, path string, form url.Values, out any) error {
	return c.request(ctx, http.MethodPost, path, form, out)
}

// request decodes the JSON response of path into out. Throttling waits as
// long as the usage headers say, or the doubling delay when they say nothing;
// 5xx responses, transient Graph errors and transport errors wait the
// doubling delay. Any other error is returned at once.
func (c *apiClient) request(ctx context.Context, method, path string, params url.Values, out any) error {
	delay := c.retryDelay
	for attempt := 0; ; attempt++ {
		err := c.do(ctx, method, path, params, out)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var r retryable
		if err == nil || !errors.As(err, &r) || attempt == retries {
			return err
		}
		wait := r.wait
		if wait == 0 {
			wait = delay
			delay = min(delay*2, maxRetryDelay)
		}
		if err := c.wait(ctx, wait); err != nil {
			return err
		}
	}
}

func (c *apiClient) do(ctx context.Context, method, path string, params url.Values, out any) (rerr error) {
	u := c.root + path
	var body io.Reader
	if method == http.MethodGet {
		if len(params) > 0 {
			u += "?" + params.Encode()
		}
	} else {
		body = strings.NewReader(params.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return err
	}
	for _, h := range c.headers {
		req.Header.Set(h.Name, h.Value)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{err: fmt.Errorf("%s %s: %w", method, path, err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{err: fmt.Errorf("%s %s: read body: %w", method, path, err)}
	}
	var e struct {
		Error *graphError `json:"error"`
	}
	_ = json.Unmarshal(raw, &e)
	if resp.StatusCode == http.StatusOK && e.Error == nil {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("%s %s: decode response: %w", method, path, err)
		}
		return nil
	}
	ae := &apiError{method: method, path: path, status: resp.StatusCode}
	if e.Error != nil {
		ae.graph = *e.Error
	}
	switch {
	case ae.graph.Code == 190:
		return synclet.ConfigError(fmt.Errorf("invalid or expired access token (%w)", ae))
	case isThrottleCode(ae.graph.Code):
		return retryable{err: ae, wait: throttleWait(resp.Header)}
	case transientCodes[ae.graph.Code], resp.StatusCode >= 500:
		return retryable{err: ae}
	}
	return ae
}

// throttleWait returns how long the usage headers of a throttled response ask
// to wait: the longest estimated_time_to_regain_access (minutes) of
// X-Business-Use-Case-Usage and reset_time_duration (seconds) of
// X-Ad-Account-Usage, capped at maxThrottleWait, or zero when neither says.
func throttleWait(h http.Header) time.Duration {
	var d time.Duration
	if v := h.Get("X-Business-Use-Case-Usage"); v != "" {
		var usage map[string][]struct {
			Minutes float64 `json:"estimated_time_to_regain_access"`
		}
		if json.Unmarshal([]byte(v), &usage) == nil {
			for _, entries := range usage {
				for _, u := range entries {
					d = max(d, time.Duration(u.Minutes*float64(time.Minute)))
				}
			}
		}
	}
	if v := h.Get("X-Ad-Account-Usage"); v != "" {
		var usage struct {
			Seconds float64 `json:"reset_time_duration"`
		}
		if json.Unmarshal([]byte(v), &usage) == nil {
			d = max(d, time.Duration(usage.Seconds*float64(time.Second)))
		}
	}
	return min(d, maxThrottleWait)
}

// listPage is one page of a Graph API list response.
type listPage struct {
	Data   []map[string]any `json:"data"`
	Paging struct {
		Cursors struct {
			After string `json:"after"`
		} `json:"cursors"`
		Next string `json:"next"`
	} `json:"paging"`
}

// list calls fn with every object of a list endpoint, following
// paging.cursors.after while the response has a next page.
func (c *apiClient) list(ctx context.Context, path string, query url.Values, fn func(map[string]any) error) error {
	q := url.Values{}
	for k, v := range query {
		q[k] = v
	}
	q.Set("limit", fmt.Sprint(pageLimit))
	for {
		var p listPage
		if err := c.get(ctx, path, q, &p); err != nil {
			return err
		}
		for _, obj := range p.Data {
			if err := fn(obj); err != nil {
				return err
			}
		}
		if p.Paging.Next == "" || p.Paging.Cursors.After == "" {
			return nil
		}
		q.Set("after", p.Paging.Cursors.After)
	}
}
