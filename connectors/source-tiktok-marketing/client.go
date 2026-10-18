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
	defaultRetryDelay = time.Second
	maxRetryDelay     = time.Minute
	// retries is how many times a request is repeated after throttling, a
	// transient error code, HTTP 429, a 5xx response or a transport error.
	retries = 5
	// pageSize is the page size of every paged request, the documented
	// maximum of the get and report endpoints.
	pageSize = 1000
)

// throttleCodes are the response codes of rate limiting. TikTok sends them,
// like every error, with HTTP 200.
var throttleCodes = map[int]bool{40016: true, 40100: true, 40133: true}

// transientCodes are the response codes of temporary server-side failures.
var transientCodes = map[int]bool{50000: true, 51002: true, 51004: true, 51041: true, 60001: true}

// authCodes are the response codes of an access token that is invalid,
// expired or revoked.
var authCodes = map[int]bool{40102: true, 40104: true, 40105: true}

const (
	// codeNoPermission is the response code of an advertiser the token may not read.
	codeNoPermission = 40001
	// codeDateRange is the response code of a report date range that is too long.
	codeDateRange = 40067
)

// apiClient sends authenticated GET requests to the TikTok Business API. The
// access token travels only in the Access-Token header, never in a URL.
type apiClient struct {
	http       *http.Client
	root       string
	token      string
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func (s Source) apiClient(cfg Config) *apiClient {
	root := s.baseURL
	if root == "" {
		root = baseURLs[cfg.environment()]
	}
	c := &apiClient{
		http:       s.client,
		root:       strings.TrimRight(root, "/") + "/",
		token:      cfg.AccessToken,
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

// apiError is a failed request: a non-zero code in the response body, or an
// HTTP status other than 200 without one.
type apiError struct {
	path    string
	status  int
	code    int
	message string
}

func (e *apiError) Error() string {
	if e.code != 0 {
		return fmt.Sprintf("tiktok %s: code %d: %s", e.path, e.code, e.message)
	}
	return fmt.Sprintf("tiktok %s: HTTP %d: %s", e.path, e.status, e.message)
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// envelope is the body of every response.
type envelope struct {
	Code      int             `json:"code"`
	Message   string          `json:"message"`
	RequestID string          `json:"request_id"`
	Data      json.RawMessage `json:"data"`
}

// get decodes the data of the response of path into out. Throttling and
// transient codes, HTTP 429, 5xx responses and transport errors are retried
// after a doubling delay; any other error is returned at once.
func (c *apiClient) get(ctx context.Context, path string, query url.Values, out any) error {
	delay := c.retryDelay
	for attempt := 0; ; attempt++ {
		err := c.do(ctx, path, query, out)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var r retryable
		if err == nil || !errors.As(err, &r) {
			return err
		}
		if attempt == retries {
			return r.err
		}
		if err := c.wait(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

func (c *apiClient) do(ctx context.Context, path string, query url.Values, out any) (rerr error) {
	u := c.root + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Access-Token", c.token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{err: fmt.Errorf("tiktok %s: %w", path, err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{err: fmt.Errorf("tiktok %s: read body: %w", path, err)}
	}
	var env envelope
	decErr := json.Unmarshal(raw, &env)
	if resp.StatusCode != http.StatusOK && (decErr != nil || env.Code == 0) {
		ae := &apiError{path: path, status: resp.StatusCode, message: snippet(raw)}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return retryable{err: ae}
		}
		return ae
	}
	if decErr != nil {
		return fmt.Errorf("tiktok %s: decode response: %w", path, decErr)
	}
	if env.Code != 0 {
		ae := &apiError{path: path, status: resp.StatusCode, code: env.Code, message: env.Message}
		switch {
		case throttleCodes[env.Code], transientCodes[env.Code]:
			return retryable{err: ae}
		case authCodes[env.Code]:
			return synclet.ConfigError(ae)
		}
		return ae
	}
	dec := json.NewDecoder(bytes.NewReader(env.Data))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("tiktok %s: decode data: %w", path, err)
	}
	return nil
}

// snippet returns the start of a body for an error message.
func snippet(raw []byte) string {
	s := strings.TrimSpace(string(raw))
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// jsonList encodes a list parameter the way the API expects it, as JSON.
func jsonList(items ...string) string {
	b, _ := json.Marshal(items)
	return string(b)
}

// page is the data of one page of a paged response.
type page struct {
	List     []map[string]any `json:"list"`
	PageInfo struct {
		Page      int `json:"page"`
		TotalPage int `json:"total_page"`
	} `json:"page_info"`
}

// list calls fn with every item of a paged endpoint, requesting pages until
// page_info.total_page.
func (c *apiClient) list(ctx context.Context, path string, query url.Values, fn func(map[string]any) error) error {
	q := url.Values{}
	for k, v := range query {
		q[k] = v
	}
	q.Set("page_size", fmt.Sprint(pageSize))
	for n := 1; ; n++ {
		q.Set("page", fmt.Sprint(n))
		var p page
		if err := c.get(ctx, path, q, &p); err != nil {
			return err
		}
		for _, item := range p.List {
			if err := fn(item); err != nil {
				return err
			}
		}
		if len(p.List) == 0 || n >= p.PageInfo.TotalPage {
			return nil
		}
	}
}
