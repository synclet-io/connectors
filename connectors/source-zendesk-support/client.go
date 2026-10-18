package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultRetryDelay = time.Second
	// defaultExportInterval spaces incremental export requests so a read
	// stays within Zendesk's limit of 10 export requests per minute.
	defaultExportInterval = 6 * time.Second
	// defaultRetryAfter is the wait after a 429 without a Retry-After header.
	defaultRetryAfter = time.Minute
	// retries is how many times a request is repeated after a 5xx response
	// or a transport error.
	retries = 3
	// rateLimitRetries is how many times a request is repeated after a 429.
	rateLimitRetries = 5
)

// apiClient sends authenticated GET requests to the Zendesk Support API.
// The API token travels only in the Authorization header, never in a URL.
type apiClient struct {
	http           *http.Client
	baseURL        string
	subdomain      string
	auth           string
	retryDelay     time.Duration
	exportInterval time.Duration
	now            func() time.Time
	wait           func(ctx context.Context, d time.Duration) error
	lastExport     time.Time
}

func (s Source) apiClient(cfg Config) *apiClient {
	c := &apiClient{
		http:           s.client,
		baseURL:        s.baseURL,
		subdomain:      cfg.Subdomain,
		auth:           "Basic " + base64.StdEncoding.EncodeToString([]byte(cfg.Email+"/token:"+cfg.APIToken)),
		retryDelay:     s.retryDelay,
		exportInterval: s.exportInterval,
		now:            s.clock(),
		wait:           s.wait,
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: time.Minute}
	}
	if c.baseURL == "" {
		c.baseURL = "https://" + cfg.Subdomain + ".zendesk.com/api/v2/"
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	if c.exportInterval == 0 {
		c.exportInterval = defaultExportInterval
	}
	if c.wait == nil {
		c.wait = sleep
	}
	return c
}

func (s Source) clock() func() time.Time {
	if s.now != nil {
		return s.now
	}
	return time.Now
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

// rateLimited is a 429 response; the request may be repeated after wait.
type rateLimited struct {
	path string
	wait time.Duration
}

func (e rateLimited) Error() string {
	return fmt.Sprintf("GET %s (HTTP 429): rate limited", e.path)
}

// exportGet is get for an incremental export: it waits until
// exportInterval has passed since the previous export request of this
// client.
func (c *apiClient) exportGet(ctx context.Context, path string, query url.Values, out any) error {
	if !c.lastExport.IsZero() {
		if d := c.lastExport.Add(c.exportInterval).Sub(c.now()); d > 0 {
			if err := c.wait(ctx, d); err != nil {
				return err
			}
		}
	}
	c.lastExport = c.now()
	return c.get(ctx, path, query, out)
}

// get decodes the JSON response of path into out. A 429 is repeated after
// its Retry-After up to rateLimitRetries times; a 5xx response or a
// transport error is repeated with a doubling delay up to retries times.
func (c *apiClient) get(ctx context.Context, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	delay := c.retryDelay
	var failures, limited int
	for {
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
		if err := sleep(ctx, delay); err != nil {
			return err
		}
		delay *= 2
	}
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
		var dns *net.DNSError
		if errors.As(err, &dns) && dns.IsNotFound {
			return synclet.ConfigError(fmt.Errorf("wrong subdomain %q: host %s not found", c.subdomain, dns.Name))
		}
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
		return rateLimited{path: req.URL.Path, wait: retryAfter(resp.Header.Get("Retry-After"))}
	case http.StatusUnauthorized:
		return synclet.ConfigError(fmt.Errorf("invalid email or API token %s", detail))
	case http.StatusForbidden:
		return synclet.ConfigError(fmt.Errorf("missing permission for GET %s %s", req.URL.Path, detail))
	}
	he := &httpError{status: resp.StatusCode, path: req.URL.Path, detail: detail}
	if resp.StatusCode >= 500 {
		return retryable{he}
	}
	return he
}

// errorDetail returns the message of a Zendesk error body, whose "error" is
// a code string or an object with a title and a message.
func errorDetail(body []byte) string {
	var e struct {
		Error       json.RawMessage `json:"error"`
		Description string          `json:"description"`
	}
	if json.Unmarshal(body, &e) != nil {
		return ""
	}
	var code string
	if json.Unmarshal(e.Error, &code) != nil {
		var obj struct {
			Title   string `json:"title"`
			Message string `json:"message"`
		}
		_ = json.Unmarshal(e.Error, &obj)
		code = obj.Title
		if e.Description == "" {
			e.Description = obj.Message
		}
	}
	switch {
	case code != "" && e.Description != "":
		return code + ": " + e.Description
	case code != "":
		return code
	default:
		return e.Description
	}
}

// retryAfter parses a Retry-After header given in seconds.
func retryAfter(h string) time.Duration {
	n, err := strconv.Atoi(h)
	if err != nil || n < 0 {
		return defaultRetryAfter
	}
	return time.Duration(n) * time.Second
}
