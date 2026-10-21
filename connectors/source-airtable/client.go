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
	"sync"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL    = "https://api.airtable.com/v0/"
	defaultRetryDelay = time.Second
	// requestSpacing keeps requests to one base within Airtable's limit of
	// 5 requests per second per base.
	requestSpacing = 200 * time.Millisecond
	// tokenSpacing keeps all requests within Airtable's limit of 50
	// requests per second per token.
	tokenSpacing = 20 * time.Millisecond
	// rateLimitWait is how long Airtable blocks a base after a 429; the
	// response carries no Retry-After.
	rateLimitWait = 30 * time.Second
	// rateLimitRetries is how many times a request is repeated after a 429.
	rateLimitRetries = 5
	// metadataRateLimitBudget is the total rate-limit waiting check and
	// discover allow across all their requests, so that they end well
	// within the platform's 5-minute limit on a task.
	metadataRateLimitBudget = time.Minute
	// defaultMetadataTimeout bounds check and discover below the platform's
	// 5-minute limit on a task: each of their requests may take the HTTP
	// timeout on every retry.
	defaultMetadataTimeout = 4 * time.Minute
	// billingLimitExceeded is the 429 error type of a workspace that has
	// used up the API calls of its plan; it does not clear within a run.
	billingLimitExceeded = "PUBLIC_API_BILLING_LIMIT_EXCEEDED"
	// retries is how many times a request is repeated after a 5xx response
	// or a transport error.
	retries = 3
)

// apiClient sends authenticated GET requests to the Airtable API. The token
// travels only in the Authorization header, never in a URL. It is safe for
// concurrent use.
type apiClient struct {
	http       *http.Client
	baseURL    string
	auth       string
	retryDelay time.Duration
	now        func() time.Time
	wait       func(ctx context.Context, d time.Duration) error
	// rateLimitBudget is the total rate-limit waiting allowed across all
	// requests; zero allows rateLimitRetries waits per request without a
	// total.
	rateLimitBudget time.Duration

	mu      sync.Mutex
	last    map[string]time.Time
	lastAny time.Time
	waited  time.Duration
}

// apiClient returns a client for cfg whose requests together wait at most
// rateLimitBudget for rate limits, or zero for no total.
func (s Source) apiClient(cfg Config, rateLimitBudget time.Duration) *apiClient {
	c := &apiClient{
		http:            s.client,
		baseURL:         s.baseURL,
		auth:            "Bearer " + cfg.PersonalAccessToken,
		retryDelay:      s.retryDelay,
		now:             s.now,
		wait:            s.wait,
		rateLimitBudget: rateLimitBudget,
		last:            map[string]time.Time{},
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

// errMetadataTimeout is the cause of a check or discover that ran out of
// metadataTimeout.
var errMetadataTimeout = errors.New("metadata timeout")

// withMetadataTimeout runs f with a context that ends after the metadata
// timeout, and names that timeout in the error of f when it ended it.
func (s Source) withMetadataTimeout(ctx context.Context, f func(ctx context.Context) error) error {
	d := s.metadataTimeout
	if d == 0 {
		d = defaultMetadataTimeout
	}
	ctx, cancel := context.WithTimeoutCause(ctx, d, errMetadataTimeout)
	defer cancel()
	err := f(ctx)
	if err != nil && errors.Is(context.Cause(ctx), errMetadataTimeout) {
		return fmt.Errorf("no answer from Airtable within %s: %w", d, err)
	}
	return err
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

// rateLimited is a 429 response with the error type of its body.
type rateLimited struct {
	path    string
	errType string
	detail  string
}

func (e rateLimited) Error() string { return fmt.Sprintf("GET %s %s", e.path, e.detail) }

// get decodes the JSON response of path into out. Requests that share base
// are spaced by requestSpacing, and all requests by tokenSpacing; base is
// the base id, or "" for requests not about one base. A 429 is repeated
// after rateLimitWait up to rateLimitRetries times and within
// rateLimitBudget, except one for a used-up plan API call limit, which is a
// configuration error; a 5xx response or a transport error is repeated with
// a doubling delay up to retries times.
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
			if rl.errType == billingLimitExceeded {
				return synclet.ConfigError(fmt.Errorf("the Airtable workspace has used up the API calls its plan allows (%s); requests succeed again once the limit resets or the plan is upgraded: %w", billingLimitExceeded, err))
			}
			if limited == rateLimitRetries {
				return fmt.Errorf("%w; gave up after %d waits of %s", err, rateLimitRetries, rateLimitWait)
			}
			if !c.spend(rateLimitWait) {
				return fmt.Errorf("%w; gave up after waiting %s in total for Airtable's rate limit", err, c.rateLimitBudget)
			}
			limited++
			synclet.Logger(ctx).Warn("rate limited by Airtable; waiting before repeating the request",
				"path", rl.path, "type", rl.errType, "attempt", fmt.Sprintf("%d/%d", limited, rateLimitRetries), "wait", rateLimitWait)
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

// spend takes d from rateLimitBudget, or reports false when less than d is
// left.
func (c *apiClient) spend(d time.Duration) bool {
	if c.rateLimitBudget == 0 {
		return true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waited+d > c.rateLimitBudget {
		return false
	}
	c.waited += d
	return true
}

// space waits for the slot reserve gives a request for base.
func (c *apiClient) space(ctx context.Context, base string) error {
	if d := c.reserve(base).Sub(c.now()); d > 0 {
		return c.wait(ctx, d)
	}
	return nil
}

// reserve returns the time a request for base may start: requestSpacing
// after the previous request for base and tokenSpacing after the previous
// request of any base.
func (c *apiClient) reserve(base string) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.now()
	if last, ok := c.last[base]; ok && last.Add(requestSpacing).After(at) {
		at = last.Add(requestSpacing)
	}
	if !c.lastAny.IsZero() && c.lastAny.Add(tokenSpacing).After(at) {
		at = c.lastAny.Add(tokenSpacing)
	}
	c.last[base] = at
	c.lastAny = at
	return at
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
	errType, _ := airtableError(body)
	detail := "(HTTP " + strconv.Itoa(resp.StatusCode) + ")"
	if d := errorDetail(body); d != "" {
		detail += ": " + d
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return rateLimited{path: req.URL.Path, errType: errType, detail: detail}
	case http.StatusUnauthorized:
		return synclet.ConfigError(fmt.Errorf("invalid personal access token %s", detail))
	}
	he := &httpError{status: resp.StatusCode, path: req.URL.Path, detail: detail}
	if resp.StatusCode >= 500 {
		return retryable{he}
	}
	return he
}

// errorDetail returns the type and message of an Airtable error body.
func errorDetail(body []byte) string {
	typ, msg := airtableError(body)
	switch {
	case typ != "" && msg != "":
		return typ + ": " + msg
	case typ != "":
		return typ
	default:
		return msg
	}
}

// airtableError returns the type and message of an Airtable error body,
// whose "error" is a code string or an object with a type and a message,
// or whose "errors" lists objects with an "error" code and a message, of
// which the first counts.
func airtableError(body []byte) (typ, msg string) {
	var e struct {
		Error  json.RawMessage `json:"error"`
		Errors []struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if json.Unmarshal(body, &e) != nil {
		return "", ""
	}
	if len(e.Error) == 0 {
		if len(e.Errors) > 0 {
			return e.Errors[0].Error, e.Errors[0].Message
		}
		return "", ""
	}
	var code string
	if json.Unmarshal(e.Error, &code) == nil {
		return code, ""
	}
	var obj struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	if json.Unmarshal(e.Error, &obj) != nil {
		return "", ""
	}
	return obj.Type, obj.Message
}

// statusOf returns the HTTP status of an httpError in err, or 0.
func statusOf(err error) int {
	var he *httpError
	if errors.As(err, &he) {
		return he.status
	}
	return 0
}
