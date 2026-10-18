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
	"sync"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// apiVersion is the Salesforce REST API version of every request.
	apiVersion        = "v62.0"
	defaultRetryDelay = 2 * time.Second
	maxRetryDelay     = time.Minute
	// attempts is how many times a request is sent while it fails with a
	// 5xx response, 429 or a transport error.
	attempts = 5
)

// apiClient sends authenticated GET requests to the Salesforce REST API of
// one org. The access token travels only in the Authorization header.
type apiClient struct {
	http       *http.Client
	instance   string
	auth       *tokenSource
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
	maxSelect  int

	mu       sync.Mutex
	used     int
	allotted int
}

func (s Source) apiClient(cfg Config) (*apiClient, error) {
	instance, err := cfg.instance()
	if err != nil {
		return nil, synclet.ConfigError(err)
	}
	hc := s.client
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}
	host := s.loginURL
	if host == "" {
		host = defaultLoginURL
	}
	if cfg.IsSandbox {
		host = s.sandboxURL
		if host == "" {
			host = defaultSandboxURL
		}
	}
	c := &apiClient{
		http:     hc,
		instance: instance,
		auth: &tokenSource{
			http: hc, url: strings.TrimRight(host, "/") + "/services/oauth2/token", instance: instance, isSandbox: cfg.IsSandbox,
			clientID: cfg.ClientID, clientSecret: cfg.ClientSecret, refreshToken: cfg.RefreshToken,
		},
		retryDelay: s.retryDelay,
		wait:       s.wait,
		maxSelect:  s.maxSelectLen,
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	if c.wait == nil {
		c.wait = sleep
	}
	if c.maxSelect == 0 {
		c.maxSelect = defaultMaxSelectLen
	}
	return c, nil
}

// warnRotation makes the client log a warning when the token endpoint
// rotates the refresh token, for commands that cannot save the new one.
func (c *apiClient) warnRotation() {
	c.auth.rotated = func(ctx context.Context, _ string) error {
		synclet.Logger(ctx).Warn("the token endpoint rotated the refresh token; only read saves the new one, so use a connected app whose refresh token is valid until revoked")
		return nil
	}
}

// apiError is a failed Salesforce REST API request; code is the errorCode
// of its first error, such as INVALID_SESSION_ID.
type apiError struct {
	path    string
	status  int
	code    string
	message string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("GET %s: HTTP %d", e.path, e.status)
	if e.code != "" {
		msg += " " + e.code
	}
	if e.message != "" {
		msg += ": " + e.message
	}
	return msg
}

func newAPIError(path string, status int, raw []byte) *apiError {
	ae := &apiError{path: path, status: status}
	var body []struct {
		ErrorCode string `json:"errorCode"`
		Message   string `json:"message"`
	}
	if json.Unmarshal(raw, &body) != nil || len(body) == 0 {
		ae.message = strings.TrimSpace(string(raw))
		return ae
	}
	ae.code, ae.message = body[0].ErrorCode, body[0].Message
	return ae
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// get requests path, relative to /services/data/<apiVersion>/ or an
// absolute path such as a nextRecordsUrl, and decodes the response into
// out, numbers as json.Number. A request rejected with 401 gets a new access
// token and is sent once more; 429, 5xx and transport errors are sent again
// after a doubling delay, up to attempts times in all.
func (c *apiClient) get(ctx context.Context, path string, query url.Values, out any) error {
	target := c.instance + "/services/data/" + apiVersion + "/" + path
	if strings.HasPrefix(path, "/") {
		target = c.instance + path
	}
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	delay := c.retryDelay
	refreshed := false
	for attempt := 1; ; attempt++ {
		err := c.do(ctx, path, target, out)
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
		if err := c.wait(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

func (c *apiClient) do(ctx context.Context, path, target string, out any) (rerr error) {
	token, err := c.auth.get(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{err: fmt.Errorf("GET %s: %w", path, err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	c.track(resp.Header.Get("Sforce-Limit-Info"))
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{err: fmt.Errorf("GET %s: read body: %w", path, err)}
	}
	if resp.StatusCode == http.StatusOK {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("GET %s: decode response: %w", path, err)
		}
		return nil
	}
	ae := newAPIError(path, resp.StatusCode, raw)
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		c.auth.invalidate(token)
		return ae
	case ae.code == "API_DISABLED_FOR_ORG", ae.code == "API_CURRENTLY_DISABLED":
		return synclet.ConfigError(fmt.Errorf("API access is not enabled for this user or org: the org's edition must include the API and the user's profile needs the API Enabled permission (%w)", ae))
	case ae.code == "REQUEST_LIMIT_EXCEEDED":
		return fmt.Errorf("the org's daily API request limit is used up; the read can run again once Salesforce frees requests over the next 24 hours (%w)", ae)
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return retryable{err: ae}
	}
	return ae
}

// track records the api-usage of a Sforce-Limit-Info header, if it has one.
func (c *apiClient) track(header string) {
	used, allotted := parseLimitInfo(header)
	if allotted == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.used, c.allotted = used, allotted
}

// usage returns the latest api-usage the API reported: requests used today
// and the daily allocation, zero before any report.
func (c *apiClient) usage() (used, allotted int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used, c.allotted
}

// parseLimitInfo returns the api-usage=<used>/<allotted> pair of a
// Sforce-Limit-Info header, or zeros when it has none.
func parseLimitInfo(header string) (used, allotted int) {
	for part := range strings.SplitSeq(header, ",") {
		v, ok := strings.CutPrefix(strings.TrimSpace(part), "api-usage=")
		if !ok {
			continue
		}
		a, b, ok := strings.Cut(v, "/")
		if !ok {
			return 0, 0
		}
		u, err1 := strconv.Atoi(a)
		m, err2 := strconv.Atoi(b)
		if err1 != nil || err2 != nil || m <= 0 {
			return 0, 0
		}
		return u, m
	}
	return 0, 0
}

// queryPage is one page of a query or queryAll response.
type queryPage struct {
	Done           bool             `json:"done"`
	NextRecordsURL string           `json:"nextRecordsUrl"`
	Records        []map[string]any `json:"records"`
}

// query runs a SOQL query, through queryAll when all is set so soft-deleted
// rows are included, and calls fn with the records of every page, following
// nextRecordsUrl. Each record is passed without its attributes.
func (c *apiClient) query(ctx context.Context, all bool, soql string, fn func([]map[string]any) error) error {
	path := "query"
	if all {
		path = "queryAll"
	}
	query := url.Values{"q": {soql}}
	for {
		var p queryPage
		if err := c.get(ctx, path, query, &p); err != nil {
			return err
		}
		for _, r := range p.Records {
			delete(r, "attributes")
		}
		if err := fn(p.Records); err != nil {
			return err
		}
		if p.Done || p.NextRecordsURL == "" {
			return nil
		}
		if !strings.HasPrefix(p.NextRecordsURL, "/services/data/") {
			return fmt.Errorf("%s: unexpected nextRecordsUrl %q", path, p.NextRecordsURL)
		}
		path, query = p.NextRecordsURL, nil
	}
}

// encodeQuery returns q as it is sent in the q parameter of a URL.
func encodeQuery(q string) string { return url.QueryEscape(q) }
