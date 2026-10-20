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
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL    = "https://searchconsole.googleapis.com"
	apiVersion        = "webmasters/v3"
	defaultRetryDelay = 2 * time.Second
	maxRetryDelay     = time.Minute
	// attempts is how many times a request is sent while it fails with a
	// retryable 429, a 5xx response or a transport error.
	attempts = 5
	// paceInterval is the least time between two API requests: 1,200 per
	// minute, the Search Console quota per site and per user.
	paceInterval = 50 * time.Millisecond
)

// apiClient sends authenticated requests to the Search Console API. The
// access token travels only in the Authorization header.
type apiClient struct {
	http       *http.Client
	root       string
	auth       *tokenSource
	retryDelay time.Duration
	jitter     func(time.Duration) time.Duration
	wait       func(ctx context.Context, d time.Duration) error
	pace       *pacer
}

func (s Source) apiClient(ctx context.Context, cfg Config) (*apiClient, error) {
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
		tokenURL = googleTokenURL
	}
	auth, err := newTokenSource(ctx, hc, tokenURL, cfg.Credentials)
	if err != nil {
		return nil, err
	}
	c := &apiClient{
		http:       hc,
		root:       strings.TrimRight(root, "/") + "/" + apiVersion + "/",
		auth:       auth,
		retryDelay: s.retryDelay,
		jitter:     s.jitter,
		wait:       s.wait,
		pace:       &pacer{interval: paceInterval, now: s.clock, wait: s.paceWait},
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
	if c.pace.wait == nil {
		c.pace.wait = sleep
	}
	return c, nil
}

// pacer spaces requests at least interval apart.
type pacer struct {
	interval time.Duration
	now      func() time.Time
	wait     func(ctx context.Context, d time.Duration) error

	mu   sync.Mutex
	last time.Time
}

// next waits until interval has passed since the previous request.
func (p *pacer) next(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.last.IsZero() {
		if d := p.interval - p.now().Sub(p.last); d > 0 {
			if err := p.wait(ctx, d); err != nil {
				return err
			}
		}
	}
	p.last = p.now()
	return nil
}

// sitePath returns the API path of a site: the site URL escaped as one path
// segment, with : and / escaped too.
func sitePath(site string) string {
	return "sites/" + strings.ReplaceAll(url.QueryEscape(site), "+", "%20")
}

// apiError is a failed API request. reasons are the reason values of the
// ErrorInfo details and of the v3 errors list, such as SERVICE_DISABLED or
// dailyLimitExceeded.
type apiError struct {
	path     string
	status   int
	state    string
	reasons  []string
	consumer string
	message  string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%s: HTTP %d", e.path, e.status)
	switch {
	case len(e.reasons) > 0:
		msg += " " + e.reasons[0]
	case e.state != "":
		msg += " " + e.state
	}
	if e.message != "" {
		msg += ": " + e.message
	}
	return msg
}

func (e *apiError) has(reason string) bool { return slices.Contains(e.reasons, reason) }

// dailyQuota reports whether the error is an exhausted daily quota, which no
// retry within the day can pass.
func (e *apiError) dailyQuota() bool {
	return e.has("dailyLimitExceeded") ||
		e.status == http.StatusTooManyRequests && strings.Contains(strings.ToLower(e.message), "per day")
}

// rateLimited reports whether the error is a per-minute rate or quota limit,
// which Search Console may answer with 403 as well as 429 and which passes
// after a wait.
func (e *apiError) rateLimited() bool {
	return e.has("rateLimitExceeded") || e.has("userRateLimitExceeded") || e.has("quotaExceeded")
}

// errorBody is the error body of Google APIs: the ErrorInfo details of the
// newer error model and the errors list of v3 APIs.
type errorBody struct {
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
		Errors  []struct {
			Reason string `json:"reason"`
		} `json:"errors"`
		Details []struct {
			Reason   string            `json:"reason"`
			Metadata map[string]string `json:"metadata"`
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
	for _, d := range body.Error.Details {
		if d.Reason != "" {
			ae.reasons = append(ae.reasons, d.Reason)
		}
		if c := d.Metadata["consumer"]; c != "" && ae.consumer == "" {
			ae.consumer = c
		}
	}
	for _, e := range body.Error.Errors {
		if e.Reason != "" && !slices.Contains(ae.reasons, e.Reason) {
			ae.reasons = append(ae.reasons, e.Reason)
		}
	}
	return ae
}

var projectInMessage = regexp.MustCompile(`project (\d+)`)

// project returns the Google Cloud project number a SERVICE_DISABLED error
// names, or "" when it names none.
func (e *apiError) project() string {
	if p, ok := strings.CutPrefix(e.consumer, "projects/"); ok {
		return p
	}
	if m := projectInMessage.FindStringSubmatch(e.message); m != nil {
		return m[1]
	}
	return ""
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// notVerified is the configuration error of a site the credentials have no
// access to.
func (c *apiClient) notVerified(site string, cause error) error {
	err := fmt.Errorf("site %s is not verified for %s: add it as a user in Search Console", site, c.auth.account)
	if cause != nil {
		err = fmt.Errorf("%w (%w)", err, cause)
	}
	return synclet.ConfigError(err)
}

// send sends a request for site and decodes the response into out. A request
// rejected with 401 gets a new access token and is sent once more; a
// retryable failure is sent again after a doubling, jittered delay, up to
// attempts times in all. A disabled API, missing scope and a site the
// credentials cannot read become configuration errors; an exhausted daily
// quota fails at once.
func (c *apiClient) send(ctx context.Context, method, path, site string, body, out any) error {
	var payload []byte
	if body != nil {
		var err error
		if payload, err = json.Marshal(body); err != nil {
			return err
		}
	}
	delay := c.retryDelay
	refreshed := false
	for attempt := 1; ; attempt++ {
		err := c.do(ctx, method, path, payload, out)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var ae *apiError
		if errors.As(err, &ae) {
			switch {
			case ae.status == http.StatusUnauthorized && !refreshed:
				refreshed = true
				attempt--
				continue
			case ae.dailyQuota():
				return fmt.Errorf("daily quota of Search Console API is exhausted; the next sync continues from the saved state (%w)", ae)
			case ae.has("SERVICE_DISABLED") || ae.has("accessNotConfigured"):
				project := ae.project()
				if project == "" {
					project = "(unknown)"
				}
				return synclet.ConfigError(fmt.Errorf("Google Search Console API is not enabled in Google Cloud project %s: enable searchconsole.googleapis.com (%w)", project, ae)) //nolint:staticcheck // the message starts with the product name
			case ae.has("ACCESS_TOKEN_SCOPE_INSUFFICIENT") || strings.Contains(ae.message, "insufficient authentication scopes"):
				return synclet.ConfigError(fmt.Errorf("the credentials lack the %s scope: authorize the refresh token with it (%w)", scope, ae))
			case ae.rateLimited():
			case ae.status == http.StatusForbidden, ae.status == http.StatusNotFound:
				return c.notVerified(site, ae)
			}
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

func (c *apiClient) do(ctx context.Context, method, path string, payload []byte, out any) (rerr error) {
	token, err := c.auth.get()
	if err != nil {
		return err
	}
	if err := c.pace.next(ctx); err != nil {
		return err
	}
	var reqBody io.Reader
	if payload != nil {
		reqBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.root+path, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
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
		c.auth.invalidate()
		return ae
	case ae.dailyQuota():
		return ae
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500, ae.rateLimited():
		return retryable{err: ae}
	}
	return ae
}

// site is a sites.get response.
type site struct {
	SiteURL         string `json:"siteUrl"`
	PermissionLevel string `json:"permissionLevel"`
}

func (c *apiClient) site(ctx context.Context, siteURL string) (site, error) {
	var s site
	err := c.send(ctx, http.MethodGet, sitePath(siteURL), siteURL, nil, &s)
	return s, err
}

// sitemap is one entry of a sitemaps.list response. int64 fields come as
// JSON strings.
type sitemap struct {
	Path            string      `json:"path"`
	LastSubmitted   *string     `json:"lastSubmitted"`
	LastDownloaded  *string     `json:"lastDownloaded"`
	IsPending       *bool       `json:"isPending"`
	IsSitemapsIndex *bool       `json:"isSitemapsIndex"`
	Type            *string     `json:"type"`
	Warnings        json.Number `json:"warnings"`
	Errors          json.Number `json:"errors"`
	Contents        []struct {
		Type      *string     `json:"type"`
		Submitted json.Number `json:"submitted"`
		Indexed   json.Number `json:"indexed"`
	} `json:"contents"`
}

func (c *apiClient) sitemaps(ctx context.Context, siteURL string) ([]sitemap, error) {
	var r struct {
		Sitemap []sitemap `json:"sitemap"`
	}
	err := c.send(ctx, http.MethodGet, sitePath(siteURL)+"/sitemaps", siteURL, nil, &r)
	return r.Sitemap, err
}

// analyticsRequest is the body of searchAnalytics.query.
type analyticsRequest struct {
	StartDate  string   `json:"startDate"`
	EndDate    string   `json:"endDate"`
	Dimensions []string `json:"dimensions"`
	Type       string   `json:"type"`
	DataState  string   `json:"dataState"`
	RowLimit   int      `json:"rowLimit"`
	StartRow   int      `json:"startRow"`
}

type analyticsRow struct {
	Keys        []string    `json:"keys"`
	Clicks      json.Number `json:"clicks"`
	Impressions json.Number `json:"impressions"`
	CTR         json.Number `json:"ctr"`
	Position    json.Number `json:"position"`
}

// analyticsResponse is one page of a searchAnalytics.query response.
type analyticsResponse struct {
	Rows                    []analyticsRow `json:"rows"`
	ResponseAggregationType string         `json:"responseAggregationType"`
}

func (c *apiClient) query(ctx context.Context, siteURL string, req analyticsRequest) (analyticsResponse, error) {
	var r analyticsResponse
	err := c.send(ctx, http.MethodPost, sitePath(siteURL)+"/searchAnalytics/query", siteURL, req, &r)
	return r, err
}
