package main

import (
	"bytes"
	"context"
	"encoding/base64"
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
	defaultRetryDelay = time.Second
	// rateLimitDelay is the first wait after a 429 without Retry-After; it
	// doubles up to maxRateLimitDelay.
	rateLimitDelay    = time.Minute
	maxRateLimitDelay = 15 * time.Minute
	// retries is how many times a request is repeated after a 5xx response
	// or a transport error.
	retries = 3
	// rateLimitRetries is how many times a request is repeated after a 429.
	rateLimitRetries = 8
	// hourlyLimit is the requests per hour Mixpanel allows a project on each
	// of the Export and Query APIs.
	hourlyLimit = 60
)

// api is a Mixpanel API with its own host and hourly request budget.
type api int

const (
	queryAPI api = iota
	exportAPI
)

func (a api) String() string {
	if a == exportAPI {
		return "Export API"
	}
	return "Query API"
}

// hosts are the API roots of each region.
var hosts = map[string]map[api]string{
	regionUS: {queryAPI: "https://mixpanel.com/api/2.0/", exportAPI: "https://data.mixpanel.com/api/2.0/"},
	regionEU: {queryAPI: "https://eu.mixpanel.com/api/2.0/", exportAPI: "https://data-eu.mixpanel.com/api/2.0/"},
}

// limiter allows at most limit requests in any window: next waits until
// the oldest of the last limit requests is window old.
type limiter struct {
	limit  int
	window time.Duration
	sent   []time.Time
}

func (l *limiter) next(ctx context.Context, now func() time.Time, wait func(context.Context, time.Duration) error) error {
	if len(l.sent) >= l.limit {
		if d := l.sent[len(l.sent)-l.limit].Add(l.window).Sub(now()); d > 0 {
			if err := wait(ctx, d); err != nil {
				return err
			}
		}
	}
	l.sent = append(l.sent, now())
	if len(l.sent) > l.limit {
		l.sent = l.sent[len(l.sent)-l.limit:]
	}
	return nil
}

// apiClient sends authenticated requests to the Mixpanel APIs. Credentials
// travel only in the Authorization header. http serves the Query API with a
// bound on each response; export serves the Export API, whose response
// streams a whole date window, and bounds only the wait for its headers. It
// is used by one goroutine at a time, so at most one request is in flight,
// within every concurrency limit of Mixpanel.
type apiClient struct {
	http       *http.Client
	export     *http.Client
	roots      map[api]string
	auth       string
	projectID  int64
	retryDelay time.Duration
	now        func() time.Time
	wait       func(ctx context.Context, d time.Duration) error
	limiters   map[api]*limiter
}

func (s Source) apiClient(cfg Config) *apiClient {
	roots := map[api]string{}
	for a, u := range hosts[cfg.region()] {
		roots[a] = u
	}
	if s.queryURL != "" {
		roots[queryAPI] = s.queryURL
	}
	if s.exportURL != "" {
		roots[exportAPI] = s.exportURL
	}
	c := &apiClient{
		http:       s.client,
		export:     s.client,
		roots:      roots,
		retryDelay: s.retryDelay,
		now:        s.clock,
		wait:       s.wait,
		limiters: map[api]*limiter{
			queryAPI:  {limit: hourlyLimit, window: time.Hour},
			exportAPI: {limit: hourlyLimit, window: time.Hour},
		},
	}
	cr := cfg.Credentials
	if cr.OptionTitle == authServiceAccount {
		c.auth = basic(cr.Username, cr.Secret)
		c.projectID = cr.ProjectID
	} else {
		c.auth = basic(cr.APISecret, "")
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 30 * time.Minute}
	}
	if c.export == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.ResponseHeaderTimeout = 30 * time.Minute
		c.export = &http.Client{Transport: t}
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	if c.wait == nil {
		c.wait = sleep
	}
	return c
}

func basic(user, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+password))
}

// apiError is a response with an error status.
type apiError struct {
	api     api
	path    string
	status  int
	message string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("%s %s: HTTP %d", e.api, e.path, e.status)
	if e.message != "" {
		msg += ": " + e.message
	}
	return msg
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// rateLimited is a 429; wait is its Retry-After, zero when it names none.
type rateLimited struct {
	err  *apiError
	wait time.Duration
}

func (e rateLimited) Error() string { return e.err.Error() }
func (e rateLimited) Unwrap() error { return e.err }

// errorMessage returns the error of a Mixpanel error body, which is
// {"error": "..."} on most endpoints, else the body as text.
func errorMessage(body []byte) string {
	var eb struct {
		Error any `json:"error"`
	}
	if json.Unmarshal(body, &eb) == nil {
		if s, ok := eb.Error.(string); ok && s != "" {
			return s
		}
	}
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300] + "..."
	}
	return msg
}

// request is one API call: form values go in the body of a POST and in the
// query of a GET.
type request struct {
	api    api
	method string
	path   string
	params url.Values
}

// send sends req and hands the body of its 200 response to handle. Each
// attempt first waits for the hourly budget of its API. A 429 waits for its
// Retry-After, else a delay that starts at a minute and doubles up to 15
// minutes, up to rateLimitRetries times; a 5xx response or a transport
// error is repeated with a doubling delay up to retries times. A failure
// inside handle is not retried, since handle may have emitted records.
func (c *apiClient) send(ctx context.Context, req request, handle func(io.Reader) error) error {
	delay, limitDelay := c.retryDelay, rateLimitDelay
	var failures, limited int
	for {
		err := c.attempt(ctx, req, handle)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var rl rateLimited
		if errors.As(err, &rl) {
			if limited == rateLimitRetries {
				return fmt.Errorf("%w; gave up after %d waits for the rate limit", err, rateLimitRetries)
			}
			limited++
			d := rl.wait
			if d == 0 {
				d = limitDelay
				limitDelay = min(limitDelay*2, maxRateLimitDelay)
			}
			if err := c.wait(ctx, d); err != nil {
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

func (c *apiClient) attempt(ctx context.Context, req request, handle func(io.Reader) error) (rerr error) {
	if err := c.limiters[req.api].next(ctx, c.now, c.wait); err != nil {
		return err
	}
	q := url.Values{}
	if c.projectID != 0 {
		q.Set("project_id", strconv.FormatInt(c.projectID, 10))
	}
	var body io.Reader
	if req.method == http.MethodPost {
		body = strings.NewReader(req.params.Encode())
	} else {
		for k, v := range req.params {
			q[k] = v
		}
	}
	u := c.roots[req.api] + req.path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	hr, err := http.NewRequestWithContext(ctx, req.method, u, body)
	if err != nil {
		return err
	}
	hr.Header.Set("Authorization", c.auth)
	hr.Header.Set("Accept", "application/json")
	if body != nil {
		hr.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	hc := c.http
	if req.api == exportAPI {
		hc = c.export
	}
	resp, err := hc.Do(hr)
	if err != nil {
		return retryable{fmt.Errorf("%s %s: %w", req.api, req.path, err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	if resp.StatusCode == http.StatusOK {
		return handle(resp.Body)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return retryable{fmt.Errorf("%s %s: read body: %w", req.api, req.path, err)}
	}
	ae := &apiError{api: req.api, path: req.path, status: resp.StatusCode, message: errorMessage(raw)}
	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		return rateLimited{err: ae, wait: retryAfter(resp.Header.Get("Retry-After"))}
	case resp.StatusCode == http.StatusUnauthorized:
		return synclet.ConfigError(fmt.Errorf("invalid credentials: check the service account username and secret, or the project secret (%w)", ae))
	case resp.StatusCode == http.StatusForbidden:
		return synclet.ConfigError(fmt.Errorf("the credentials have no access to this project or endpoint: a service account needs a project role of Analyst or above (%w)", ae))
	case resp.StatusCode >= 500:
		return retryable{ae}
	}
	return ae
}

// retryAfter returns a Retry-After header in seconds, zero when absent.
func retryAfter(h string) time.Duration {
	if n, err := strconv.Atoi(strings.TrimSpace(h)); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return 0
}

// getJSON decodes the JSON response of req into out, keeping numbers as
// json.Number.
func (c *apiClient) getJSON(ctx context.Context, req request, out any) error {
	return c.send(ctx, req, func(r io.Reader) error {
		raw, err := io.ReadAll(r)
		if err != nil {
			return fmt.Errorf("%s %s: read body: %w", req.api, req.path, err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("%s %s: decode response: %w", req.api, req.path, err)
		}
		return nil
	})
}

func get(a api, path string, params url.Values) request {
	return request{api: a, method: http.MethodGet, path: path, params: params}
}
