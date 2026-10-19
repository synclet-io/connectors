package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	defaultAPIURL = "https://api.linkedin.com/rest/"
	// apiVersion is the LinkedIn-Version header: the monthly version of the
	// Marketing API the requests and fixtures follow. LinkedIn supports a
	// version for at least a year after its release.
	apiVersion        = "202609"
	restliProtocol    = "2.0.0"
	defaultRetryDelay = time.Second
	maxRetryDelay     = time.Minute
	// retries is how many times a request is repeated after HTTP 429, a 5xx
	// response or a transport error.
	retries = 5
	// pageSize is the page size of every paged finder.
	pageSize = 100
)

// apiClient sends authenticated GET requests to the LinkedIn Marketing API.
// The access token travels only in the Authorization header, never in a URL.
type apiClient struct {
	http       *http.Client
	root       string
	auth       *tokenSource
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func (s Source) apiClient(cfg Config) *apiClient {
	hc := s.client
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}
	root, tokenURL := s.apiURL, s.tokenURL
	if root == "" {
		root = defaultAPIURL
	}
	if tokenURL == "" {
		tokenURL = defaultTokenURL
	}
	c := &apiClient{
		http: hc,
		root: strings.TrimRight(root, "/") + "/",
		auth: &tokenSource{
			http: hc, url: tokenURL, now: s.clock(),
			clientID: cfg.ClientID, clientSecret: cfg.ClientSecret, refreshToken: cfg.RefreshToken,
		},
		retryDelay: s.retryDelay,
		wait:       s.wait,
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	if c.wait == nil {
		c.wait = sleep
	}
	return c
}

// apiError is a response with a status other than 200, with the fields of
// LinkedIn's error body when it has one.
type apiError struct {
	route       string
	status      int
	code        string
	serviceCode int
	message     string
}

func (e *apiError) Error() string {
	code := e.code
	if e.serviceCode != 0 {
		code = fmt.Sprintf("%s (%d)", code, e.serviceCode)
	}
	return fmt.Sprintf("linkedin GET %s: HTTP %d %s: %s", e.route, e.status, code, e.message)
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// get decodes the response of route into out. A 401 gets a new access token
// and the request is sent once more; HTTP 429, 5xx responses and transport
// errors are retried after a doubling delay; any other error is returned at
// once.
func (c *apiClient) get(ctx context.Context, route string, q query, out any) error {
	delay := c.retryDelay
	refreshed := false
	for attempt := 0; ; {
		token, err := c.auth.get(ctx)
		if err == nil {
			err = c.do(ctx, token, route, q, out)
			var ae *apiError
			if errors.As(err, &ae) && ae.status == http.StatusUnauthorized && !refreshed {
				refreshed = true
				c.auth.invalidate(token)
				continue
			}
		}
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
		attempt++
		if err := c.wait(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

func (c *apiClient) do(ctx context.Context, token, route string, q query, out any) (rerr error) {
	u := c.root + route
	if len(q) > 0 {
		u += "?" + q.encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("LinkedIn-Version", apiVersion)
	req.Header.Set("X-Restli-Protocol-Version", restliProtocol)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{err: fmt.Errorf("linkedin GET %s: %w", route, err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{err: fmt.Errorf("linkedin GET %s: read body: %w", route, err)}
	}
	if resp.StatusCode != http.StatusOK {
		ae := &apiError{route: route, status: resp.StatusCode}
		var body struct {
			Code        string `json:"code"`
			ServiceCode int    `json:"serviceErrorCode"`
			Message     string `json:"message"`
		}
		if json.Unmarshal(raw, &body) == nil && body.Message != "" {
			ae.code, ae.serviceCode, ae.message = body.Code, body.ServiceCode, body.Message
		} else {
			ae.code, ae.message = http.StatusText(resp.StatusCode), snippet(raw)
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			return retryable{err: ae}
		}
		return ae
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("linkedin GET %s: decode response: %w", route, err)
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

// page is one page of a paged finder.
type page struct {
	Elements []map[string]any `json:"elements"`
	Metadata struct {
		NextPageToken string `json:"nextPageToken"`
	} `json:"metadata"`
}

// list calls fn with every element of a paged finder, following
// metadata.nextPageToken until a page has none.
func (c *apiClient) list(ctx context.Context, route string, q query, fn func(map[string]any) error) error {
	q = q.with("pageSize", fmt.Sprint(pageSize))
	seen := map[string]bool{}
	for n := 1; ; n++ {
		var p page
		if err := c.get(ctx, route, q, &p); err != nil {
			if n > 1 {
				return fmt.Errorf("page %d: %w", n, err)
			}
			return err
		}
		for _, el := range p.Elements {
			if err := fn(el); err != nil {
				return err
			}
		}
		next := p.Metadata.NextPageToken
		if next == "" {
			return nil
		}
		if seen[next] {
			return fmt.Errorf("linkedin GET %s: page token %q repeats", route, next)
		}
		seen[next] = true
		q = q.with("pageToken", next)
	}
}
