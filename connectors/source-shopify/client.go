package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// apiVersion is the Admin API version every request is pinned to.
	apiVersion        = "2026-10"
	tokenHeader       = "X-Shopify-Access-Token"
	defaultRetryDelay = time.Second
	// defaultRetryAfter is the wait after a 429 without a Retry-After header.
	defaultRetryAfter = 2 * time.Second
	// retries is how many times a request is repeated after a 5xx response,
	// a transport error or an INTERNAL_SERVER_ERROR.
	retries = 3
	// limitRetries is how many times a request is repeated after a
	// THROTTLED error or a 429.
	limitRetries = 10
)

// apiClient sends GraphQL requests to one shop's Admin API, one at a time,
// paced by the query cost Shopify reports. The access token travels only
// in the X-Shopify-Access-Token header.
type apiClient struct {
	http *http.Client
	// download fetches bulk outputs, which may take longer than any
	// request timeout; it bounds only the wait for response headers.
	download   *http.Client
	shop       string
	origin     string
	creds      *credentials
	retryDelay time.Duration
	now        func() time.Time
	wait       func(ctx context.Context, d time.Duration) error
	bucket     bucket
}

// bucket is the query cost state of the last response: the cost it asked
// for, the points available after it, the most there can be and the
// points restored per second.
type bucket struct {
	requested float64
	available float64
	maximum   float64
	restore   float64
	at        time.Time
}

func (s Source) apiClient(cfg Config) *apiClient {
	shop, _ := cfg.shop()
	c := &apiClient{
		http:       s.client,
		download:   s.client,
		shop:       shop,
		creds:      &credentials{token: cfg.AccessToken, clientID: cfg.ClientID, clientSecret: cfg.ClientSecret},
		retryDelay: s.retryDelay,
		now:        s.now,
		wait:       s.wait,
	}
	if s.baseURL != nil {
		c.origin = s.baseURL(shop)
	} else {
		c.origin = "https://" + shop + shopDomainSuffix
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 5 * time.Minute}
	}
	if c.download == nil {
		t := http.DefaultTransport.(*http.Transport).Clone()
		t.ResponseHeaderTimeout = 5 * time.Minute
		c.download = &http.Client{Transport: t}
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

func (c *apiClient) domain() string { return c.shop + shopDomainSuffix }

// gqlError is an entry of the errors array of a GraphQL response.
type gqlError struct {
	Message    string `json:"message"`
	Extensions struct {
		Code           string `json:"code"`
		RequiredAccess string `json:"requiredAccess"`
	} `json:"extensions"`
}

// queryError is a GraphQL response with errors. Data that came with them
// is never used.
type queryError struct {
	op     string
	errors []gqlError
}

func (e *queryError) Error() string {
	msgs := make([]string, len(e.errors))
	for i, g := range e.errors {
		msgs[i] = g.Message
		if g.Extensions.Code != "" {
			msgs[i] = g.Extensions.Code + ": " + g.Message
		}
	}
	return fmt.Sprintf("%s: %s", e.op, strings.Join(msgs, "; "))
}

func (e *queryError) has(code string) bool {
	for _, g := range e.errors {
		if g.Extensions.Code == code {
			return true
		}
	}
	return false
}

// accessError is an ACCESS_DENIED error: the app lacks a scope.
type accessError struct {
	op       string
	required []string
	message  string
}

func (e *accessError) Error() string {
	if len(e.required) == 0 {
		return fmt.Sprintf("%s: access denied: %s", e.op, e.message)
	}
	return fmt.Sprintf("%s: access denied, the app needs %s", e.op, strings.Join(e.required, ", "))
}

// httpError is a response with an error status.
type httpError struct {
	op      string
	status  int
	message string
}

func (e *httpError) Error() string {
	s := fmt.Sprintf("%s (HTTP %d)", e.op, e.status)
	if e.message != "" {
		s += ": " + e.message
	}
	return s
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// limited is a THROTTLED error or a 429: the request may be repeated after
// wait, or once the bucket has restored when costed. A THROTTLED error
// without a cost waits a doubling delay.
type limited struct {
	err    error
	wait   time.Duration
	costed bool
}

func (e limited) Error() string { return e.err.Error() }

// unauthorized is a 401: the access token was rejected.
type unauthorized struct{ *httpError }

type gqlResponse struct {
	Data       json.RawMessage `json:"data"`
	Errors     json.RawMessage `json:"errors"`
	Extensions struct {
		Cost *struct {
			RequestedQueryCost float64 `json:"requestedQueryCost"`
			ThrottleStatus     struct {
				MaximumAvailable   float64 `json:"maximumAvailable"`
				CurrentlyAvailable float64 `json:"currentlyAvailable"`
				RestoreRate        float64 `json:"restoreRate"`
			} `json:"throttleStatus"`
		} `json:"cost"`
		Search []searchInfo `json:"search"`
	} `json:"extensions"`
}

// query sends the GraphQL operation op and decodes its data into out. A
// THROTTLED error waits until the bucket holds the requested cost and a
// 429 its Retry-After, each up to limitRetries times; a 5xx, a transport
// error or an INTERNAL_SERVER_ERROR is repeated with a doubling delay up
// to retries times. A rejected token from the client credentials grant is
// fetched again once.
func (c *apiClient) query(ctx context.Context, op, doc string, vars map[string]any, out any) error {
	body, err := json.Marshal(struct {
		Query         string         `json:"query"`
		Variables     map[string]any `json:"variables,omitempty"`
		OperationName string         `json:"operationName"`
	}{doc, vars, op})
	if err != nil {
		return fmt.Errorf("%s: encode request: %w", op, err)
	}
	delay, throttleDelay := c.retryDelay, c.retryDelay
	var failures, throttles int
	refetched := false
	for {
		if err := c.pace(ctx); err != nil {
			return err
		}
		err := c.do(ctx, op, body, out)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var lim limited
		var unauth unauthorized
		var r retryable
		switch {
		case errors.As(err, &lim):
			if throttles == limitRetries {
				return fmt.Errorf("%w; gave up after %d waits", err, limitRetries)
			}
			throttles++
			if lim.wait == 0 && (!lim.costed || c.bucket.restore <= 0) {
				lim.wait = throttleDelay
				throttleDelay *= 2
			}
			if lim.wait > 0 {
				if err := c.wait(ctx, lim.wait); err != nil {
					return err
				}
			}
		case errors.As(err, &unauth):
			if c.creds.grant() && !refetched {
				refetched = true
				c.creds.reset()
				continue
			}
			if c.creds.grant() {
				return synclet.ConfigError(fmt.Errorf("access token of the client credentials grant was rejected (HTTP 401): %s", unauth.message))
			}
			return synclet.ConfigError(fmt.Errorf("access token is invalid or revoked (HTTP 401): %s", unauth.message))
		case errors.As(err, &r) && failures < retries:
			failures++
			if err := c.wait(ctx, delay); err != nil {
				return err
			}
			delay *= 2
		default:
			return err
		}
	}
}

// pace waits until the bucket, restoring since the last response, holds
// the cost the last request asked for.
func (c *apiClient) pace(ctx context.Context) error {
	b := c.bucket
	if b.restore <= 0 || b.requested <= 0 {
		return nil
	}
	avail := b.available + b.restore*c.now().Sub(b.at).Seconds()
	if b.maximum > 0 {
		avail = math.Min(avail, b.maximum)
	}
	if avail >= b.requested {
		return nil
	}
	ms := math.Ceil((b.requested - avail) * 1000 / b.restore)
	if err := c.wait(ctx, time.Duration(ms)*time.Millisecond); err != nil {
		return err
	}
	c.bucket.available, c.bucket.at = b.requested, c.now()
	return nil
}

func (c *apiClient) graphqlURL() string {
	return c.origin + "/admin/api/" + apiVersion + "/graphql.json"
}

func (c *apiClient) do(ctx context.Context, op string, body []byte, out any) (rerr error) {
	token, err := c.creds.get(ctx, c)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.graphqlURL(), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set(tokenHeader, token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{fmt.Errorf("%s: %w", op, err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{fmt.Errorf("%s: read body: %w", op, err)}
	}
	if resp.StatusCode != http.StatusOK {
		return c.statusError(op, resp, raw)
	}
	var gr gqlResponse
	if err := json.Unmarshal(raw, &gr); err != nil {
		return fmt.Errorf("%s: decode response: %w", op, err)
	}
	if cost := gr.Extensions.Cost; cost != nil {
		c.bucket = bucket{
			requested: cost.RequestedQueryCost,
			available: cost.ThrottleStatus.CurrentlyAvailable,
			maximum:   cost.ThrottleStatus.MaximumAvailable,
			restore:   cost.ThrottleStatus.RestoreRate,
			at:        c.now(),
		}
	}
	if len(gr.Errors) > 0 && string(gr.Errors) != "null" {
		err := c.graphqlError(op, gr.Errors)
		if lim, ok := err.(limited); ok {
			lim.costed = gr.Extensions.Cost != nil
			return lim
		}
		return err
	}
	if err := searchWarnings(op, gr.Extensions.Search); err != nil {
		return err
	}
	if len(gr.Data) == 0 || string(gr.Data) == "null" {
		return fmt.Errorf("%s: response without data", op)
	}
	if err := json.Unmarshal(gr.Data, out); err != nil {
		return fmt.Errorf("%s: decode data: %w", op, err)
	}
	return nil
}

// searchInfo is how Shopify parsed the search query of a connection.
type searchInfo struct {
	Path     []any  `json:"path"`
	Query    string `json:"query"`
	Warnings []struct {
		Field   string `json:"field"`
		Message string `json:"message"`
	} `json:"warnings"`
}

// searchWarnings fails a response whose search query Shopify did not fully
// apply, such as a filter on a field the connection cannot search: its
// result may be unfiltered or empty.
func searchWarnings(op string, infos []searchInfo) error {
	var msgs []string
	for _, si := range infos {
		for _, w := range si.Warnings {
			msgs = append(msgs, fmt.Sprintf("search query %q: %s: %s", si.Query, w.Field, w.Message))
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	return fmt.Errorf("%s: Shopify did not apply the filter: %s", op, strings.Join(msgs, "; "))
}

// graphqlError classifies the errors of a 200 response.
func (c *apiClient) graphqlError(op string, raw json.RawMessage) error {
	var list []gqlError
	if err := json.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("%s: %s", op, errorText(raw))
	}
	qe := &queryError{op: op, errors: list}
	switch {
	case qe.has("THROTTLED"):
		return limited{err: qe}
	case qe.has("ACCESS_DENIED"):
		ae := &accessError{op: op}
		for _, g := range list {
			if g.Extensions.Code != "ACCESS_DENIED" {
				continue
			}
			if g.Extensions.RequiredAccess != "" {
				ae.required = append(ae.required, g.Extensions.RequiredAccess)
			} else if ae.message == "" {
				ae.message = g.Message
			}
		}
		return synclet.ConfigError(ae)
	case qe.has("INTERNAL_SERVER_ERROR"):
		return retryable{qe}
	}
	return qe
}

// statusError maps an error status of the GraphQL endpoint.
func (c *apiClient) statusError(op string, resp *http.Response, raw []byte) error {
	he := &httpError{op: op, status: resp.StatusCode, message: errorText(raw)}
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return unauthorized{he}
	case resp.StatusCode == http.StatusPaymentRequired:
		return synclet.ConfigError(fmt.Errorf("shop %s is frozen (unpaid): %w", c.domain(), he))
	case resp.StatusCode == http.StatusLocked:
		return synclet.ConfigError(fmt.Errorf("shop %s is locked: %w", c.domain(), he))
	case resp.StatusCode == http.StatusNotFound:
		return synclet.ConfigError(fmt.Errorf("shop %s not found; use the shop's myshopify.com domain: %w", c.domain(), he))
	case resp.StatusCode == http.StatusForbidden:
		return synclet.ConfigError(he)
	case resp.StatusCode == http.StatusTooManyRequests:
		return limited{err: he, wait: retryAfter(resp.Header.Get("Retry-After"))}
	case resp.StatusCode >= 500:
		return retryable{he}
	}
	return he
}

// errorText is the message of an error body: its errors or error field,
// which Shopify sends as a string or an array, or the trimmed body.
func errorText(raw []byte) string {
	var body struct {
		Errors           json.RawMessage `json:"errors"`
		Error            string          `json:"error"`
		ErrorDescription string          `json:"error_description"`
	}
	if json.Unmarshal(raw, &body) == nil {
		var s string
		switch {
		case len(body.Errors) > 0 && json.Unmarshal(body.Errors, &s) == nil:
			return s
		case len(body.Errors) > 0:
			return string(body.Errors)
		case body.ErrorDescription != "":
			return body.Error + ": " + body.ErrorDescription
		case body.Error != "":
			return body.Error
		}
	}
	text := strings.TrimSpace(string(raw))
	if len(text) > 300 {
		text = text[:300] + "..."
	}
	return text
}

// retryAfter parses a Retry-After header given in seconds, which Shopify
// may send with a fraction.
func retryAfter(h string) time.Duration {
	f, err := strconv.ParseFloat(h, 64)
	if err != nil || f < 0 {
		return defaultRetryAfter
	}
	return time.Duration(f * float64(time.Second))
}
