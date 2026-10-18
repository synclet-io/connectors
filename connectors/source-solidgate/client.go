package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"golang.org/x/time/rate"
)

const (
	defaultReportsURL       = "https://reports.solidgate.com/api/v1/"
	defaultSubscriptionsURL = "https://subscriptions.solidgate.com/api/v1/"
	defaultRetryDelay       = 2 * time.Second
	maxRetryDelay           = 60 * time.Second
	// maxAttempts is how many times a request is sent before a 429, 5xx or
	// transport error is returned.
	maxAttempts = 5
	// requestsPerSecond stays under the sandbox limit of 10 requests per
	// second per merchant account; the live limit is 25.
	requestsPerSecond = 10
	// codeAuthFailed and codeRateLimited are Solidgate error codes.
	codeAuthFailed  = "1.01"
	codeRateLimited = "5.07"
)

// signature returns the value of the signature header of a request with
// body: Base64 of the hex HMAC-SHA512 of public key + body + public key,
// keyed by the secret key.
func signature(publicKey, secretKey string, body []byte) string {
	mac := hmac.New(sha512.New, []byte(secretKey))
	mac.Write([]byte(publicKey))
	mac.Write(body)
	mac.Write([]byte(publicKey))
	return base64.StdEncoding.EncodeToString([]byte(hex.EncodeToString(mac.Sum(nil))))
}

// apiClient sends signed requests for one merchant account. The secret key
// never leaves the process; it only signs.
type apiClient struct {
	http             *http.Client
	reportsURL       string
	subscriptionsURL string
	account          Account
	retryDelay       time.Duration
	limiter          *rate.Limiter
}

func (s Source) apiClient(a Account) *apiClient {
	c := &apiClient{
		http: s.client, reportsURL: s.reportsURL, subscriptionsURL: s.subscriptionsURL,
		account: a, retryDelay: s.retryDelay, limiter: rate.NewLimiter(requestsPerSecond, requestsPerSecond),
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 2 * time.Minute}
	}
	if c.reportsURL == "" {
		c.reportsURL = defaultReportsURL
	}
	if c.subscriptionsURL == "" {
		c.subscriptionsURL = defaultSubscriptionsURL
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	if s.noRateLimit {
		c.limiter = rate.NewLimiter(rate.Inf, 0)
	}
	return c
}

// apiError is the error object Solidgate sends, with an error status or
// inside a 200 response.
type apiError struct {
	Error *struct {
		Code     json.RawMessage `json:"code"`
		Messages []string        `json:"messages"`
		Message  string          `json:"message"`
	} `json:"error"`
}

// parseAPIError returns the code and message of body's error object; ok is
// false when body is not JSON.
func parseAPIError(body []byte) (code, message string, isJSON bool) {
	var e apiError
	if err := json.Unmarshal(body, &e); err != nil {
		return "", "", false
	}
	if e.Error == nil {
		return "", "", true
	}
	code = strings.Trim(string(e.Error.Code), `"`)
	msgs := e.Error.Messages
	if e.Error.Message != "" {
		msgs = append(msgs, e.Error.Message)
	}
	return code, strings.Join(msgs, ", "), true
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// errInvalidKeys, errWAF and errRateLimited name the causes check reports.
var (
	errInvalidKeys = errors.New("invalid key pair: Solidgate rejected the public and secret key")
	errWAF         = errors.New("blocked by Solidgate WAF; check the API host")
	errRateLimited = errors.New("rate limited by Solidgate")
)

// postJSON sends body as JSON to path of base and decodes the response into
// out.
func (c *apiClient) postJSON(ctx context.Context, base, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := c.send(ctx, http.MethodPost, base+path, raw)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(resp, out); err != nil {
		return fmt.Errorf("POST %s: decode response: %w", path, err)
	}
	return nil
}

// getJSON sends a signed GET to path of base and decodes the response into
// out.
func (c *apiClient) getJSON(ctx context.Context, base, path string, out any) error {
	resp, err := c.send(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(resp, out); err != nil {
		return fmt.Errorf("GET %s: decode response: %w", path, err)
	}
	return nil
}

// send repeats a signed request on 429, 5xx and transport errors with a
// doubling delay capped at maxRetryDelay, maxAttempts times at most, and
// returns the body of its 2xx response.
func (c *apiClient) send(ctx context.Context, method, u string, body []byte) ([]byte, error) {
	delay := c.retryDelay
	for attempt := 1; ; attempt++ {
		resp, err := c.do(ctx, method, u, body)
		var r retryable
		if err == nil || !errors.As(err, &r) || attempt == maxAttempts || ctx.Err() != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return resp, err
		}
		if err := sleep(ctx, delay); err != nil {
			return nil, err
		}
		delay = min(delay*2, maxRetryDelay)
	}
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

// newRequest builds a request carrying the merchant and signature headers.
func (c *apiClient) newRequest(ctx context.Context, method, u string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("merchant", c.account.PublicKey)
	req.Header.Set("signature", signature(c.account.PublicKey, c.account.SecretKey, body))
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req, nil
}

// roundTrip waits for the rate limiter and sends req, reading the whole
// response body.
func (c *apiClient) roundTrip(client *http.Client, req *http.Request) (*http.Response, []byte, error) {
	if err := c.limiter.Wait(req.Context()); err != nil {
		return nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, retryable{fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, err)}
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, retryable{fmt.Errorf("%s %s: read body: %w", req.Method, req.URL.Path, err)}
	}
	return resp, body, nil
}

func (c *apiClient) do(ctx context.Context, method, u string, body []byte) ([]byte, error) {
	req, err := c.newRequest(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	resp, data, err := c.roundTrip(c.http, req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return data, errorInBody(req, data)
	}
	return nil, statusError(req, resp.StatusCode, data)
}

// errorInBody returns the error object Solidgate sends with HTTP 200 for
// some failures, such as failed authentication.
func errorInBody(req *http.Request, data []byte) error {
	code, msg, _ := parseAPIError(data)
	if code == "" && msg == "" {
		return nil
	}
	detail := fmt.Errorf("%s %s (HTTP 200, code %s): %s", req.Method, req.URL.Path, code, msg)
	switch code {
	case codeAuthFailed:
		return synclet.ConfigError(fmt.Errorf("%w (%w)", errInvalidKeys, detail))
	case codeRateLimited:
		return retryable{fmt.Errorf("%w (%w)", errRateLimited, detail)}
	}
	return detail
}

// statusError names the cause of a non-2xx response: 401 or a 403 with a
// JSON error comes from the key pair, a 403 with any other body from the
// WAF in front of the API, which blocks requests for a wrong host or path.
func statusError(req *http.Request, status int, data []byte) error {
	code, msg, isJSON := parseAPIError(data)
	detail := fmt.Sprintf("%s %s (HTTP %d", req.Method, req.URL.Path, status)
	if code != "" {
		detail += ", code " + code
	}
	detail += ")"
	if msg != "" {
		detail += ": " + msg
	}
	switch {
	case status == http.StatusTooManyRequests || code == codeRateLimited:
		return retryable{fmt.Errorf("%w (%s)", errRateLimited, detail)}
	case status == http.StatusUnauthorized || (status == http.StatusForbidden && isJSON) || code == codeAuthFailed:
		return synclet.ConfigError(fmt.Errorf("%w (%s)", errInvalidKeys, detail))
	case status == http.StatusForbidden:
		return synclet.ConfigError(fmt.Errorf("%w (%s)", errWAF, detail))
	case status >= 500:
		return retryable{errors.New(detail)}
	}
	return errors.New(detail)
}
