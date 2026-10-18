package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL    = "https://openexchangerates.org/api/"
	defaultRetryDelay = time.Second
	// retries is how many times a request is repeated after a 5xx response
	// or a transport error.
	retries = 3
)

// apiClient sends authenticated GET requests to the Open Exchange Rates API.
// The app_id travels only in the Authorization header, never in a URL.
type apiClient struct {
	http       *http.Client
	baseURL    string
	appID      string
	retryDelay time.Duration
}

func (s Source) apiClient(cfg Config) *apiClient {
	c := &apiClient{http: s.client, baseURL: s.baseURL, appID: cfg.AppID, retryDelay: s.retryDelay}
	if c.http == nil {
		c.http = &http.Client{Timeout: time.Minute}
	}
	if c.baseURL == "" {
		c.baseURL = defaultBaseURL
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	return c
}

// apiError is the body Open Exchange Rates sends with an error status.
type apiError struct {
	Status      int    `json:"status"`
	Message     string `json:"message"`
	Description string `json:"description"`
}

// configCauses names the cause of the statuses that come from the app_id or
// its plan; Open Exchange Rates answers 429 for a route the plan lacks, not
// for throttling, so none of them is retried.
var configCauses = map[int]string{
	http.StatusUnauthorized:    "invalid app_id",
	http.StatusTooManyRequests: "plan does not allow this route",
	http.StatusForbidden:       "access restricted",
}

// messageCauses names the cause of error codes that come from the
// configuration but share their status with other errors, such as 400.
var messageCauses = map[string]string{
	"invalid_base": "invalid base currency",
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// get decodes the JSON response of path into out, repeating the request on
// a 5xx response or a transport error with a doubling delay.
func (c *apiClient) get(ctx context.Context, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	delay := c.retryDelay
	for attempt := 0; ; attempt++ {
		err := c.do(ctx, u, out)
		var r retryable
		if err == nil || !errors.As(err, &r) || attempt == retries || ctx.Err() != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
}

func (c *apiClient) do(ctx context.Context, u string, out any) (rerr error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Token "+c.appID)
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
	var e apiError
	_ = json.Unmarshal(body, &e)
	detail := fmt.Sprintf("HTTP %d", resp.StatusCode)
	if e.Message != "" {
		detail += " " + e.Message
	}
	detail = "(" + detail + ")"
	if e.Description != "" {
		detail += ": " + e.Description
	}
	cause, ok := messageCauses[e.Message]
	if !ok {
		cause, ok = configCauses[resp.StatusCode]
	}
	if ok {
		return synclet.ConfigError(fmt.Errorf("%s %s", cause, detail))
	}
	err = fmt.Errorf("GET %s %s", req.URL.Path, detail)
	if resp.StatusCode >= 500 {
		return retryable{err}
	}
	return err
}
