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
	"os"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultRetryDelay = 2 * time.Second
	// attempts is how many times a request is sent before a 429, a 5xx or a
	// transport error fails it.
	attempts = 6
	// maxRetryAfter caps the wait a Retry-After header asks for.
	maxRetryAfter = 10 * time.Minute
	// maxErrorBody is how much of an error response goes into the error.
	maxErrorBody = 300
	// maxExportSize bounds an export archive on disk; the Export API rejects
	// exports over 4 GB with a 400.
	maxExportSize int64 = 8 << 30
)

// apiClient sends Amplitude API requests with the project's API key and
// secret key as HTTP basic credentials.
type apiClient struct {
	http       *http.Client
	baseURL    string
	apiKey     string
	secretKey  string
	retryDelay time.Duration
	// maxDownload is the most bytes download writes to disk.
	maxDownload int64
}

func (s Source) apiClient(cfg Config) *apiClient {
	c := &apiClient{http: s.client, baseURL: s.baseURL, apiKey: cfg.APIKey, secretKey: cfg.SecretKey, retryDelay: s.retryDelay, maxDownload: maxExportSize}
	if c.http == nil {
		// An export archive of a large window takes long to download, so the
		// timeout bounds a whole request generously.
		c.http = &http.Client{Timeout: time.Hour}
	}
	if c.baseURL == "" {
		c.baseURL = regionBaseURLs[cfg.region()]
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	return c
}

// statusError is the cause of a response whose status is not retried.
type statusError struct {
	status int
	where  string
	body   string
}

func (e statusError) Error() string {
	return fmt.Sprintf("%s: HTTP %d%s", e.where, e.status, e.body)
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct {
	err   error
	after time.Duration
}

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

func hasStatus(err error, status int) bool {
	var se statusError
	return errors.As(err, &se) && se.status == status
}

// getJSON decodes the JSON body of GET <path>?<query> into out.
func (c *apiClient) getJSON(ctx context.Context, path string, query url.Values, out any) error {
	return c.retry(ctx, func() error {
		resp, err := c.send(ctx, path, query)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return retryable{err: fmt.Errorf("GET /%s: read body: %w", path, err)}
		}
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("GET /%s: decode response: %w", path, err)
		}
		return nil
	})
}

// download writes the body of GET <path>?<query> into a new temporary file
// and returns it positioned at its start. The caller closes and removes it.
func (c *apiClient) download(ctx context.Context, path string, query url.Values) (*os.File, error) {
	var f *os.File
	err := c.retry(ctx, func() error {
		resp, err := c.send(ctx, path, query)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		tmp, err := os.CreateTemp("", "amplitude-export-*.zip")
		if err != nil {
			return err
		}
		n, err := io.Copy(tmp, io.LimitReader(resp.Body, c.maxDownload+1))
		if err != nil {
			discard(tmp)
			return retryable{err: fmt.Errorf("GET /%s: read body: %w", path, err)}
		}
		if n > c.maxDownload {
			discard(tmp)
			return fmt.Errorf("GET /%s: response is larger than %d bytes", path, c.maxDownload)
		}
		if _, err := tmp.Seek(0, io.SeekStart); err != nil {
			discard(tmp)
			return err
		}
		f = tmp
		return nil
	})
	return f, err
}

// discard closes and removes a temporary file.
func discard(f *os.File) {
	_ = f.Close()
	_ = os.Remove(f.Name())
}

// retry runs do until it succeeds, fails with an error that is not
// retryable, or has run attempts times, waiting a doubling delay or the
// server's Retry-After between runs.
func (c *apiClient) retry(ctx context.Context, do func() error) error {
	delay := c.retryDelay
	for attempt := 1; ; attempt++ {
		err := do()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var r retryable
		if !errors.As(err, &r) || attempt == attempts {
			return err
		}
		wait := delay
		if r.after > 0 {
			wait = r.after
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		delay *= 2
	}
}

// send sends one GET request and returns a 200 response. Other statuses
// close the body and return an error: retryable for a 429 or a 5xx, a config
// error for a 401 or a 403, a statusError otherwise.
func (c *apiClient) send(ctx context.Context, path string, query url.Values) (*http.Response, error) {
	where := "GET /" + path
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", where, err)
	}
	req.SetBasicAuth(c.apiKey, c.secretKey)
	resp, err := c.http.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, retryable{err: fmt.Errorf("%s: %w", where, err)}
	}
	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
	se := statusError{status: resp.StatusCode, where: where, body: bodyText(body)}
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		return nil, synclet.ConfigError(fmt.Errorf("invalid api_key or secret_key, or a data_region the project is not in (HTTP 401)%s", se.body))
	case resp.StatusCode == http.StatusForbidden:
		return nil, synclet.ConfigError(fmt.Errorf("%s: the API key may not read this endpoint (HTTP 403)%s", where, se.body))
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return nil, retryable{err: se, after: retryAfter(resp.Header.Get("Retry-After"))}
	}
	return nil, se
}

// retryAfter returns the wait of a Retry-After header in seconds, capped at
// maxRetryAfter; zero when there is none.
func retryAfter(h string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(h))
	if err != nil || n <= 0 {
		return 0
	}
	return min(time.Duration(n)*time.Second, maxRetryAfter)
}

// bodyText returns ": <body>" for a non-empty error body cut to
// maxErrorBody.
func bodyText(body []byte) string {
	s := strings.TrimSpace(string(body))
	if s == "" {
		return ""
	}
	if len(s) > maxErrorBody {
		s = s[:maxErrorBody] + "..."
	}
	return ": " + s
}
