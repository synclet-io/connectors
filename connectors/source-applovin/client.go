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
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL    = "https://r.applovin.com/"
	defaultRetryDelay = 2 * time.Second
	// attempts is how many times a request is sent before a 429, a 5xx or a
	// transport error fails it.
	attempts = 5
	// pageSize is the limit of every report request.
	pageSize = 10000
	// maxPages stops a day whose pages never come back short.
	maxPages = 100
	// maxErrorBody is how much of an error response goes into the error.
	maxErrorBody = 300
	redacted     = "REDACTED"
)

// apiClient requests AppLovin reports. AppLovin takes the Report Key only as
// the api_key query parameter, so request URLs are never logged and every
// error text has the key replaced by REDACTED.
type apiClient struct {
	http       *http.Client
	baseURL    string
	key        string
	retryDelay time.Duration
}

func (s Source) apiClient(cfg Config) *apiClient {
	c := &apiClient{http: s.client, baseURL: s.baseURL, key: cfg.APIKey, retryDelay: s.retryDelay}
	if c.http == nil {
		c.http = &http.Client{Timeout: 5 * time.Minute}
	}
	if c.baseURL == "" {
		c.baseURL = defaultBaseURL
	}
	if c.retryDelay == 0 {
		c.retryDelay = defaultRetryDelay
	}
	return c
}

// errInvalidKey is the cause of a 401 response.
var errInvalidKey = errors.New("invalid report key")

// unavailableError is the cause of a 403 response: the key may not read the
// report type.
type unavailableError struct{ typeName string }

func (e unavailableError) Error() string {
	return fmt.Sprintf("report type %s is not available to the report key", e.typeName)
}

// retryable marks a failure that may pass when the request is repeated.
type retryable struct{ err error }

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// redactedError carries the text of err with the report key replaced.
type redactedError struct {
	msg string
	err error
}

func (e redactedError) Error() string { return e.msg }
func (e redactedError) Unwrap() error { return e.err }

// redactText replaces the key, plain or query-escaped, in s.
func (c *apiClient) redactText(s string) string {
	if c.key == "" {
		return s
	}
	return strings.NewReplacer(c.key, redacted, url.QueryEscape(c.key), redacted).Replace(s)
}

func (c *apiClient) redact(err error) error {
	if err == nil {
		return err
	}
	msg := c.redactText(err.Error())
	if msg == err.Error() {
		return err
	}
	return redactedError{msg: msg, err: err}
}

// reportPage is the JSON body of a report response.
type reportPage struct {
	Code    int              `json:"code"`
	Results []map[string]any `json:"results"`
}

// report returns every row of d for one day, page by page until a page comes
// back shorter than pageSize.
func (c *apiClient) report(ctx context.Context, d streamDef, day string, columns []string) ([]map[string]any, error) {
	var rows []map[string]any
	for page := 0; ; page++ {
		if page == maxPages {
			return nil, fmt.Errorf("%s report %s: still paging after %d pages", d.typeName, day, maxPages)
		}
		results, err := c.page(ctx, d, day, columns, pageSize, page*pageSize)
		if err != nil {
			return nil, err
		}
		rows = append(rows, results...)
		if len(results) < pageSize {
			return rows, nil
		}
	}
}

// page requests one page of d for one day, repeating the request on a 429, a
// 5xx or a transport error with a doubling delay.
func (c *apiClient) page(ctx context.Context, d streamDef, day string, columns []string, limit, offset int) ([]map[string]any, error) {
	q := url.Values{}
	q.Set("api_key", c.key)
	if d.reportType != "" {
		q.Set("report_type", d.reportType)
	}
	q.Set("start", day)
	q.Set("end", day)
	q.Set("columns", strings.Join(columns, ","))
	q.Set("format", "json")
	q.Set("limit", strconv.Itoa(limit))
	q.Set("offset", strconv.Itoa(offset))
	u := c.baseURL + d.path + "?" + q.Encode()
	delay := c.retryDelay
	for attempt := 1; ; attempt++ {
		results, err := c.do(ctx, d, u)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var r retryable
		if err == nil || !errors.As(err, &r) || attempt == attempts {
			return results, c.redact(err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
}

func (c *apiClient) do(ctx context.Context, d streamDef, u string) (_ []map[string]any, rerr error) {
	where := fmt.Sprintf("GET /%s (%s report)", d.path, d.typeName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", where, err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, retryable{fmt.Errorf("%s: %w", where, err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, retryable{fmt.Errorf("%s: read body: %w", where, err)}
	}
	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusUnauthorized:
		return nil, synclet.ConfigError(fmt.Errorf("%w (HTTP 401)%s", errInvalidKey, c.bodyText(body)))
	case resp.StatusCode == http.StatusForbidden:
		return nil, synclet.ConfigError(fmt.Errorf("%w (HTTP 403)%s", unavailableError{d.typeName}, c.bodyText(body)))
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return nil, retryable{fmt.Errorf("%s: HTTP %d%s", where, resp.StatusCode, c.bodyText(body))}
	default:
		return nil, fmt.Errorf("%s: HTTP %d%s", where, resp.StatusCode, c.bodyText(body))
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var p reportPage
	if err := dec.Decode(&p); err != nil {
		return nil, fmt.Errorf("%s: decode response: %w", where, err)
	}
	if p.Code != http.StatusOK {
		return nil, fmt.Errorf("%s: response code %d%s", where, p.Code, c.bodyText(body))
	}
	return p.Results, nil
}

// bodyText returns ": <body>" for a non-empty error body, with the key
// replaced before the body is cut to maxErrorBody.
func (c *apiClient) bodyText(body []byte) string {
	s := strings.TrimSpace(c.redactText(string(body)))
	if s == "" {
		return ""
	}
	if len(s) > maxErrorBody {
		s = s[:maxErrorBody] + "..."
	}
	return ": " + s
}
