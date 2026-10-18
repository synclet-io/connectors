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
	"strconv"
	"strings"
	"sync"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultSheetsURL  = "https://sheets.googleapis.com"
	defaultDriveURL   = "https://www.googleapis.com"
	defaultRetryDelay = 2 * time.Second
	maxRetryDelay     = time.Minute
	// attempts is how many times a request is sent while it fails with a
	// rate limit, a 5xx response or a transport error.
	attempts = 5
)

// api is one of the two Google APIs the connector calls.
type api struct {
	name    string
	service string
	scope   string
}

var (
	sheetsAPI = api{name: "Google Sheets API", service: "sheets.googleapis.com", scope: sheetsScope}
	driveAPI  = api{name: "Google Drive API", service: "drive.googleapis.com", scope: driveScope}
)

// apiClient sends authenticated requests to the Sheets and Drive APIs. The
// access token travels only in the Authorization header.
type apiClient struct {
	http       *http.Client
	sheetsRoot string
	driveRoot  string
	auth       *tokenSource
	retryDelay time.Duration
	jitter     func(time.Duration) time.Duration
	wait       func(ctx context.Context, d time.Duration) error
	pace       *pacer
	rpm        int
}

func (s Source) apiClient(ctx context.Context, cfg Config) (*apiClient, error) {
	hc := s.client
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Minute}
	}
	auth, err := newTokenSource(ctx, hc, s.tokenURL, cfg.Credentials)
	if err != nil {
		return nil, err
	}
	rpm := cfg.requestsPerMinute()
	c := &apiClient{
		http:       hc,
		sheetsRoot: strings.TrimRight(cmpOr(s.sheetsURL, defaultSheetsURL), "/"),
		driveRoot:  strings.TrimRight(cmpOr(s.driveURL, defaultDriveURL), "/"),
		auth:       auth,
		retryDelay: s.retryDelay,
		jitter:     s.jitter,
		wait:       s.wait,
		pace:       &pacer{interval: time.Minute / time.Duration(rpm), now: s.clock, wait: s.paceWait},
		rpm:        rpm,
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

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}
	return b
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

// apiError is a failed API request. reasons are the reason values of the
// ErrorInfo details and of the v3 errors list, such as SERVICE_DISABLED or
// rateLimitExceeded.
type apiError struct {
	path     string
	status   int
	state    string
	reasons  []string
	consumer string
	quota    string
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

// rateLimited reports whether the error is a per-minute rate or quota limit,
// which Google answers with 429 or with 403 and which passes after a wait.
func (e *apiError) rateLimited() bool {
	return e.status == http.StatusTooManyRequests || e.state == "RESOURCE_EXHAUSTED" ||
		e.has("rateLimitExceeded") || e.has("userRateLimitExceeded") || e.has("RATE_LIMIT_EXCEEDED")
}

func (e *apiError) scopeInsufficient() bool {
	return e.has("ACCESS_TOKEN_SCOPE_INSUFFICIENT") || e.has("insufficientPermissions") ||
		strings.Contains(e.message, "insufficient authentication scopes")
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
		if q := d.Metadata["quota_limit"]; q != "" && ae.quota == "" {
			ae.quota = q
			if v := d.Metadata["quota_limit_value"]; v != "" {
				ae.quota += " (" + v + ")"
			}
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
// names, or "(unknown)".
func (e *apiError) project() string {
	if p, ok := strings.CutPrefix(e.consumer, "projects/"); ok {
		return p
	}
	if m := projectInMessage.FindStringSubmatch(e.message); m != nil {
		return m[1]
	}
	return "(unknown)"
}

// retryable marks a failure that may pass when the request is repeated;
// after is the wait the server asked for with Retry-After.
type retryable struct {
	err   error
	after time.Duration
}

func (e retryable) Error() string { return e.err.Error() }
func (e retryable) Unwrap() error { return e.err }

// send gets root+path of a and decodes the response into out. A request
// rejected with 401 gets a new access token and is sent once more; a
// retryable failure is sent again after Retry-After or a doubling, jittered
// delay, up to attempts times in all. For the Sheets API, spreadsheet is
// the ID the request reads, named by the errors of a spreadsheet the
// credentials cannot read.
func (c *apiClient) send(ctx context.Context, a api, spreadsheet, path string, out any) error {
	root := c.sheetsRoot
	if a == driveAPI {
		root = c.driveRoot
	}
	delay := c.retryDelay
	refreshed := false
	for attempt := 1; ; attempt++ {
		err := c.do(ctx, root, path, out)
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
			case ae.has("SERVICE_DISABLED") || ae.has("accessNotConfigured"):
				return synclet.ConfigError(fmt.Errorf("%s is disabled in project %s: enable %s in that Google Cloud project (%w)", a.name, ae.project(), a.service, ae))
			case ae.scopeInsufficient():
				return synclet.ConfigError(fmt.Errorf("the credentials lack the %s scope: authorize them with it (%w)", a.scope, ae))
			case ae.rateLimited():
			case a == sheetsAPI && ae.status == http.StatusForbidden:
				return synclet.ConfigError(fmt.Errorf("spreadsheet %s is not shared with %s: share it with that account (%w)", spreadsheet, c.auth.account, ae))
			case a == sheetsAPI && ae.status == http.StatusNotFound:
				return synclet.ConfigError(fmt.Errorf("spreadsheet %s not found: check the spreadsheet ID (%w)", spreadsheet, ae))
			}
		}
		var r retryable
		if err == nil || !errors.As(err, &r) {
			return err
		}
		if attempt == attempts {
			if ae != nil && ae.rateLimited() {
				quota := ae.quota
				if quota == "" {
					quota = "of requests per minute"
				}
				return fmt.Errorf("%s quota %s is still exceeded after %d attempts: lower requests_per_minute (now %d) or raise the quota in Google Cloud (%w)", a.name, quota, attempts, c.rpm, ae)
			}
			return err
		}
		d := c.jitter(delay)
		if r.after > 0 {
			d = min(r.after, maxRetryDelay)
		}
		if err := c.wait(ctx, d); err != nil {
			return err
		}
		delay = min(delay*2, maxRetryDelay)
	}
}

func (c *apiClient) do(ctx context.Context, root, path string, out any) (rerr error) {
	token, err := c.auth.get()
	if err != nil {
		return err
	}
	if err := c.pace.next(ctx); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, root+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return retryable{err: fmt.Errorf("%s: %w", stripQuery(path), err)}
	}
	defer func() {
		if err := resp.Body.Close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return retryable{err: fmt.Errorf("%s: read body: %w", stripQuery(path), err)}
	}
	if resp.StatusCode == http.StatusOK {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(out); err != nil {
			return fmt.Errorf("%s: decode response: %w", stripQuery(path), err)
		}
		return nil
	}
	ae := newAPIError(stripQuery(path), resp.StatusCode, raw)
	switch {
	case resp.StatusCode == http.StatusUnauthorized:
		c.auth.invalidate()
		return ae
	case resp.StatusCode >= 500, ae.rateLimited():
		return retryable{err: ae, after: retryAfter(resp.Header.Get("Retry-After"))}
	}
	return ae
}

func stripQuery(path string) string {
	p, _, _ := strings.Cut(path, "?")
	return p
}

// retryAfter reads a Retry-After header in seconds; anything else is zero.
func retryAfter(h string) time.Duration {
	n, err := strconv.Atoi(strings.TrimSpace(h))
	if err != nil || n <= 0 {
		return 0
	}
	return time.Duration(n) * time.Second
}

// sheetProperties are the properties of one sheet of a spreadsheet.
type sheetProperties struct {
	SheetID        int64  `json:"sheetId"`
	Title          string `json:"title"`
	SheetType      string `json:"sheetType"`
	GridProperties *struct {
		RowCount    int `json:"rowCount"`
		ColumnCount int `json:"columnCount"`
	} `json:"gridProperties"`
}

// rowCount is the number of rows of a grid sheet, empty ones included.
func (p sheetProperties) rowCount() int {
	if p.GridProperties == nil {
		return 0
	}
	return p.GridProperties.RowCount
}

// readable reports whether the sheet is a grid sheet with rows.
func (p sheetProperties) readable() bool {
	return p.SheetType == "GRID" && p.rowCount() > 0
}

func spreadsheetPath(id string) string { return "/v4/spreadsheets/" + url.PathEscape(id) }

func (c *apiClient) sheets(ctx context.Context, id string) ([]sheetProperties, error) {
	var r struct {
		Sheets []struct {
			Properties sheetProperties `json:"properties"`
		} `json:"sheets"`
	}
	if err := c.send(ctx, sheetsAPI, id, spreadsheetPath(id)+"?fields=sheets.properties", &r); err != nil {
		return nil, err
	}
	out := make([]sheetProperties, 0, len(r.Sheets))
	for _, s := range r.Sheets {
		out = append(out, s.Properties)
	}
	return out, nil
}

// a1Range is the A1 notation of rows first to last of a sheet.
func a1Range(title string, first, last int) string {
	return "'" + strings.ReplaceAll(title, "'", "''") + "'!" + strconv.Itoa(first) + ":" + strconv.Itoa(last)
}

// values reads ranges of spreadsheet id in one batchGet, row by row, and
// returns the rows of each range in order. formatted reads the values as
// they are displayed; otherwise numbers and booleans come unformatted and
// dates and times as formatted strings.
func (c *apiClient) values(ctx context.Context, id string, ranges []string, formatted bool) ([][][]any, error) {
	q := url.Values{"ranges": ranges, "majorDimension": {"ROWS"}}
	if formatted {
		q.Set("valueRenderOption", "FORMATTED_VALUE")
	} else {
		q.Set("valueRenderOption", "UNFORMATTED_VALUE")
		q.Set("dateTimeRenderOption", "FORMATTED_STRING")
	}
	var r struct {
		ValueRanges []struct {
			Values [][]any `json:"values"`
		} `json:"valueRanges"`
	}
	if err := c.send(ctx, sheetsAPI, id, spreadsheetPath(id)+"/values:batchGet?"+q.Encode(), &r); err != nil {
		return nil, err
	}
	if len(r.ValueRanges) != len(ranges) {
		return nil, fmt.Errorf("spreadsheet %s: batchGet answered %d ranges for %d requested", id, len(r.ValueRanges), len(ranges))
	}
	out := make([][][]any, len(ranges))
	for i, vr := range r.ValueRanges {
		out[i] = vr.Values
	}
	return out, nil
}

// modifiedTime reads the time a spreadsheet last changed from Drive.
func (c *apiClient) modifiedTime(ctx context.Context, id string) (time.Time, error) {
	var r struct {
		ModifiedTime string `json:"modifiedTime"`
	}
	path := "/drive/v3/files/" + url.PathEscape(id) + "?fields=modifiedTime&supportsAllDrives=true"
	if err := c.send(ctx, driveAPI, id, path, &r); err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, r.ModifiedTime)
	if err != nil {
		return time.Time{}, fmt.Errorf("Drive file %s: modifiedTime %q: %w", id, r.ModifiedTime, err) //nolint:staticcheck // the message starts with the product name
	}
	return t.UTC(), nil
}
