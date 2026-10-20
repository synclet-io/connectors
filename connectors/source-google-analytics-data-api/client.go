package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	defaultBaseURL    = "https://analyticsdata.googleapis.com"
	apiVersion        = "v1beta"
	defaultRetryDelay = 2 * time.Second
	maxRetryDelay     = time.Minute
	// attempts is how many times a request is sent while it fails with a
	// retryable 429, a 5xx response or a transport error.
	attempts = 5
)

// apiClient sends authenticated requests to the Google Analytics Data API.
// The access token travels only in the Authorization header.
type apiClient struct {
	http       *http.Client
	root       string
	auth       *tokenSource
	retryDelay time.Duration
	jitter     func(time.Duration) time.Duration
	wait       func(ctx context.Context, d time.Duration) error
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
	return c, nil
}

// apiError is a failed Data API request. reasons are the reason values of
// the ErrorInfo details, such as SERVICE_DISABLED.
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

// dailyQuota reports whether the error is the exhausted daily token quota
// of a property, which no retry within the day can pass.
func (e *apiError) dailyQuota() bool {
	return e.status == http.StatusTooManyRequests && strings.Contains(strings.ToLower(e.message), "per day")
}

// errorBody is the error body of Google APIs.
type errorBody struct {
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Status  string `json:"status"`
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

// send sends a request for property prop and decodes the response into out.
// A request rejected with 401 gets a new access token and is sent once more;
// a retryable failure is sent again after a doubling, jittered delay, up to
// attempts times in all. A disabled API and a property the credentials
// cannot read become configuration errors. A PERMISSION_DENIED without an
// ErrorInfo reason is the Data API's answer for a property the credentials
// have no role on.
func (c *apiClient) send(ctx context.Context, method, path, prop string, body, out any) error {
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
			case ae.has("SERVICE_DISABLED"):
				project := ae.project()
				if project == "" {
					project = "(unknown)"
				}
				return synclet.ConfigError(fmt.Errorf("Google Analytics Data API is not enabled in Google Cloud project %s: enable analyticsdata.googleapis.com (%w)", project, ae)) //nolint:staticcheck // the message starts with the product name
			case ae.has("ACCESS_TOKEN_SCOPE_INSUFFICIENT"):
				return synclet.ConfigError(fmt.Errorf("the credentials lack the %s scope: authorize the refresh token with it (%w)", scope, ae))
			case ae.status == http.StatusForbidden && ae.state == "PERMISSION_DENIED" && len(ae.reasons) == 0, ae.status == http.StatusNotFound:
				return synclet.ConfigError(fmt.Errorf("no access to property %s: grant the service account or user Viewer on it (%w)", prop, ae))
			case ae.status == http.StatusForbidden:
				return synclet.ConfigError(fmt.Errorf("permission denied for property %s (%w)", prop, ae))
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
		if err := json.Unmarshal(raw, out); err != nil {
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
	case resp.StatusCode == http.StatusTooManyRequests, resp.StatusCode >= 500:
		return retryable{err: ae}
	}
	return ae
}

// metadata is the dimensions and metrics a property offers.
type metadata struct {
	Dimensions []struct {
		APIName string `json:"apiName"`
	} `json:"dimensions"`
	Metrics []struct {
		APIName string `json:"apiName"`
		Type    string `json:"type"`
	} `json:"metrics"`
}

func (c *apiClient) metadata(ctx context.Context, prop string) (metadata, error) {
	var m metadata
	err := c.send(ctx, http.MethodGet, "properties/"+prop+"/metadata", prop, nil, &m)
	return m, err
}

type nameObj struct {
	Name string `json:"name"`
}

type dateRange struct {
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
}

// reportRequest is the body of runReport.
type reportRequest struct {
	DateRanges          []dateRange `json:"dateRanges"`
	Dimensions          []nameObj   `json:"dimensions"`
	Metrics             []nameObj   `json:"metrics"`
	Limit               int         `json:"limit"`
	Offset              int         `json:"offset"`
	ReturnPropertyQuota bool        `json:"returnPropertyQuota"`
}

type value struct {
	Value string `json:"value"`
}

type quotaStatus struct {
	Consumed  int `json:"consumed"`
	Remaining int `json:"remaining"`
}

// reportResponse is one page of a runReport response. int64 fields come as
// JSON strings, which json.Number accepts as well as numbers.
type reportResponse struct {
	DimensionHeaders []nameObj `json:"dimensionHeaders"`
	MetricHeaders    []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"metricHeaders"`
	Rows []struct {
		DimensionValues []value `json:"dimensionValues"`
		MetricValues    []value `json:"metricValues"`
	} `json:"rows"`
	RowCount int `json:"rowCount"`
	Metadata struct {
		SamplingMetadatas []struct {
			SamplesReadCount  json.Number `json:"samplesReadCount"`
			SamplingSpaceSize json.Number `json:"samplingSpaceSize"`
		} `json:"samplingMetadatas"`
		DataLossFromOtherRow  bool `json:"dataLossFromOtherRow"`
		SubjectToThresholding bool `json:"subjectToThresholding"`
	} `json:"metadata"`
	PropertyQuota *struct {
		TokensPerDay  *quotaStatus `json:"tokensPerDay"`
		TokensPerHour *quotaStatus `json:"tokensPerHour"`
	} `json:"propertyQuota"`
}

func (c *apiClient) runReport(ctx context.Context, prop string, req reportRequest) (reportResponse, error) {
	var r reportResponse
	err := c.send(ctx, http.MethodPost, "properties/"+prop+":runReport", prop, req, &r)
	return r, err
}
