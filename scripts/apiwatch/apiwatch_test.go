package apiwatch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var errOffline = errors.New("tests are offline: use FixtureTransport or httptest")

type offlineTransport struct{}

func (offlineTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, errOffline }

// TestMain makes any request that bypasses the fixtures fail, so the tests
// never reach the network.
func TestMain(m *testing.M) {
	http.DefaultTransport = offlineTransport{}
	if err := os.Unsetenv("GITHUB_TOKEN"); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

var recordedNow = time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)

func fixtureClient() *http.Client {
	return &http.Client{Transport: FixtureTransport{Dir: "testdata"}}
}

// inventory mirrors the polled api_versions of connectors/*/meta.json as of
// the recording.
var inventory = []Target{
	{Connector: "source-facebook-marketing", API: "Meta Marketing API", Pinned: "v26.0", Checker: "meta-graph", Params: map[string]string{"table": "Marketing API"}},
	{Connector: "source-google-ads", API: "Google Ads API", Pinned: "v26", Checker: "google-ads"},
	{Connector: "source-linkedin-ads", API: "LinkedIn Marketing API", Pinned: "202609", Checker: "linkedin"},
	{Connector: "source-shopify", API: "Shopify Admin API", Pinned: "2026-10", Checker: "shopify"},
	{Connector: "source-tiktok-marketing", API: "TikTok Business API", Pinned: "v1.3", Checker: "tiktok-probe"},
	{Connector: "source-stripe", API: "Stripe API", Pinned: "2025-08-27.basil", Checker: "stripe"},
	{Connector: "source-klaviyo", API: "Klaviyo API", Pinned: "2025-07-15", Checker: "klaviyo"},
	{Connector: "source-intercom", API: "Intercom API", Pinned: "2.11", Checker: "intercom"},
	{Connector: "source-notion", API: "Notion API", Pinned: "2025-09-03", Checker: "notion"},
	{Connector: "source-github", API: "GitHub REST API", Pinned: "2022-11-28", Checker: "github"},
	{Connector: "source-salesforce", API: "Salesforce REST API", Pinned: "v62.0", Checker: "salesforce", Params: map[string]string{"max_behind": "6"}},
	{Connector: "source-google-analytics-data-api", API: "Google Analytics Data API", Pinned: "v1beta", Checker: "google-discovery", Params: map[string]string{"api": "analyticsdata"}},
	{Connector: "source-google-search-console", API: "Search Console API", Pinned: "webmasters/v3", Checker: "google-discovery", Params: map[string]string{"successor": "searchconsole"}},
	{Connector: "source-google-sheets", API: "Google Sheets API", Pinned: "v4", Checker: "google-discovery", Params: map[string]string{"api": "sheets"}},
	{Connector: "source-google-sheets", API: "Google Drive API", Pinned: "v3", Checker: "google-discovery", Params: map[string]string{"api": "drive"}},
	{Connector: "source-hubspot", API: "HubSpot CRM API", Pinned: "v3", Checker: None},
}

func TestRecordedReport(t *testing.T) {
	want := map[string]struct {
		status Status
		latest string
		sunset string
	}{
		"Meta Marketing API":        {StatusOK, "v26.0", ""},
		"Google Ads API":            {StatusOK, "v26", "2027-11-01"},
		"LinkedIn Marketing API":    {StatusNewVersion, "202610", "2027-09-01"},
		"Shopify Admin API":         {StatusOK, "2026-10", "2027-10-16"},
		"TikTok Business API":       {StatusOK, "v1.3", ""},
		"Stripe API":                {StatusNewVersion, "2026-09-30.endive", ""},
		"Klaviyo API":               {StatusNewVersion, "2026-10-15", "2027-07-15"},
		"Intercom API":              {StatusNewVersion, "2.17", ""},
		"Notion API":                {StatusNewVersion, "2026-03-11", ""},
		"GitHub REST API":           {StatusNewVersion, "2026-03-10", "2028-03-10"},
		"Salesforce REST API":       {StatusNewVersion, "v68.0", ""},
		"Google Analytics Data API": {StatusOK, "v1beta", ""},
		"Search Console API":        {StatusNewVersion, "searchconsole/v1", ""},
		"Google Sheets API":         {StatusOK, "v4", ""},
		"Google Drive API":          {StatusOK, "v3", ""},
		"HubSpot CRM API":           {StatusUnchecked, "", ""},
	}

	report := Run(context.Background(), fixtureClient(), inventory, recordedNow)

	if report.Checked != "2026-10-21" {
		t.Errorf("Checked = %s", report.Checked)
	}

	if len(report.Entries) != len(inventory) {
		t.Fatalf("%d entries, want %d", len(report.Entries), len(inventory))
	}

	for _, e := range report.Entries {
		w := want[e.API]
		if e.Status != w.status || e.Latest != w.latest || e.Sunset != w.sunset {
			t.Errorf("%s: status %s latest %q sunset %q (error %q), want %s %q %q",
				e.API, e.Status, e.Latest, e.Sunset, e.Error, w.status, w.latest, w.sunset)
		}
	}
}

func TestSalesforceWithinMaxBehindIsOK(t *testing.T) {
	report := Run(context.Background(), fixtureClient(), []Target{
		{API: "a", Pinned: "v63.0", Checker: "salesforce", Params: map[string]string{"max_behind": "6"}},
		{API: "b", Pinned: "v20.0", Checker: "salesforce"},
	}, recordedNow)

	if e := report.Entries[0]; e.Status != StatusOK || e.Note != "5 versions behind" {
		t.Errorf("v63.0: %+v, want ok, 5 versions behind", e)
	}

	if e := report.Entries[1]; e.Status != StatusSunsetPassed {
		t.Errorf("unlisted v20.0: %+v, want sunset_passed", e)
	}
}

func TestPinnedVersionGoneUpstream(t *testing.T) {
	report := Run(context.Background(), fixtureClient(), []Target{
		{API: "github", Pinned: "2021-01-01", Checker: "github"},
		{API: "tiktok", Pinned: "v1.4", Checker: "tiktok-probe"},
	}, recordedNow)

	for _, e := range report.Entries {
		if e.Status != StatusSunsetPassed {
			t.Errorf("%s: %+v, want sunset_passed", e.API, e)
		}
	}
}

// A page that changed shape must fail the checker, never pass as ok.
func TestShapeChangedPagesFail(t *testing.T) {
	client := &http.Client{Transport: staticTransport("<!doctype html><html><body><h1>This page has moved</h1></body></html>")}

	for id, c := range Checkers() {
		if _, err := c.Check(context.Background(), client, pinnedFor(id), paramsFor(id)); err == nil {
			t.Errorf("%s: no error on a page without versions", id)
		}
	}
}

func TestScrapeWithoutPinnedRowFails(t *testing.T) {
	for _, tc := range []struct{ checker, pinned string }{
		{"meta-graph", "v99.0"},
		{"google-ads", "v99"},
	} {
		_, err := Checkers()[tc.checker].Check(context.Background(), fixtureClient(), tc.pinned, nil)
		if err == nil || !strings.Contains(err.Error(), "no") {
			t.Errorf("%s pinned %s: err = %v, want a missing row error", tc.checker, tc.pinned, err)
		}
	}
}

func TestGoogleAdsTableLagging(t *testing.T) {
	client := &http.Client{Transport: overlay{
		base:  FixtureTransport{Dir: "testdata"},
		extra: map[string]string{"https://googleads.googleapis.com/$discovery/rest?version=v27": `{"version":"v27"}`},
	}}

	r, err := googleAds{}.Check(context.Background(), client, "v26", nil)
	if err != nil {
		t.Fatal(err)
	}

	if r.Latest != "v27" || !strings.Contains(r.Note, "v27 Discovery document") {
		t.Errorf("Result = %+v, want v27 from the Discovery probe", r)
	}
}

func TestStatusThresholds(t *testing.T) {
	today := day(recordedNow)

	for _, tc := range []struct {
		days  int
		newer bool
		want  Status
	}{
		{-1, false, StatusSunsetPassed},
		{0, false, StatusSunset30},
		{30, true, StatusSunset30},
		{31, false, StatusSunset90},
		{90, false, StatusSunset90},
		{91, true, StatusNewVersion},
		{91, false, StatusOK},
	} {
		sunset := today.AddDate(0, 0, tc.days)
		if got := status(tc.newer, &sunset, today); got != tc.want {
			t.Errorf("status(%v, +%d days) = %s, want %s", tc.newer, tc.days, got, tc.want)
		}
	}
}

func TestCompareVersions(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"v26.0", "v27.0", -1},
		{"v9.0", "v10.0", -1},
		{"v26", "v26", 0},
		{"2026-07", "2026-10", -1},
		{"202609", "202610", -1},
		{"2025-08-27.basil", "2026-09-30.endive", -1},
		{"2.9", "2.11", -1},
		{"v62.0", "v68.0", -1},
		{"v1beta", "v1", -1},
		{"v1alpha", "v1beta", -1},
		{"v2", "v1beta", 1},
	} {
		if got := sign(compareVersions(tc.a, tc.b)); got != tc.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", tc.a, tc.b, got, tc.want)
		}

		if got := sign(compareVersions(tc.b, tc.a)); got != -tc.want {
			t.Errorf("compareVersions(%q, %q) = %d, want %d", tc.b, tc.a, got, -tc.want)
		}
	}
}

func TestOfflineGuard(t *testing.T) {
	report := Run(context.Background(), &http.Client{}, inventory[:1], recordedNow)

	if e := report.Entries[0]; e.Status != StatusCheckerError || !strings.Contains(e.Error, errOffline.Error()) {
		t.Fatalf("entry = %+v, want the offline transport to refuse the request", e)
	}
}

func TestMarkdownListsEveryEntry(t *testing.T) {
	md := Markdown(Run(context.Background(), fixtureClient(), inventory, recordedNow))

	for _, target := range inventory {
		if !strings.Contains(md, "["+target.API+"]") {
			t.Errorf("summary misses %s", target.API)
		}
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	}

	return 0
}

func pinnedFor(id string) string {
	for _, t := range inventory {
		if t.Checker == id {
			return t.Pinned
		}
	}

	return ""
}

func paramsFor(id string) map[string]string {
	for _, t := range inventory {
		if t.Checker == id {
			return t.Params
		}
	}

	return nil
}

type staticTransport string

func (s staticTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{}, Request: req,
		Body: io.NopCloser(strings.NewReader(string(s))),
	}, nil
}

type overlay struct {
	base  http.RoundTripper
	extra map[string]string
}

func (o overlay) RoundTrip(req *http.Request) (*http.Response, error) {
	if body, ok := o.extra[req.URL.String()]; ok {
		return staticTransport(body).RoundTrip(req)
	}

	return o.base.RoundTrip(req)
}
