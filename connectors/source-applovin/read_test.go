package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runRead(t *testing.T, src Source, config string, cat *synclet.ConfiguredCatalog, state string) *testkit.Output {
	t.Helper()
	rawCat, err := json.Marshal(cat)
	require.NoError(t, err)
	req := synclet.Request{Command: synclet.CommandRead, Config: json.RawMessage(config), Catalog: rawCat}
	if state != "" {
		req.State = json.RawMessage(state)
	}
	out, err := testkit.RunSource(t.Context(), src, req)
	require.NoError(t, err)
	return out
}

// withProperties returns cat with the schema of its only stream reduced to
// the named properties.
func withProperties(t *testing.T, cat *synclet.ConfiguredCatalog, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(cat.Streams[0].Stream.JSONSchema, &schema))
	props := map[string]json.RawMessage{}
	for _, n := range names {
		p, ok := schema.Properties[n]
		require.True(t, ok, n)
		props[n] = p
	}
	raw, err := json.Marshal(map[string]any{"properties": props})
	require.NoError(t, err)
	cat.Streams[0].Stream.JSONSchema = raw
	return cat
}

var advertiserPK = "day,campaign_id_external,platform,device_type,country"

func TestReadRequestsSelectedColumns(t *testing.T) {
	tests := []struct {
		name    string
		stream  string
		props   []string
		columns string
	}{
		{
			name: "metric deselected", stream: streamAdvertiserReport,
			props:   []string{"day", "campaign_id_external", "country", "platform", "device_type", "campaign", "cost"},
			columns: "day,campaign,campaign_id_external,platform,device_type,country,cost",
		},
		{
			name: "primary key column deselected", stream: streamAdvertiserReport,
			props:   []string{"day", "campaign_id_external", "platform", "device_type", "impressions"},
			columns: advertiserPK + ",impressions",
		},
		{
			name: "day deselected", stream: streamAdvertiserReport,
			props:   []string{"campaign_id_external", "country", "platform", "device_type", "clicks"},
			columns: advertiserPK + ",clicks",
		},
		{
			name: "cohort column selected", stream: streamAdvertiserReport,
			props:   []string{"day", "campaign_id_external", "country", "platform", "device_type", "roas_7d", "ret_14d", "cpp_90d"},
			columns: advertiserPK + ",roas_7d,cpp_90d,ret_14d",
		},
		{
			name: "max network deselected", stream: streamMaxRevenueReport,
			props:   []string{"day", "package_name", "estimated_revenue"},
			columns: "day,package_name,platform,country,device_type,ad_format,max_ad_unit_id,network,estimated_revenue",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src := testSource(srv)
			cat := withProperties(t, configured(t, src, synclet.SyncModeFullRefresh, tt.stream), tt.props...)
			out := runRead(t, src, testConfig, cat, "")
			require.NoError(t, out.Err)
			hs := h.all()
			require.Len(t, hs, 5)
			for _, x := range hs {
				assert.Equal(t, tt.columns, x.query.Get("columns"), x.path)
			}
		})
	}
}

func TestReadRequestsAllColumnsWithoutSelection(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cat := configured(t, src, synclet.SyncModeFullRefresh, streamAdvertiserReport)
	cat.Streams[0].Stream.JSONSchema = json.RawMessage(`{}`)
	out := runRead(t, src, testConfig, cat, "")
	require.NoError(t, out.Err)
	var all []string
	for _, c := range advertiserColumns() {
		all = append(all, c.name)
	}
	assert.Equal(t, strings.Join(all, ","), h.all()[0].query.Get("columns"))
}

// recordingServer answers every report request with an empty page and
// records the requested days and columns.
type recordingServer struct {
	mu      sync.Mutex
	days    []string
	columns []string
}

func newRecordingServer(t *testing.T) (*httptest.Server, *recordingServer) {
	t.Helper()
	rs := &recordingServer{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rs.mu.Lock()
		rs.days = append(rs.days, r.URL.Query().Get("start"))
		rs.columns = append(rs.columns, r.URL.Query().Get("columns"))
		rs.mu.Unlock()
		_, _ = w.Write([]byte(`{"code":200,"count":0,"results":[]}`))
	}))
	t.Cleanup(srv.Close)
	return srv, rs
}

// days returns every day from from to to, both included.
func days(t *testing.T, from, to string) []string {
	t.Helper()
	d, err := time.Parse(dateLayout, from)
	require.NoError(t, err)
	var ds []string
	for s := from; s <= to; s = d.Format(dateLayout) {
		ds = append(ds, s)
		d = d.AddDate(0, 0, 1)
	}
	return ds
}

func TestReadResume(t *testing.T) {
	state := `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"advertiser_report","namespace":"applovin"},"stream_state":{"day":"2026-10-12"}}}]`
	tests := []struct {
		name   string
		config string
		state  string
		first  string
	}{
		{name: "no state starts at start_date", config: `{"api_key":"k","start_date":"2026-10-01"}`, first: "2026-10-01"},
		{name: "default lookback of 3 days", config: `{"api_key":"k","start_date":"2026-10-01"}`, state: state, first: "2026-10-09"},
		{name: "custom lookback", config: `{"api_key":"k","start_date":"2026-10-01","lookback_days":7}`, state: state, first: "2026-10-05"},
		{name: "zero lookback", config: `{"api_key":"k","start_date":"2026-10-01","lookback_days":0}`, state: state, first: "2026-10-12"},
		{name: "start_date after the resume day", config: `{"api_key":"k","start_date":"2026-10-11"}`, state: state, first: "2026-10-11"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, rs := newRecordingServer(t)
			src := testSource(srv)
			out := runRead(t, src, tt.config, configured(t, src, synclet.SyncModeIncremental, streamAdvertiserReport), tt.state)
			require.NoError(t, out.Err)
			assert.Equal(t, days(t, tt.first, "2026-10-18"), rs.days)
			states := out.States()
			require.NotEmpty(t, states)
			assert.JSONEq(t, `{"day":"2026-10-18"}`, string(states[len(states)-1].Stream.StreamState))
		})
	}
}

func TestReadWindow(t *testing.T) {
	tests := []struct {
		name   string
		config string
		state  string
	}{
		{name: "start_date before the window", config: `{"api_key":"k","start_date":"2026-01-01"}`},
		{
			name: "lookback before the window", config: `{"api_key":"k","start_date":"2026-01-01","lookback_days":30}`,
			state: `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"advertiser_report","namespace":"applovin"},"stream_state":{"day":"2026-09-10"}}}]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, rs := newRecordingServer(t)
			src := testSource(srv)
			out := runRead(t, src, tt.config, configured(t, src, synclet.SyncModeIncremental, streamAdvertiserReport), tt.state)
			require.NoError(t, out.Err)
			assert.Equal(t, days(t, "2026-09-04", "2026-10-18"), rs.days, "45 days, today included")
			assert.Contains(t, strings.Join(warnings(out), "\n"), "is before the API's 45-day request window; reading from 2026-09-04")
		})
	}
}

// TestReadEmitsOnlyDaysFromStart checks that rows the API returns for a day
// before the start are not emitted.
func TestReadEmitsOnlyDaysFromStart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d := r.URL.Query().Get("start")
		_, _ = w.Write([]byte(`{"code":200,"results":[{"day":"2026-10-13","campaign_id_external":"cmp-0009"},{"day":"` + d + `","campaign_id_external":"cmp-0001"}]}`))
	}))
	t.Cleanup(srv.Close)
	src := testSource(srv)
	state := `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"advertiser_report","namespace":"applovin"},"stream_state":{"day":"2026-10-17"}}}]`
	out := runRead(t, src, `{"api_key":"k","start_date":"2026-10-01"}`, configured(t, src, synclet.SyncModeIncremental, streamAdvertiserReport), state)
	require.NoError(t, out.Err)
	var got []string
	for _, r := range out.Records(synclet.StreamDescriptor{Name: streamAdvertiserReport, Namespace: namespace}) {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(r.Data, &rec))
		got = append(got, rec["day"].(string))
	}
	assert.Equal(t, []string{"2026-10-14", "2026-10-15", "2026-10-16", "2026-10-17", "2026-10-18"}, got)
}

func TestReadHourOnlyForLast30Days(t *testing.T) {
	srv, rs := newRecordingServer(t)
	src := testSource(srv)
	cat := withProperties(t, configured(t, src, synclet.SyncModeFullRefresh, streamAdvertiserReport), "day", "hour", "cost")
	out := runRead(t, src, `{"api_key":"k","start_date":"2026-09-17"}`, cat, "")
	require.NoError(t, out.Err)
	require.Equal(t, days(t, "2026-09-17", "2026-10-18"), rs.days)
	withHour := "day,hour,campaign_id_external,platform,device_type,country,cost"
	withoutHour := "day,campaign_id_external,platform,device_type,country,cost"
	for i, d := range rs.days {
		want := withHour
		if d < "2026-09-19" {
			want = withoutHour
		}
		assert.Equal(t, want, rs.columns[i], d)
	}
	assert.Contains(t, strings.Join(warnings(out), "\n"), "hour is available for the last 30 days only; days before 2026-09-19 are read without it")
}

func TestReadConvertsNumbers(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, `{"api_key":"`+testReportKey+`","start_date":"2026-10-18"}`, configured(t, src, synclet.SyncModeFullRefresh, streamAdvertiserReport, streamMaxRevenueReport), "")
	require.NoError(t, out.Err)
	adv := out.Records(synclet.StreamDescriptor{Name: streamAdvertiserReport, Namespace: namespace})
	require.Len(t, adv, 2)
	assert.JSONEq(t, `{"day":"2026-10-18","campaign":"Example Puzzle - US iOS","campaign_id_external":"cmp-0001",
		"campaign_package_name":"com.example.puzzle","campaign_store_id":"1000000001","campaign_type":"CPP","campaign_ad_type":"ua",
		"platform":"ios","device_type":"phone","country":"us","impressions":10400,"clicks":200,"ctr":0.02,"conversions":20,
		"cost":50.00,"average_cpa":2.50,"ad_rev_0d":5.00,"roas_7d":0.40,"ret_1d":0.35}`, string(adv[0].Data))
	mx := out.Records(synclet.StreamDescriptor{Name: streamMaxRevenueReport, Namespace: namespace})
	require.Len(t, mx, 2)
	assert.JSONEq(t, `{"day":"2026-10-18","application":"Example Runner","package_name":"com.example.runner","store_id":"com.example.runner",
		"platform":"android","country":"de","device_type":"phone","ad_format":"BANNER","max_ad_unit_id":"au0000000000002",
		"max_ad_unit":"Example Banner","network":"EXAMPLE_NETWORK","impressions":4000,"estimated_revenue":2.00,"ecpm":0.50,"fill_rate":0.75}`, string(mx[1].Data))
}

func TestReadFailsOnNonNumericValue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":200,"results":[{"day":"2026-10-18","campaign_id_external":"cmp-0001","cost":"12.5%"}]}`))
	}))
	t.Cleanup(srv.Close)
	src := testSource(srv)
	out := runRead(t, src, `{"api_key":"k","start_date":"2026-10-18"}`, configured(t, src, synclet.SyncModeFullRefresh, streamAdvertiserReport), "")
	require.ErrorContains(t, out.Err, `advertiser_report: day 2026-10-18: column cost: "12.5%" is not a number`)
	assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: streamAdvertiserReport, Namespace: namespace}))
}

func TestReadUnavailableReportType(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{"maxReport": forbidden})
	src := testSource(srv)
	out := runRead(t, src, testConfig, configured(t, src, synclet.SyncModeFullRefresh, streamMaxRevenueReport), "")
	require.ErrorContains(t, out.Err, "max_revenue_report: day 2026-10-14: report type MAX revenue is not available to the report key (HTTP 403)")
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
}
