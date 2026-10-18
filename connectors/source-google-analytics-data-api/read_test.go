package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runRead(t *testing.T, src Source, cfg json.RawMessage, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: cfg, Catalog: raw, State: state,
	})
	require.NoError(t, err)
	return out
}

// records decodes the records of stream with numbers kept as json.Number.
func records(t *testing.T, out *testkit.Output, stream string) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream, Namespace: namespace}) {
		dec := json.NewDecoder(strings.NewReader(string(r.Data)))
		dec.UseNumber()
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		recs = append(recs, m)
	}
	return recs
}

func streamStates(t *testing.T, out *testkit.Output) []string {
	t.Helper()
	var states []string
	for _, s := range out.States() {
		require.NotNil(t, s.Stream)
		states = append(states, string(s.Stream.StreamState))
	}
	return states
}

func warnings(out *testkit.Output) []string {
	var ws []string
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn {
			ws = append(ws, l.Message)
		}
	}
	return ws
}

func onlyPropA(c *Config) { c.PropertyIDs = []string{testPropA} }

var pagesMetrics = []string{"screenPageViews", "bounceRate"}

func TestReadPaging(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	src.pageLimit = 2
	cfg := testConfig(t, func(c *Config) { onlyPropA(c); c.StartDate = "2026-10-14"; c.EndDate = "2026-10-14" })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "pages"), nil)
	require.NoError(t, out.Err)

	var offsets []int
	for _, r := range h.reports(testPropA, pagesMetrics...) {
		assert.Equal(t, 2, r.Limit)
		assert.Equal(t, []dateRange{{StartDate: "2026-10-14", EndDate: "2026-10-14"}}, r.DateRanges)
		assert.Equal(t, []nameObj{{"date"}, {"hostName"}, {"pagePathPlusQueryString"}}, r.Dimensions)
		offsets = append(offsets, r.Offset)
	}
	assert.Equal(t, []int{0, 2, 4}, offsets)

	recs := records(t, out, "pages")
	require.Len(t, recs, 5)
	var paths []string
	for _, r := range recs {
		paths = append(paths, r["pagePathPlusQueryString"].(string))
		assert.Equal(t, "2026-10-14", r["date"])
		assert.Equal(t, testPropA, r["property_id"])
		assert.Equal(t, "shop.example.test", r["hostName"])
	}
	assert.Equal(t, []string{"/", "/pricing", "/blog/a", "/blog/b", "/signup"}, paths)
	assert.Empty(t, out.States(), "a full refresh emits no state")
}

func TestReadShortPage(t *testing.T) {
	report := "POST properties/" + testPropA + ":runReport"
	srv, _ := fixtureServer(t, map[string][]fixture{report: {{}, {}, {file: "responses/pages_short"}}})
	src, _ := testSource(srv)
	src.pageLimit = 2
	cfg := testConfig(t, func(c *Config) { onlyPropA(c); c.StartDate = "2026-10-13"; c.EndDate = "2026-10-14" })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, "pages"), nil)

	require.Error(t, out.Err)
	assert.Equal(t, "pages: property 123456789, 2026-10-14: empty page at offset 2 of 5 rows", out.Err.Error())
	assert.Len(t, records(t, out, "pages"), 2, "the first page of 2026-10-14 was emitted")
	assert.Equal(t, []string{`{"properties":{"123456789":{"date":"2026-10-13"}}}`}, streamStates(t, out),
		"no state after the day that failed")
}

func TestReadRowCountChange(t *testing.T) {
	report := "POST properties/" + testPropA + ":runReport"
	srv, _ := fixtureServer(t, map[string][]fixture{report: {{}, {file: "responses/conversions_bad_type"}}})
	src, _ := testSource(srv)
	src.pageLimit = 2
	cfg := testConfig(t, func(c *Config) { onlyPropA(c); c.StartDate = "2026-10-14"; c.EndDate = "2026-10-14" })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "pages"), nil)
	require.Error(t, out.Err)
	assert.Equal(t, "pages: property 123456789, 2026-10-14: rowCount changed from 5 to 1 at offset 2", out.Err.Error())
}

var conversionsReport = CustomReport{Name: "conversions", Metrics: []string{"sessions", "bounceRate"}}

func TestReadConversion(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		onlyPropA(c)
		c.StartDate, c.EndDate = "2026-10-10", "2026-10-11"
		c.CustomReports = []CustomReport{conversionsReport}
	})

	t.Run("integers and decimals", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src, _ := testSource(srv)
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "conversions"), nil)
		require.NoError(t, out.Err)
		recs := out.Records(synclet.StreamDescriptor{Name: "conversions", Namespace: namespace})
		require.Len(t, recs, 2)
		assert.JSONEq(t, `{"property_id":"123456789","date":"2026-10-10","sessions":12,"bounceRate":0.1234567890123456789,`+
			`"sampled":false,"sampling_ratio":null,"subject_to_thresholding":false,"data_loss_from_other_row":false}`, string(recs[0].Data))
		assert.Contains(t, string(recs[0].Data), `"bounceRate":0.1234567890123456789`, "the decimal literal is emitted unchanged")
		assert.Contains(t, string(recs[1].Data), `"sessions":9223372036854775807`)
		assert.Contains(t, string(recs[1].Data), `"bounceRate":1e-7`)
	})

	report := "POST properties/" + testPropA + ":runReport"
	for name, tc := range map[string]struct{ file, err string }{
		"non-integer value of an integer metric": {
			file: "responses/conversions_bad_integer",
			err:  `conversions: property 123456789, 2026-10-10: row 0: metric sessions: "1.5" is not an integer`,
		},
		"metric type differs from the schema": {
			file: "responses/conversions_bad_type",
			err:  "conversions: property 123456789, 2026-10-10: metric sessions is TYPE_FLOAT in the response, integer in the schema",
		},
	} {
		t.Run(name, func(t *testing.T) {
			srv, _ := fixtureServer(t, map[string][]fixture{report: {{file: tc.file}}})
			src, _ := testSource(srv)
			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, "conversions"), nil)
			require.Error(t, out.Err)
			assert.Equal(t, tc.err, out.Err.Error())
			assert.Empty(t, records(t, out, "conversions"))
			assert.Empty(t, out.States())
		})
	}
}

func TestReadSampling(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		onlyPropA(c)
		c.StartDate, c.EndDate = "2026-10-12", "2026-10-12"
		c.CustomReports = []CustomReport{{Name: "sampled_report", Metrics: []string{"totalUsers"}}}
	})
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "sampled_report"), nil)
	require.NoError(t, out.Err)
	recs := out.Records(synclet.StreamDescriptor{Name: "sampled_report", Namespace: namespace})
	require.Len(t, recs, 1)
	assert.JSONEq(t, `{"property_id":"123456789","date":"2026-10-12","totalUsers":4210,`+
		`"sampled":true,"sampling_ratio":0.25,"subject_to_thresholding":true,"data_loss_from_other_row":false}`, string(recs[0].Data))
	assert.Equal(t, []string{
		"report data may be incomplete property=123456789 report=sampled_report day=2026-10-12 sampled=true sampling_ratio=0.25 " +
			"subject_to_thresholding=true data_loss_from_other_row=false",
	}, warnings(out))
}

func TestReadDataQuality(t *testing.T) {
	tests := []struct {
		report, metric, record, warning string
	}{
		{
			report: "data_loss_report", metric: "newUsers",
			record: `{"property_id":"123456789","date":"2026-10-12","newUsers":318,` +
				`"sampled":false,"sampling_ratio":null,"subject_to_thresholding":false,"data_loss_from_other_row":true}`,
			warning: "report data may be incomplete property=123456789 report=data_loss_report day=2026-10-12 sampled=false sampling_ratio=<nil> " +
				"subject_to_thresholding=false data_loss_from_other_row=true",
		},
		{
			report: "thresholded_report", metric: "screenPageViews",
			record: `{"property_id":"123456789","date":"2026-10-12","screenPageViews":77,` +
				`"sampled":false,"sampling_ratio":null,"subject_to_thresholding":true,"data_loss_from_other_row":false}`,
			warning: "report data may be incomplete property=123456789 report=thresholded_report day=2026-10-12 sampled=false sampling_ratio=<nil> " +
				"subject_to_thresholding=true data_loss_from_other_row=false",
		},
	}
	for _, tt := range tests {
		t.Run(tt.report, func(t *testing.T) {
			cfg := testConfig(t, func(c *Config) {
				onlyPropA(c)
				c.StartDate, c.EndDate = "2026-10-12", "2026-10-12"
				c.CustomReports = []CustomReport{{Name: tt.report, Metrics: []string{tt.metric}}}
			})
			srv, _ := fixtureServer(t, nil)
			src, _ := testSource(srv)
			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, tt.report), nil)
			require.NoError(t, out.Err)
			recs := out.Records(synclet.StreamDescriptor{Name: tt.report, Namespace: namespace})
			require.Len(t, recs, 1)
			assert.JSONEq(t, tt.record, string(recs[0].Data))
			assert.Equal(t, []string{tt.warning}, warnings(out))
		})
	}
}

func TestReadRowOfAnotherDay(t *testing.T) {
	report := "POST properties/" + testPropA + ":runReport"
	srv, _ := fixtureServer(t, map[string][]fixture{report: {{file: "responses/wrong_date"}}})
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) { onlyPropA(c); c.StartDate = "2026-10-16" })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, "daily_active_users"), nil)
	require.Error(t, out.Err)
	assert.Equal(t, "daily_active_users: property 123456789, 2026-10-16: row 0: row of date 2026-10-15 in the report of 2026-10-16", out.Err.Error())
	assert.Empty(t, records(t, out, "daily_active_users"))
	assert.Empty(t, out.States())
}

func TestReadSamplingWithoutDataQualityFields(t *testing.T) {
	cfg := testConfig(t, func(c *Config) {
		onlyPropA(c)
		c.StartDate, c.EndDate = "2026-10-12", "2026-10-12"
		c.DataQualityFields = boolp(false)
		c.CustomReports = []CustomReport{{Name: "sampled_report", Metrics: []string{"totalUsers"}}}
	})
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "sampled_report"), nil)
	require.NoError(t, out.Err)
	recs := out.Records(synclet.StreamDescriptor{Name: "sampled_report", Namespace: namespace})
	require.Len(t, recs, 1)
	assert.JSONEq(t, `{"property_id":"123456789","date":"2026-10-12","totalUsers":4210}`, string(recs[0].Data))
	assert.Len(t, warnings(out), 1, "the warning does not depend on data_quality_fields")
}

func TestDecimal(t *testing.T) {
	for in, want := range map[[2]string]string{
		{"250000", "1000000"}: "0.25",
		{"1", "3"}:            "0.333333333333333333",
		{"7", "8"}:            "0.875",
		{"5", "5"}:            "1",
		{"1", "1024"}:         "0.0009765625",
	} {
		var r reportResponse
		r.Metadata.SamplingMetadatas = append(r.Metadata.SamplingMetadatas, struct {
			SamplesReadCount  json.Number `json:"samplesReadCount"`
			SamplingSpaceSize json.Number `json:"samplingSpaceSize"`
		}{json.Number(in[0]), json.Number(in[1])})
		q, err := responseQuality(r)
		require.NoError(t, err)
		assert.Equal(t, json.Number(want), q.ratio, in)
	}
}

func TestReadQuota(t *testing.T) {
	report := "POST properties/" + testPropA + ":runReport"
	low := fixture{file: "responses/hour_low"}
	cfg := testConfig(t, func(c *Config) { onlyPropA(c); c.StartDate = "2026-10-11" })
	tests := []struct {
		name      string
		overrides []fixture
		waits     []time.Duration
		err       string
		requests  int
		warnings  []string
	}{
		{
			name:      "hourly tokens low waits for the next hour",
			overrides: []fixture{low, {}},
			waits:     []time.Duration{30 * time.Minute},
			requests:  6,
		},
		{
			name:      "hourly tokens low twice in a row waits twice",
			overrides: []fixture{low, low, {}},
			waits:     []time.Duration{30 * time.Minute, 30 * time.Minute},
			requests:  6,
		},
		{
			name:      "hourly tokens low three times in a row fails",
			overrides: []fixture{low, low, low},
			waits:     []time.Duration{30 * time.Minute, 30 * time.Minute},
			err:       "daily_active_users: property 123456789, 2026-10-14: hourly token quota of property 123456789 is still exhausted after waiting 2 hours",
			requests:  3,
		},
		{
			name:      "daily tokens exhausted fails before the next request",
			overrides: []fixture{{}, {file: "responses/day_out"}, {}},
			err:       "daily_active_users: property 123456789, 2026-10-13: daily token quota of property 123456789 is exhausted (last request for 2026-10-12); the next sync continues from the saved state",
			requests:  2,
			warnings:  []string{"daily token quota of the property is exhausted; its next request fails property=123456789 day=2026-10-12 consumed=12 remaining=0"},
		},
		{
			name:      "daily tokens exhausted on the last request warns",
			overrides: []fixture{{}, {}, {}, {}, {}, {file: "responses/day_out"}},
			requests:  6,
			warnings:  []string{"daily token quota of the property is exhausted; its next request fails property=123456789 day=2026-10-16 consumed=12 remaining=0"},
		},
		{
			name:      "429 naming daily tokens fails at once",
			overrides: []fixture{{}, {status: http.StatusTooManyRequests, file: "errors/quota_daily"}},
			err:       "daily_active_users: property 123456789, 2026-10-12: daily token quota of property 123456789 is exhausted; the next sync continues from the saved state (properties/123456789:runReport: HTTP 429 RESOURCE_EXHAUSTED: Exhausted property tokens per day",
			requests:  2,
		},
		{
			name:      "other 429 retries with backoff",
			overrides: []fixture{{status: http.StatusTooManyRequests, file: "errors/quota_concurrent"}, {status: http.StatusTooManyRequests, file: "errors/quota_concurrent"}, {}},
			waits:     []time.Duration{time.Second + time.Millisecond, 2*time.Second + time.Millisecond},
			requests:  8,
		},
		{
			name:      "5xx retries with backoff",
			overrides: []fixture{{status: http.StatusInternalServerError, file: "errors/internal"}, {}},
			waits:     []time.Duration{time.Second + time.Millisecond},
			requests:  7,
		},
		{
			name: "5xx five times fails",
			overrides: []fixture{
				{status: http.StatusInternalServerError, file: "errors/internal"},
			},
			waits:    []time.Duration{time.Second + time.Millisecond, 2*time.Second + time.Millisecond, 4*time.Second + time.Millisecond, 8*time.Second + time.Millisecond},
			err:      "daily_active_users: property 123456789, 2026-10-11: properties/123456789:runReport: HTTP 500 INTERNAL: Internal error encountered.",
			requests: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, map[string][]fixture{report: tt.overrides})
			src, w := testSource(srv)
			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, "daily_active_users"), nil)
			if tt.err == "" {
				require.NoError(t, out.Err)
			} else {
				require.Error(t, out.Err)
				assert.True(t, strings.HasPrefix(out.Err.Error(), tt.err), out.Err.Error())
			}
			assert.Equal(t, tt.waits, w.list())
			assert.Len(t, h.reports(testPropA, "active1DayUsers"), tt.requests)
			if tt.warnings != nil {
				assert.Equal(t, tt.warnings, warnings(out))
			}
		})
	}
}

func TestReadRefreshesExpiredToken(t *testing.T) {
	report := "POST properties/" + testPropA + ":runReport"
	srv, h := fixtureServer(t, map[string][]fixture{report: {{status: http.StatusUnauthorized, file: "errors/unauthenticated"}, {}}})
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) { onlyPropA(c); c.StartDate = "2026-10-16" })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "daily_active_users"), nil)
	require.NoError(t, out.Err)
	assert.Len(t, records(t, out, "daily_active_users"), 1)
	assert.Equal(t, []string{
		"POST token", "GET properties/123456789/metadata", // discover
		"POST token", "GET properties/123456789/metadata", // read
		"POST properties/123456789:runReport", "POST token", "POST properties/123456789:runReport",
	}, h.keys())
}

func TestReadUnknownStream(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "pages")
	cat.Streams[0].Stream.Name = "pages_v2"
	out := runRead(t, src, cfg, cat, nil)
	require.Error(t, out.Err)
	assert.Equal(t, `unknown stream "pages_v2"`, out.Err.Error())
}

func TestReadAllBuiltins(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, withMyReport)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, append(builtinNames(), "my_report")...), nil)
	require.NoError(t, out.Err)
	for _, name := range append(builtinNames(), "my_report") {
		recs := records(t, out, name)
		props := map[string]int{}
		for _, r := range recs {
			props[r["property_id"].(string)]++
		}
		assert.Positive(t, props[testPropA], name)
		assert.Positive(t, props[testPropB], name)
	}
	r := records(t, out, "my_report")[0]
	assert.Equal(t, "free", r["customEvent_plan_tier"])
	assert.IsType(t, json.Number(""), r["totalRevenue"])
}
