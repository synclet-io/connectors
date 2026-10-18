package main

import (
	"context"
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

func runDiscover(t *testing.T, src Source, cfg json.RawMessage) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: cfg})
	require.NoError(t, err)
	return out
}

// configured discovers cfg and configures the named streams of spreadsheet
// A, or every stream without names, with mode.
func configured(t *testing.T, src Source, cfg json.RawMessage, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	out := runDiscover(t, src, cfg)
	require.NoError(t, out.Err)
	byName := map[string]synclet.Stream{}
	cat := &synclet.ConfiguredCatalog{}
	for _, s := range out.Catalog().Streams {
		byName[s.Namespace+"/"+s.Name] = s
		if len(names) == 0 {
			cat.Streams = append(cat.Streams, configuredStream(s, mode))
		}
	}
	for _, n := range names {
		s, ok := byName["/"+n]
		require.True(t, ok, "stream %s not discovered", n)
		cat.Streams = append(cat.Streams, configuredStream(s, mode))
	}
	return cat
}

func configuredStream(s synclet.Stream, mode synclet.SyncMode) synclet.ConfiguredStream {
	dst := synclet.DestinationSyncModeOverwrite
	if mode == synclet.SyncModeIncremental {
		dst = synclet.DestinationSyncModeAppend
	}
	return synclet.ConfiguredStream{Stream: s, SyncMode: mode, DestinationSyncMode: dst}
}

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

// records decodes the records of a stream with numbers kept as json.Number.
func records(t *testing.T, out *testkit.Output, desc synclet.StreamDescriptor) []map[string]any {
	t.Helper()
	var recs []map[string]any
	for _, r := range out.Records(desc) {
		dec := json.NewDecoder(strings.NewReader(string(r.Data)))
		dec.UseNumber()
		var m map[string]any
		require.NoError(t, dec.Decode(&m))
		recs = append(recs, m)
	}
	return recs
}

func stream(name string) synclet.StreamDescriptor { return synclet.StreamDescriptor{Name: name} }

func warnings(out *testkit.Output) []string {
	var ws []string
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn {
			ws = append(ws, l.Message)
		}
	}
	return ws
}

func withBatchSize(n int) func(*Config) { return func(c *Config) { c.BatchSize = n } }

func TestReadBatches(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, withBatchSize(2))
	cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "People")
	before := len(h.ranges(testSheetA))
	out := runRead(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)

	assert.Equal(t, []string{"'People'!1:1", "'People'!2:3", "'People'!4:5", "'People'!6:7"}, h.ranges(testSheetA)[before:],
		"one header request, then batches of 2 up to rowCount 7, past the empty rows 4 and 5")
	assert.Equal(t, []map[string]any{
		{"Full Name": "Alice", "Age": "30", "E-mail": "alice@example.com"},
		{"Full Name": "Bob", "Age": "", "E-mail": "bob@example.com"},
		{"Full Name": "Carol", "Age": "41", "E-mail": nil},
		{"Full Name": "Dave", "Age": "27", "E-mail": "dave@example.com"},
	}, records(t, out, stream("People")))
	assert.Empty(t, out.States(), "a full refresh emits no state")
	assert.Empty(t, warnings(out))
}

func TestReadLargeSheet(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "Large")
	before := len(h.ranges(testSheetA))
	out := runRead(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)

	assert.Equal(t, []string{"'Large'!1:1", "'Large'!2:10001", "'Large'!10002:10501"}, h.ranges(testSheetA)[before:])
	recs := records(t, out, stream("Large"))
	require.Len(t, recs, largeRows)
	assert.Equal(t, map[string]any{"ID": "1", "Item": "item-00001", "Added": "2026-01-02"}, recs[0])
	assert.Equal(t, "10001", recs[10000]["ID"], "the first row of the second batch")
	assert.Equal(t, "10500", recs[largeRows-1]["ID"])
}

func TestReadRaggedRows(t *testing.T) {
	t.Run("legacy warns once", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, nil)
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "Ragged"), nil)
		require.NoError(t, out.Err)
		assert.Equal(t, []map[string]any{
			{"Name": "Yan", "Age": "19"},
			{"Name": "Zoe", "Age": "22"},
			{"Name": "Uma", "Age": "35"},
		}, records(t, out, stream("Ragged")))
		assert.Equal(t, []string{
			`sheet "Ragged": row 3 has a value in column C, right of the last named column B; values right of B are not read (column_naming: stable reads every column up to the last header cell)`,
		}, warnings(out))
	})
	t.Run("stable fails", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, withStable)
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "Ragged"), nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `row 3 of sheet "Ragged" has a value in column C, right of the last header column B: give the column a header or clear the value`)
	})
}

func TestReadEmptyHeaderCell(t *testing.T) {
	t.Run("legacy", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, nil)
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "Gaps"), nil)
		require.NoError(t, out.Err)
		assert.Equal(t, []map[string]any{{"Name": "Ann"}}, records(t, out, stream("Gaps")))
		assert.Equal(t, []string{
			`sheet "Gaps": the header stops at the empty cell B1, so 1 header cell(s) right of it get no column (column_naming: stable names every column)`,
			`sheet "Gaps": row 2 has a value in column B, right of the last named column A; values right of A are not read (column_naming: stable reads every column up to the last header cell)`,
		}, warnings(out))
	})
	t.Run("stable", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, withStable)
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, "Gaps"), nil)
		require.NoError(t, out.Err)
		assert.Equal(t, []map[string]any{{"Name": "Ann", "column_B": "x", "Age": "33"}}, records(t, out, stream("Gaps")))
		assert.Empty(t, warnings(out))
	})
}

func TestReadHeaderChanged(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "People")
	cat.Streams[0].Stream.JSONSchema = json.RawMessage(`{"type":"object","properties":{"Full Name":{"type":["null","string"]},"Phone":{"type":["null","string"]}}}`)
	out := runRead(t, src, cfg, cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `column "Phone" is no longer in the header of sheet "People"; refresh the source schema`)
	assert.Empty(t, out.Records(stream("People")))
}

func TestReadSheetGone(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "People")
	cat.Streams[0].Stream.Name = "Archive"
	out := runRead(t, src, cfg, cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `sheet "Archive" is no longer a grid sheet with rows in spreadsheet `+testSheetA+`; refresh the source schema`)
}

func TestReadValueRender(t *testing.T) {
	tests := []struct {
		name      string
		edit      func(*Config)
		render    string
		dateTime  string
		valueType []any
		want      []map[string]any
	}{
		{
			name:      "formatted by default",
			render:    "FORMATTED_VALUE",
			valueType: []any{"null", "string"},
			want: []map[string]any{
				{"Item": "Widget", "Price": "$1,234.50", "In stock": "TRUE", "Added": "2026-10-01"},
				{"Item": "Gadget", "Price": "$0.99", "In stock": "FALSE", "Added": "2026-10-02 14:30:00"},
			},
		},
		{
			name:      "unformatted",
			edit:      func(c *Config) { c.ValueRender = valueRenderUnformatted },
			render:    "UNFORMATTED_VALUE",
			dateTime:  "FORMATTED_STRING",
			valueType: []any{"null", "string", "number", "boolean"},
			want: []map[string]any{
				{"Item": "Widget", "Price": json.Number("1234.5"), "In stock": true, "Added": "2026-10-01"},
				{"Item": "Gadget", "Price": json.Number("0.99"), "In stock": false, "Added": "2026-10-02 14:30:00"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src, _ := testSource(srv)
			cfg := testConfig(t, tt.edit)
			cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "Prices")
			var schema struct {
				Properties map[string]struct {
					Type []any `json:"type"`
				} `json:"properties"`
			}
			require.NoError(t, json.Unmarshal(cat.Streams[0].Stream.JSONSchema, &schema))
			assert.Equal(t, tt.valueType, schema.Properties["Price"].Type)

			before := len(h.list())
			out := runRead(t, src, cfg, cat, nil)
			require.NoError(t, out.Err)
			assert.Equal(t, tt.want, records(t, out, stream("Prices")))
			var rowRequests, headerRequests int
			for _, r := range h.list()[before:] {
				if r.path != batchGetPath(testSheetA) {
					continue
				}
				if r.query.Get("ranges") == "'Prices'!1:1" {
					headerRequests++
					assert.Equal(t, "FORMATTED_VALUE", r.query.Get("valueRenderOption"), "header rows are read formatted, so a numeric header stays a name")
					assert.Empty(t, r.query.Get("dateTimeRenderOption"))
					continue
				}
				rowRequests++
				assert.Equal(t, tt.render, r.query.Get("valueRenderOption"))
				assert.Equal(t, tt.dateTime, r.query.Get("dateTimeRenderOption"))
			}
			assert.Equal(t, 1, rowRequests)
			assert.Equal(t, 1, headerRequests)
		})
	}
}

func TestReadQuota(t *testing.T) {
	rateLimited := fixture{status: http.StatusTooManyRequests, file: "errors/rate_limit"}
	tests := []struct {
		name     string
		pages    []fixture
		err      string
		waits    []time.Duration
		requests int
	}{
		{
			name:     "rate limit passes after doubling waits",
			pages:    []fixture{rateLimited, rateLimited, {}},
			waits:    []time.Duration{time.Second + time.Millisecond, 2*time.Second + time.Millisecond},
			requests: 3,
		},
		{
			name:     "Retry-After is honoured",
			pages:    []fixture{{status: http.StatusTooManyRequests, file: "errors/rate_limit", header: map[string]string{"Retry-After": "7"}}, {}},
			waits:    []time.Duration{7 * time.Second},
			requests: 2,
		},
		{
			name:     "5xx is retried",
			pages:    []fixture{{status: http.StatusServiceUnavailable, file: "errors/backend_error"}, {}},
			waits:    []time.Duration{time.Second + time.Millisecond},
			requests: 2,
		},
		{
			name:  "exhausted retries name the quota",
			pages: []fixture{rateLimited},
			err:   "Google Sheets API quota ReadRequestsPerMinutePerUser (60) is still exceeded after 5 attempts: lower requests_per_minute (now 60) or raise the quota in Google Cloud",
			waits: []time.Duration{
				time.Second + time.Millisecond, 2*time.Second + time.Millisecond,
				4*time.Second + time.Millisecond, 8*time.Second + time.Millisecond,
			},
			requests: 5,
		},
		{
			name:     "a missing permission is not retried",
			pages:    []fixture{{status: http.StatusForbidden, file: "errors/permission_denied"}},
			err:      "is not shared with service account",
			requests: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discoverSrv, _ := fixtureServer(t, nil)
			discoverSrc, _ := testSource(discoverSrv)
			cfg := testConfig(t, nil)
			cat := configured(t, discoverSrc, cfg, synclet.SyncModeFullRefresh, "People")

			srv, h := fixtureServer(t, map[string][]fixture{batchGetKey(testSheetA): append([]fixture{{}}, tt.pages...)})
			src, w := testSource(srv)
			out := runRead(t, src, cfg, cat, nil)
			if tt.err == "" {
				require.NoError(t, out.Err)
				assert.Len(t, out.Records(stream("People")), 4)
			} else {
				require.Error(t, out.Err)
				assert.Contains(t, out.Err.Error(), tt.err)
			}
			assert.Equal(t, tt.waits, w.list())
			assert.Len(t, h.ranges(testSheetA), 1+tt.requests, "the header request and the row requests")
		})
	}
}

func TestPacer(t *testing.T) {
	now := testNow
	var waited []time.Duration
	p := &pacer{
		interval: time.Second,
		now:      func() time.Time { return now },
		wait: func(_ context.Context, d time.Duration) error {
			waited = append(waited, d)
			now = now.Add(d)
			return nil
		},
	}
	ctx := t.Context()
	require.NoError(t, p.next(ctx))
	require.NoError(t, p.next(ctx))
	now = now.Add(400 * time.Millisecond)
	require.NoError(t, p.next(ctx))
	now = now.Add(time.Minute)
	require.NoError(t, p.next(ctx))
	assert.Equal(t, []time.Duration{time.Second, 600 * time.Millisecond}, waited)
}

func TestReadPaces(t *testing.T) {
	for _, tt := range []struct {
		name     string
		rpm      *int
		interval time.Duration
	}{
		{name: "60 requests per minute by default", interval: time.Second},
		{name: "requests_per_minute", rpm: new(120), interval: 500 * time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src, _ := testSource(srv)
			var paced []time.Duration
			src.paceWait = func(_ context.Context, d time.Duration) error { paced = append(paced, d); return nil }
			cfg := testConfig(t, func(c *Config) { c.RequestsPerMinute = tt.rpm })
			cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "People")
			paced = nil
			before := len(h.list())
			out := runRead(t, src, cfg, cat, nil)
			require.NoError(t, out.Err)
			require.Equal(t, []string{"POST token", spreadsheetKey(testSheetA), batchGetKey(testSheetA), batchGetKey(testSheetA)}, h.keys()[before:])
			assert.Equal(t, []time.Duration{tt.interval, tt.interval}, paced, "the fixed test clock makes every API request after the first wait the whole interval")
		})
	}
}

func TestReadNamesConversion(t *testing.T) {
	for _, raw := range []string{`true`, `"true"`} {
		t.Run(raw, func(t *testing.T) {
			srv, _ := fixtureServer(t, nil)
			src, _ := testSource(srv)
			var cfg map[string]any
			require.NoError(t, json.Unmarshal(testConfig(t, nil), &cfg))
			cfg["names_conversion"] = json.RawMessage(raw)
			rawCfg, err := json.Marshal(cfg)
			require.NoError(t, err)
			out := runRead(t, src, rawCfg, configured(t, src, rawCfg, synclet.SyncModeFullRefresh, "People"), nil)
			require.NoError(t, out.Err)
			recs := records(t, out, stream("People"))
			require.NotEmpty(t, recs)
			assert.Equal(t, map[string]any{"full_name": "Alice", "age": "30", "e_mail": "alice@example.com"}, recs[0])
		})
	}
}

func TestReadMultipleSpreadsheets(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) {
		c.AdditionalSpreadsheetIDs = []string{"https://docs.google.com/spreadsheets/d/" + testSheetB + "/edit"}
	})
	out := runDiscover(t, src, cfg)
	require.NoError(t, out.Err)
	var names []string
	for _, s := range out.Catalog().Streams {
		names = append(names, s.Namespace+"/"+s.Name)
	}
	assert.Equal(t, []string{"/People", "/Prices", "/Ragged", "/Gaps", "/Large", testSheetB + "/Orders"}, names)

	cat := &synclet.ConfiguredCatalog{}
	for _, s := range out.Catalog().Streams {
		if s.Name == "People" || s.Name == "Orders" {
			cat.Streams = append(cat.Streams, configuredStream(s, synclet.SyncModeFullRefresh))
		}
	}
	read := runRead(t, src, cfg, cat, nil)
	require.NoError(t, read.Err)
	assert.Len(t, read.Records(stream("People")), 4)
	assert.Equal(t, []map[string]any{
		{"Order ID": "ord-1", "Total": "10.00"},
		{"Order ID": "ord-2", "Total": "24.50"},
	}, records(t, read, synclet.StreamDescriptor{Name: "Orders", Namespace: testSheetB}))
	assert.Empty(t, read.Records(stream("Orders")), "streams of additional spreadsheets carry the spreadsheet ID as namespace")
}

func TestReadUnknownNamespace(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	cat := configured(t, src, cfg, synclet.SyncModeFullRefresh, "People")
	cat.Streams[0].Stream.Namespace = testSheetB
	out := runRead(t, src, cfg, cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "stream People has namespace "+testSheetB+", which is not a configured spreadsheet; refresh the source schema")
}

func TestDiscover(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	out := runDiscover(t, src, testConfig(t, nil))
	require.NoError(t, out.Err)
	streams := out.Catalog().Streams
	var names []string
	for _, s := range streams {
		names = append(names, s.Name)
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, s.SupportedSyncModes)
		assert.False(t, s.SourceDefinedCursor)
		assert.Empty(t, s.Namespace)
	}
	assert.Equal(t, []string{"People", "Prices", "Ragged", "Gaps", "Large"}, names, "Chart is no grid sheet and Blank has no header")
	assert.JSONEq(t, `{"type":"object","properties":{"Full Name":{"type":["null","string"]},"Age":{"type":["null","string"]},"E-mail":{"type":["null","string"]}}}`, string(streams[0].JSONSchema))
	assert.Equal(t, []string{
		`sheet "Gaps": the header stops at the empty cell B1, so 1 header cell(s) right of it get no column (column_naming: stable names every column)`,
		`Skipping sheet "Blank": no headers found in header row`,
	}, warnings(out))
	assert.Equal(t, []string{"POST token", spreadsheetKey(testSheetA), batchGetKey(testSheetA)}, h.keys(), "all header rows in one batchGet, no Drive request")
	assert.Equal(t, []string{"'People'!1:1,'Prices'!1:1,'Ragged'!1:1,'Gaps'!1:1,'Blank'!1:1,'Large'!1:1"}, h.ranges(testSheetA))
}

func TestDiscoverQuotesSheetTitles(t *testing.T) {
	assert.Equal(t, "'Bob''s list'!2:10", a1Range("Bob's list", 2, 10))
	title, first, last, ok := parseRange(a1Range("Bob's list", 2, 10))
	assert.True(t, ok)
	assert.Equal(t, "Bob's list", title)
	assert.Equal(t, []int{2, 10}, []int{first, last})
}
