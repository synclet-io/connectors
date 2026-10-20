package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func num(s string) json.Number { return json.Number(s) }

func TestExportFullRefresh(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamExport)
	before := len(h.list())
	out := read(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)

	assert.Equal(t, []string{
		"export export?from_date=2026-10-10&to_date=2026-10-14",
		"export export?from_date=2026-10-15&to_date=2026-10-18",
	}, h.list()[before:])
	recs := records(t, out, streamExport)
	require.Len(t, recs, 4)
	assert.Equal(t, map[string]any{
		"event": "Sign Up", "time": "2026-10-11T18:00:00Z", "distinct_id": "user-001", "insert_id": "ins-0001",
		"_browser": "Chrome", "browser": "custom-browser", "os": "Mac OS X", "plan": "free", "mp_country_code": "US",
	}, recs[0])
	assert.Equal(t, "10", recs[1]["amount"], "numbers become strings")
	assert.Equal(t, `["sku-a","sku-b"]`, recs[1]["items"], "lists become JSON")
	assert.Equal(t, "Firefox", recs[1]["_browser"], "$browser stays _browser where the project has browser too")
	assert.Equal(t, "2026-10-16T18:00:00.25Z", recs[2]["time"], "milliseconds")
	assert.Equal(t, "true", recs[2]["is_trial"])
	assert.Equal(t, "https://example.com/pricing", recs[2]["current_url"])
	assert.Equal(t, "25.5", recs[3]["amount"])
	assert.Empty(t, out.States(), "full refresh emits no state")
}

func TestExportIncremental(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeIncremental, streamExport)

	out := read(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"time":"2026-10-13T18:00:00Z"}`, `{"time":"2026-10-17T18:00:00Z"}`}, streamStates(out, streamExport))

	// 2026-10-17 18:00Z is 11:00 on 2026-10-17 in US/Pacific: the read
	// starts that day and emits nothing older than the saved time.
	before := len(h.list())
	out = read(t, src, cfg, cat, stateOf(t, streamExport, `{"time":"2026-10-17T18:00:00Z"}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		`export export?from_date=2026-10-17&to_date=2026-10-18&where=properties%5B%22%24time%22%5D%3E%3Ddatetime%281792260000%29`,
	}, h.list()[before:])
	assert.Equal(t, []any{"2026-10-17T18:00:00Z"}, field(records(t, out, streamExport), "time"))
	assert.Equal(t, []string{`{"time":"2026-10-17T18:00:00Z"}`}, streamStates(out, streamExport),
		"the state never goes below the saved time")

	t.Run("export lookback window", func(t *testing.T) {
		// 79200 s before the saved time is 2026-10-16 20:00Z, 13:00 on
		// 2026-10-16 in US/Pacific: the event of 18:00Z that day is older.
		lookback := 79200
		cfg := testConfig(t, func(c *Config) { c.ExportLookbackWindow = &lookback })
		before := len(h.list())
		out := read(t, src, cfg, cat, stateOf(t, streamExport, `{"time":"2026-10-17T18:00:00Z"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, []string{
			`export export?from_date=2026-10-16&to_date=2026-10-18&where=properties%5B%22%24time%22%5D%3E%3Ddatetime%281792180800%29`,
		}, h.list()[before:])
		assert.Equal(t, []any{"2026-10-17T18:00:00Z"}, field(records(t, out, streamExport), "time"))

		lookback = 100000
		cfg = testConfig(t, func(c *Config) { c.ExportLookbackWindow = &lookback })
		out = read(t, src, cfg, cat, stateOf(t, streamExport, `{"time":"2026-10-17T18:00:00Z"}`))
		require.NoError(t, out.Err)
		assert.Equal(t, []any{"2026-10-16T18:00:00.25Z", "2026-10-17T18:00:00Z"}, field(records(t, out, streamExport), "time"),
			"the lookback window is read again")
	})

	t.Run("attribution window and timezone", func(t *testing.T) {
		days := 30
		cfg := testConfig(t, func(c *Config) { c.AttributionWindow = &days; c.ProjectTimezone = "Asia/Tokyo" })
		before := len(h.list())
		out := read(t, src, cfg, cat, stateOf(t, streamExport, `{"time":"2026-10-17T18:00:00Z"}`))
		require.NoError(t, out.Err)
		// 18:00Z is 03:00 on 2026-10-18 in Tokyo, which is also today there.
		// Mixpanel reads the days of projects created since 2023 in UTC, so
		// the read starts at the earlier UTC day; attribution_window does not
		// move it.
		assert.Equal(t, []string{
			`export export?from_date=2026-10-17&to_date=2026-10-19&where=properties%5B%22%24time%22%5D%3E%3Ddatetime%281792260000%29`,
		}, h.list()[before:])
		assert.Equal(t, []any{"2026-10-17T18:00:00Z"}, field(records(t, out, streamExport), "time"))
	})
}

func TestExportBadEvent(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{"export": {file: "errors/server.json"}})
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	out := read(t, src, cfg, catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamExport), nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "has no time property")
	assert.Contains(t, out.Err.Error(), "2026-10-10 to 2026-10-14")
}

func TestEngage(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeIncremental, streamEngage)

	before := len(h.list())
	out := read(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		"query engage?page_size=2",
		"query engage?page=1&page_size=2&session_id=sess-test",
	}, h.list()[before:])
	recs := records(t, out, streamEngage)
	require.Len(t, recs, 3)
	assert.Equal(t, map[string]any{
		"distinct_id": "user-001", "last_seen": "2026-10-17T11:00:00", "created": "2026-01-01T09:00:00",
		"email": "user1@example.com", "name": "Test User 1", "plan_value": num("29"), "is_paid": true,
		"tags": []any{"beta", "vip"}, "zip": "94107",
	}, recs[0])
	assert.Equal(t, map[string]any{"city": "Springfield"}, recs[1]["address"])
	assert.Equal(t, "7", recs[2]["legacy_score"], "a property of unknown type is a string")
	assert.Equal(t, []string{`{"last_seen":"2026-10-17T11:00:00"}`}, streamStates(out, streamEngage))

	out = read(t, src, cfg, cat, stateOf(t, streamEngage, `{"last_seen":"2026-10-12T08:15:00"}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []any{"user-001", "user-002"}, field(records(t, out, streamEngage), "distinct_id"),
		"profiles last seen before the saved last_seen are skipped")
	assert.Equal(t, []string{`{"last_seen":"2026-10-17T11:00:00"}`}, streamStates(out, streamEngage))
}

func TestCohortsAndMembers(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeIncremental, streamCohorts, streamCohortMembers)

	before := len(h.list())
	out := read(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		"query cohorts/list",
		`query engage?filter_by_cohort=%7B%22id%22%3A101%7D&page_size=2`,
		`query engage?filter_by_cohort=%7B%22id%22%3A101%7D&page=1&page_size=2&session_id=sess-c101`,
		`query engage?filter_by_cohort=%7B%22id%22%3A102%7D&page_size=2`,
	}, h.list()[before:], "cohorts/list is requested once for both streams")

	cohorts := records(t, out, streamCohorts)
	assert.Equal(t, []any{num("101"), num("102")}, field(cohorts, "id"))
	assert.Equal(t, "Paid users", cohorts[0]["name"])
	assert.Equal(t, []string{`{"created":"2026-10-05 08:30:00"}`}, streamStates(out, streamCohorts))

	members := records(t, out, streamCohortMembers)
	assert.Equal(t, []any{num("101"), num("101"), num("102")}, field(members, "cohort_id"))
	assert.Equal(t, []any{"user-001", "user-003", "user-002"}, field(members, "distinct_id"))
	assert.Equal(t, []string{
		`{"last_seen":"2026-10-17T11:00:00","cohorts":{"101":"2026-10-17T11:00:00"}}`,
		`{"last_seen":"2026-10-12T08:15:00","cohorts":{"101":"2026-10-17T11:00:00","102":"2026-10-12T08:15:00"}}`,
	}, streamStates(out, streamCohortMembers))

	state, err := json.Marshal([]synclet.StateMessage{
		{Type: synclet.StateTypeStream, Stream: &synclet.StreamState{StreamDescriptor: synclet.StreamDescriptor{Name: streamCohorts}, StreamState: json.RawMessage(`{"created":"2026-10-01 00:00:00"}`)}},
		{Type: synclet.StateTypeStream, Stream: &synclet.StreamState{StreamDescriptor: synclet.StreamDescriptor{Name: streamCohortMembers}, StreamState: json.RawMessage(`{"last_seen":"2026-10-01T00:00:00"}`)}},
	})
	require.NoError(t, err)
	out = read(t, src, cfg, cat, state)
	require.NoError(t, out.Err)
	assert.Equal(t, []any{num("102")}, field(records(t, out, streamCohorts), "id"))
	assert.Equal(t, []any{"user-001", "user-002"}, field(records(t, out, streamCohortMembers), "distinct_id"))
}

func TestAnnotations(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t, func(c *Config) { c.EndDate = "2026-10-16" })
	fr := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamAnnotations)
	before := len(h.list())
	out := read(t, src, cfg, fr, nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"query annotations?from_date=2026-10-10&to_date=2026-10-16"}, h.list()[before:])
	recs := records(t, out, streamAnnotations)
	assert.Equal(t, []any{num("501"), num("502")}, field(recs, "id"))
	assert.Equal(t, "Pricing page launch", recs[0]["description"])

	cat := catalogOf(t, src, cfg, synclet.SyncModeIncremental, streamAnnotations)
	out = read(t, src, cfg, cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "full refresh only")
}

func TestFunnels(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeIncremental, streamFunnels)

	before := len(h.list())
	out := read(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		"query funnels/list",
		"query funnels?from_date=2026-10-10&funnel_id=7001&to_date=2026-10-14&unit=day",
		"query funnels?from_date=2026-10-15&funnel_id=7001&to_date=2026-10-18&unit=day",
		"query funnels?from_date=2026-10-10&funnel_id=7002&to_date=2026-10-14&unit=day",
		"query funnels?from_date=2026-10-15&funnel_id=7002&to_date=2026-10-18&unit=day",
	}, h.list()[before:])
	recs := records(t, out, streamFunnels)
	require.Len(t, recs, 18)
	assert.Equal(t, num("7001"), recs[0]["funnel_id"])
	assert.Equal(t, "Signup to purchase", recs[0]["name"])
	assert.Equal(t, "2026-10-10", recs[0]["date"])
	assert.Equal(t, map[string]any{"completion": num("10"), "starting_amount": num("20"), "steps": num("2"), "worst": num("1")}, recs[0]["analysis"])
	assert.Len(t, recs[0]["steps"], 2)
	assert.Equal(t, `{"funnels":{"7001":{"date":"2026-10-18"},"7002":{"date":"2026-10-18"}}}`, streamStates(out, streamFunnels)[3])

	before = len(h.list())
	out = read(t, src, cfg, cat, stateOf(t, streamFunnels, `{"funnels":{"7001":{"date":"2026-10-18"}}}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		"query funnels/list",
		"query funnels?from_date=2026-10-13&funnel_id=7001&to_date=2026-10-17&unit=day",
		"query funnels?from_date=2026-10-18&funnel_id=7001&to_date=2026-10-18&unit=day",
		"query funnels?from_date=2026-10-10&funnel_id=7002&to_date=2026-10-14&unit=day",
		"query funnels?from_date=2026-10-15&funnel_id=7002&to_date=2026-10-18&unit=day",
	}, h.list()[before:], "each funnel starts from its own saved day")
}

func TestRevenue(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeIncremental, streamRevenue)

	before := len(h.list())
	out := read(t, src, cfg, cat, stateOf(t, streamRevenue, `{"date":"2026-10-16"}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		"query engage/revenue?from_date=2026-10-11&to_date=2026-10-15&unit=day",
		"query engage/revenue?from_date=2026-10-16&to_date=2026-10-18&unit=day",
	}, h.list()[before:])
	recs := records(t, out, streamRevenue)
	require.Len(t, recs, 8, "$overall is left out")
	assert.Equal(t, map[string]any{"date": "2026-10-11", "amount": num("110.0"), "count": num("3"), "paid_count": num("2")}, recs[0])
	assert.Equal(t, []string{`{"date":"2026-10-16"}`, `{"date":"2026-10-18"}`}, streamStates(out, streamRevenue))
}

func TestProjectSecretAndRegion(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t, projectSecret)
	out := read(t, src, cfg, catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamCohorts), nil)
	require.NoError(t, out.Err, "a project secret authenticates without project_id")
	assert.Len(t, records(t, out, streamCohorts), 2)
	assert.Contains(t, h.list(), "query cohorts/list")

	for region, want := range map[string]map[api]string{
		"":       {queryAPI: "https://mixpanel.com/api/2.0/", exportAPI: "https://data.mixpanel.com/api/2.0/"},
		regionEU: {queryAPI: "https://eu.mixpanel.com/api/2.0/", exportAPI: "https://data-eu.mixpanel.com/api/2.0/"},
	} {
		c := Source{}.apiClient(Config{Region: region, Credentials: Credentials{OptionTitle: authProjectSecret, APISecret: "x"}})
		assert.Equal(t, want, c.roots, region)
	}
}

func TestReadAuthError(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, streamRevenue)
	before := len(h.list())
	out := read(t, src, testConfig(t, func(c *Config) { c.Credentials.Secret = "revoked" }), cat, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "invalid credentials")
	assert.Len(t, h.list()[before:], 1, "a 401 is not retried")
	require.NotEmpty(t, out.ErrorTraces())
	assert.Equal(t, synclet.FailureTypeConfigError, out.ErrorTraces()[0].FailureType)
}

func TestCohortMembersCursorPerCohort(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := testConfig(t)
	cat := catalogOf(t, src, cfg, synclet.SyncModeIncremental, streamCohortMembers)

	out := read(t, src, cfg, cat, stateOf(t, streamCohortMembers, `{"last_seen":"2026-10-15T00:00:00","cohorts":{"101":"2026-10-15T00:00:00","999":"2026-10-15T00:00:00"}}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []any{"user-001", "user-002"}, field(records(t, out, streamCohortMembers), "distinct_id"),
		"a cohort missing from the state is read in full")
	assert.Equal(t, []string{
		`{"last_seen":"2026-10-17T11:00:00","cohorts":{"101":"2026-10-17T11:00:00"}}`,
		`{"last_seen":"2026-10-12T08:15:00","cohorts":{"101":"2026-10-17T11:00:00","102":"2026-10-12T08:15:00"}}`,
	}, streamStates(out, streamCohortMembers), "a state after each cohort, without deleted cohorts")

	out = read(t, src, cfg, cat, stateOf(t, streamCohortMembers, `{"last_seen":"2026-10-15T00:00:00"}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []any{"user-001"}, field(records(t, out, streamCohortMembers), "distinct_id"),
		"a state without cohorts applies its last_seen to every cohort")
	states := streamStates(out, streamCohortMembers)
	require.Len(t, states, 2)
	assert.Equal(t, `{"last_seen":"2026-10-15T00:00:00","cohorts":{"101":"2026-10-17T11:00:00","102":"2026-10-15T00:00:00"}}`, states[1])
}
