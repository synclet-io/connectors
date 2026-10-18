package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const testConfig = `{"api_key":"` + testKey + `","project_id":"` + testProj + `","environment":"production"}`

const cust = "projects/" + testProj + "/customers/"

func allStreams() []string {
	var names []string
	for _, d := range streams {
		names = append(names, d.name)
	}
	return names
}

// discoverStreams returns the discovered streams by name.
func discoverStreams(t *testing.T, src Source) map[string]synclet.Stream {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(testConfig)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	streams := map[string]synclet.Stream{}
	for _, s := range out.Catalog().Streams {
		streams[s.Name] = s
	}
	return streams
}

func catalogOf(t *testing.T, src Source, mode synclet.SyncMode, names ...string) *synclet.ConfiguredCatalog {
	t.Helper()
	streams := discoverStreams(t, src)
	cat := &synclet.ConfiguredCatalog{}
	for _, n := range names {
		cat.Streams = append(cat.Streams, synclet.ConfiguredStream{
			Stream: streams[n], SyncMode: mode, DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
		})
	}
	return cat
}

func read(t *testing.T, src Source, config string, catalog *synclet.ConfiguredCatalog) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandRead, Config: json.RawMessage(config), Catalog: raw})
	require.NoError(t, err)
	return out
}

// field returns the string values of name in the records of stream.
func field(t *testing.T, out *testkit.Output, stream, name string) []string {
	t.Helper()
	var vals []string
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(r.Data, &rec))
		v, _ := rec[name].(string)
		vals = append(vals, v)
	}
	return vals
}

func TestReadAllStreamsPages(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, allStreams()...))
	require.NoError(t, out.Err)

	want := map[string][]string{
		streamProjects:                   {"proj_test1", "proj_test2"},
		streamApps:                       {"app_test_ios", "app_test_android"},
		streamProducts:                   {"prod_monthly_test", "prod_lifetime_test"},
		streamEntitlements:               {"entl_test_premium", "entl_test_pro"},
		streamOfferings:                  {"ofrng_test_default", "ofrng_test_promo"},
		streamCustomers:                  {"cust_test_001", "cust_test_002", "cust_test_003"},
		streamCustomerSubscriptions:      {"sub_test_001a", "sub_test_001b", "sub_test_002a", "sub_test_003a"},
		streamCustomerPurchases:          {"purch_test_001a", "purch_test_001b", "purch_test_002a"},
		streamMetricsOverview:            {"active_trials", "mrr"},
		streamCustomerActiveEntitlements: nil,
	}
	for stream, ids := range want {
		if ids != nil {
			assert.Equal(t, ids, field(t, out, stream, "id"), stream)
		}
	}
	assert.Equal(t, []string{"entl_test_premium", "entl_test_pro", "entl_test_premium"}, field(t, out, streamCustomerActiveEntitlements, "entitlement_id"))
	assert.Equal(t, []string{"cust_test_001", "cust_test_001", "cust_test_002"}, field(t, out, streamCustomerActiveEntitlements, "customer_id"))
	assert.Equal(t, []string{"cust_test_001", "cust_test_001", "cust_test_002", "cust_test_003"}, field(t, out, streamCustomerSubscriptions, "customer_id"))
	assert.Equal(t, []string{"USD", "USD"}, field(t, out, streamMetricsOverview, "currency"))

	// Records keep every field the API sent, also undeclared ones.
	sub := out.Records(synclet.StreamDescriptor{Name: streamCustomerSubscriptions})[0]
	assert.Contains(t, string(sub.Data), `"total_revenue_in_usd":{"currency":"USD","gross":10.0`)

	p := "projects/" + testProj + "/"
	assert.Equal(t, []string{
		"projects?limit=100",
		"projects?limit=100&starting_after=proj_test1",
		p + "apps?limit=100",
		p + "apps?limit=100&starting_after=app_test_ios",
		p + "products?limit=100",
		p + "products?limit=100&starting_after=prod_monthly_test",
		p + "entitlements?limit=100",
		p + "entitlements?limit=100&starting_after=entl_test_premium",
		p + "offerings?limit=100",
		p + "offerings?limit=100&starting_after=ofrng_test_default",
		p + "customers?limit=100",
		cust + "cust_test_001/subscriptions?environment=production&limit=100",
		cust + "cust_test_001/subscriptions?environment=production&limit=100&starting_after=sub_test_001a",
		cust + "cust_test_001/purchases?environment=production&limit=100",
		cust + "cust_test_001/purchases?environment=production&limit=100&starting_after=purch_test_001a",
		cust + "cust_test_001/active_entitlements?limit=100",
		cust + "cust_test_001/active_entitlements?limit=100&starting_after=entl_test_premium",
		cust + "cust_test_002/subscriptions?environment=production&limit=100",
		cust + "cust_test_002/purchases?environment=production&limit=100",
		cust + "cust_test_002/active_entitlements?limit=100",
		p + "customers?limit=100&starting_after=cust_test_002",
		cust + "cust_test_003/subscriptions?environment=production&limit=100",
		cust + "cust_test_003/purchases?environment=production&limit=100",
		cust + "cust_test_003/active_entitlements?limit=100",
		p + "metrics/overview",
	}, h.list())
}

func TestReadChildStreamsWithoutCustomers(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	cfg := `{"api_key":"` + testKey + `","project_id":"` + testProj + `"}`
	out := read(t, src, cfg, catalogOf(t, src, synclet.SyncModeFullRefresh, streamCustomerPurchases))
	require.NoError(t, out.Err)
	assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: streamCustomers}))
	assert.Equal(t, []string{"purch_test_001a", "purch_test_001b", "purch_test_002a"}, field(t, out, streamCustomerPurchases, "id"))
	for _, r := range h.list() {
		assert.NotContains(t, r, "environment", "no environment configured")
		assert.NotContains(t, r, "subscriptions")
	}
}

func TestReadResolvesTheOnlyProject(t *testing.T) {
	srv, h := fixtureServer(t, map[string]fixture{"projects": {status: http.StatusOK, file: "single_project.json"}})
	src := testSource(srv, newClock())
	out := read(t, src, `{"api_key":"`+testKey+`"}`, catalogOf(t, src, synclet.SyncModeFullRefresh, streamMetricsOverview))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"projects?limit=100", "projects/" + testProj + "/metrics/overview"}, h.list())
}

func TestReadFailures(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		overrides map[string]fixture
		streams   []string
		want      string
	}{
		{
			name:    "several projects without project_id",
			config:  `{"api_key":"` + testKey + `"}`,
			streams: []string{streamApps},
			want:    "project_id is empty and the key can see 2 projects (proj_test1, proj_test2)",
		},
		{
			name:    "unknown project",
			config:  `{"api_key":"` + testKey + `","project_id":"proj_nosuch"}`,
			streams: []string{streamCustomers},
			want:    `unknown project "proj_nosuch"`,
		},
		{
			name:      "missing permission",
			config:    testConfig,
			overrides: map[string]fixture{cust + "cust_test_001/purchases": {status: http.StatusForbidden, file: "errors/forbidden.json"}},
			streams:   []string{streamCustomers, streamCustomerPurchases},
			want:      "stream customer_purchases: key lacks permission customer_information:purchases:read",
		},
		{
			name:      "child record of another customer",
			config:    testConfig,
			overrides: map[string]fixture{cust + "cust_test_002/subscriptions": {status: http.StatusOK, file: "projects/proj_test1/customers/cust_test_003/subscriptions.json"}},
			streams:   []string{streamCustomerSubscriptions},
			want:      `customer_subscriptions of customer "cust_test_002": record has customer_id "cust_test_003"`,
		},
		{
			name:      "child server error",
			config:    testConfig,
			overrides: map[string]fixture{cust + "cust_test_002/purchases": {status: http.StatusBadRequest, file: "errors/resource_missing.json"}},
			streams:   []string{streamCustomerPurchases},
			want:      "HTTP 400",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, tt.overrides)
			src := testSource(srv, newClock())
			out := read(t, src, tt.config, catalogOf(t, src, synclet.SyncModeFullRefresh, tt.streams...))
			require.Error(t, out.Err)
			assert.Contains(t, out.Err.Error(), tt.want)
		})
	}
}

func TestReadRejectsIncremental(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv, newClock())
	out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeIncremental, streamCustomers))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "full refresh only")
	assert.Empty(t, h.list())
}

func TestReadNextPageWithoutCursor(t *testing.T) {
	for name, body := range map[string]string{
		"no starting_after": `{"object":"list","items":[{"id":"app_a"}],"next_page":"/v2/projects/proj_test1/apps?limit=100"}`,
		"repeated":          `{"object":"list","items":[{"id":"app_a"}],"next_page":"/v2/projects/proj_test1/apps?starting_after=app_a"}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptestServer(t, body)
			src := testSource(srv, newClock())
			out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh, streamApps))
			require.Error(t, out.Err)
			assert.True(t, strings.Contains(out.Err.Error(), "has no starting_after") || strings.Contains(out.Err.Error(), "repeats starting_after"), out.Err.Error())
		})
	}
}

func TestCustomerDeleted(t *testing.T) {
	srv, h := fixtureServer(t, map[string]fixture{
		cust + "cust_test_002/purchases": {status: http.StatusNotFound, file: "errors/resource_missing.json"},
	})
	src := testSource(srv, newClock())
	out := read(t, src, testConfig, catalogOf(t, src, synclet.SyncModeFullRefresh,
		streamCustomers, streamCustomerSubscriptions, streamCustomerPurchases, streamCustomerActiveEntitlements))
	require.NoError(t, out.Err)

	var warns []string
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn {
			warns = append(warns, l.Message)
		}
	}
	require.Len(t, warns, 1)
	assert.Contains(t, warns[0], `customer "cust_test_002" was not found when reading customer_purchases`)

	assert.Equal(t, []string{"cust_test_001", "cust_test_002", "cust_test_003"}, field(t, out, streamCustomers, "id"))
	assert.Equal(t, []string{"purch_test_001a", "purch_test_001b"}, field(t, out, streamCustomerPurchases, "id"))
	assert.Equal(t, []string{"sub_test_001a", "sub_test_001b", "sub_test_002a", "sub_test_003a"}, field(t, out, streamCustomerSubscriptions, "id"))
	assert.Equal(t, []string{"cust_test_001", "cust_test_001"}, field(t, out, streamCustomerActiveEntitlements, "customer_id"))
	assert.NotContains(t, h.list(), cust+"cust_test_002/active_entitlements?limit=100")
}
