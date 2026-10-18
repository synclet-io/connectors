package main

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestDiscover(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	got := discoverStreams(t, testSource(srv))
	var names []string
	for n := range got {
		names = append(names, n)
	}
	slices.Sort(names)
	assert.Equal(t, []string{
		"collections", "companies", "company_locations", "customers", "fulfillments", "inventory_items", "inventory_levels",
		"locations", "orders", "product_variants", "products", "refunds", "shop", "transactions",
	}, names)

	full := []synclet.SyncMode{synclet.SyncModeFullRefresh}
	both := []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}
	for _, n := range []string{"shop", "locations", "inventory_levels"} {
		assert.Equal(t, full, got[n].SupportedSyncModes, n)
		assert.Empty(t, got[n].DefaultCursorField, n)
	}
	for _, n := range []string{"products", "orders", "collections", "companies"} {
		assert.Equal(t, both, got[n].SupportedSyncModes, n)
		assert.Equal(t, []string{"updatedAt"}, got[n].DefaultCursorField, n)
		assert.True(t, got[n].SourceDefinedCursor, n)
	}
	for _, n := range []string{"refunds", "fulfillments", "transactions"} {
		assert.Equal(t, []string{"order_updated_at"}, got[n].DefaultCursorField, n)
	}
	for n, s := range got {
		assert.Equal(t, [][]string{{"id"}}, s.SourceDefinedPrimaryKey, n)
	}
	assert.Contains(t, string(got["orders"].JSONSchema), `"lineItems":{"type":["null","array"],"items":{"properties":{`)
	assert.Contains(t, string(got["refunds"].JSONSchema), `"order_id":{"type":"string"}`)
	assert.Contains(t, string(got["inventory_levels"].JSONSchema), `"location_id":{"type":"string"}`)
}

func runDiscover(t *testing.T, src Source) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(testConfig)})
	require.NoError(t, err)
	return out
}

func TestDiscoverWithoutPlus(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]http.HandlerFunc{"ShopPlan": jsonBody(http.StatusOK, `{"data":{"shop":{"plan":{"shopifyPlus":false}}}}`)})
	out := runDiscover(t, testSource(srv))
	require.NoError(t, out.Err)
	var names []string
	for _, s := range out.Catalog().Streams {
		names = append(names, s.Name)
	}
	assert.NotContains(t, names, "companies")
	assert.NotContains(t, names, "company_locations")
	assert.Contains(t, names, "orders")
	var warned bool
	for _, m := range out.Messages {
		if m.Type == synclet.MessageTypeLog && m.Log.Level == synclet.LogLevelWarn && strings.Contains(m.Log.Message, "companies and company_locations left out") {
			warned = true
		}
	}
	assert.True(t, warned, "no WARN about the left-out B2B streams")
}

func TestDiscoverFails(t *testing.T) {
	tests := []struct {
		name string
		body http.HandlerFunc
		want string
	}{
		{"internal error", jsonBody(http.StatusOK, `{"errors":[{"message":"Internal error","extensions":{"code":"INTERNAL_SERVER_ERROR"}}]}`), "ShopPlan: INTERNAL_SERVER_ERROR: Internal error"},
		{"unknown field", jsonBody(http.StatusOK, `{"errors":[{"message":"Field 'shopifyPlus' doesn't exist on type 'ShopPlan'","extensions":{"code":"undefinedField"}}]}`), "Field 'shopifyPlus' doesn't exist"},
		{"access denied", jsonBody(http.StatusOK, `{"errors":[{"message":"Access denied for plan field.","extensions":{"code":"ACCESS_DENIED"}}]}`), "access denied"},
		{"no plan", jsonBody(http.StatusOK, `{"data":{"shop":{"plan":null}}}`), "response without shop.plan.shopifyPlus"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, map[string]http.HandlerFunc{"ShopPlan": tt.body})
			out := runDiscover(t, testSource(srv))
			require.Error(t, out.Err)
			assert.Contains(t, out.Err.Error(), tt.want)
		})
	}
}

func TestDiscoverFailsOnAuth(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	_, err := src.Discover(t.Context(), Config{Shop: testShop, AccessToken: "wrong-admin-token"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access token is invalid or revoked")
}
