package main

import (
	"net/http"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
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

func TestDiscoverWithoutB2B(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]http.HandlerFunc{"ProbeCompanies": jsonBody(http.StatusOK,
		`{"errors":[{"message":"Access denied for companies field.","extensions":{"code":"ACCESS_DENIED"}}]}`)})
	got := discoverStreams(t, testSource(srv))
	assert.NotContains(t, got, "companies")
	assert.NotContains(t, got, "company_locations")
	assert.Contains(t, got, "orders")
}

func TestDiscoverFailsOnAuth(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]http.HandlerFunc{"ProbeCompanies": jsonBody(http.StatusInternalServerError, `{"errors":"oops"}`)})
	src := testSource(srv)
	_, err := src.Discover(t.Context(), Config{Shop: testShop, AccessToken: "wrong-admin-token"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access token is invalid or revoked")
}
