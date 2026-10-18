package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// searchCollections answers Collections pages from recs, applying the
// updated_at filter of the request as Shopify would, and records each
// filter.
func searchCollections(t *testing.T, filters *[]string, recs map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var b gqlBody
		require.NoError(t, json.NewDecoder(r.Body).Decode(&b))
		q, _ := b.Variables["query"].(string)
		*filters = append(*filters, q)
		m := windowPattern.FindStringSubmatch(q)
		require.NotNil(t, m, q)
		var nodes []string
		for id, updated := range recs {
			if updated >= m[1] && (m[2] == "" || updated < m[2]) {
				nodes = append(nodes, fmt.Sprintf(`{"id":"gid://shopify/Collection/%s","updatedAt":"%s"}`, id, updated))
			}
		}
		jsonBody(http.StatusOK, `{"data":{"collections":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[`+strings.Join(nodes, ",")+`]}}}`)(w, r)
	}
}

// TestFullRefreshLastWindowOpen reads a full refresh in two windows.
// Collection 2 was updated in the second window when the read started and
// is updated again while the first window is read, past the read's end; the
// open last window still returns it.
func TestFullRefreshLastWindowOpen(t *testing.T) {
	var filters []string
	srv, h := fixtureServer(t, map[string]http.HandlerFunc{
		"Collections": searchCollections(t, &filters, map[string]string{
			"1": "2026-10-03T00:00:00Z",
			"2": "2026-10-18T11:59:30Z",
		}),
		"BulkRun-orders-20261011T000000Z": bulkRun("2004"),
		"BulkPoll-2004":                   poll("2004", "COMPLETED"),
	})
	src := testSource(srv)
	cfg := `{"shop":"example","access_token":"` + testToken + `","start_date":"2026-10-01","window_days":10}`
	out := readWith(t, src, cfg, catalogOf(t, src, synclet.SyncModeFullRefresh, "collections", "orders"), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		"updated_at:>='2026-10-01T00:00:00Z' AND updated_at:<'2026-10-11T00:00:00Z'",
		"updated_at:>='2026-10-11T00:00:00Z'",
	}, filters)
	assert.Equal(t, []string{"gid://shopify/Collection/1", "gid://shopify/Collection/2"}, recordIDs(t, out, "collections"))
	runs := h.ops("BulkRun")
	require.Len(t, runs, 2)
	assert.Contains(t, runs[1].vars["query"], `orders(query: "updated_at:>='2026-10-11T00:00:00Z'")`)
	assert.Empty(t, states(out))
}

func TestSearchWarnings(t *testing.T) {
	warned := `{"data":{"probe":{"nodes":[]}},"extensions":{"search":[{"path":["probe"],"query":"updated_at:>='2026-10-01T00:00:00Z'",` +
		`"warnings":[{"field":"updated_at","message":"Invalid search field for this query."}]}]}}`

	t.Run("bulk stream", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{"SearchProbe": jsonBody(http.StatusOK, warned)})
		src := testSource(srv)
		out := read(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "orders"), nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream orders: probe the updated_at filter: SearchProbe: Shopify did not apply the filter: search query "updated_at:>='2026-10-01T00:00:00Z'": updated_at: Invalid search field for this query.`)
		assert.Empty(t, h.ops("BulkRun"))
		assert.Empty(t, states(out))
	})

	t.Run("paged stream", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{"Collections": jsonBody(http.StatusOK,
			`{"data":{"collections":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[{"id":"gid://shopify/Collection/1"}]}},`+
				`"extensions":{"search":[{"path":["collections"],"query":"x","warnings":[{"field":"updated_at","message":"Invalid search field for this query."}]}]}}`)})
		src := testSource(srv)
		out := read(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "collections"), nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "Collections: Shopify did not apply the filter")
		assert.Empty(t, records(out, "collections"))
		assert.Empty(t, states(out))
	})
}

func TestAllOrders(t *testing.T) {
	without := jsonBody(http.StatusOK, `{"data":{"currentAppInstallation":{"accessScopes":[{"handle":"read_orders"},{"handle":"read_products"}]}}}`)
	with := jsonBody(http.StatusOK, `{"data":{"currentAppInstallation":{"accessScopes":[{"handle":"read_orders"},{"handle":"read_all_orders"}]}}}`)
	old := `{"shop":"example","access_token":"` + testToken + `","start_date":"2026-01-01","window_days":365}`

	t.Run("check fails with an old start", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
			"CheckShop": jsonBody(http.StatusOK, scopesBody("read_products", "read_customers", "read_orders", "read_inventory", "read_locations")),
		})
		st, _ := runCheck(t, testSource(srv), Config{Shop: testShop, AccessToken: testToken, StartDate: "2026-01-01"})
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "the app lacks read_all_orders, so Shopify returns only the orders of the last 60 days, but the read starts at 2026-01-01T00:00:00Z")
	})

	t.Run("check fails for an old shop without start_date", func(t *testing.T) {
		body := strings.Replace(scopesBody("read_products", "read_customers", "read_orders", "read_inventory", "read_locations"), "2026-09-20T08:15:30Z", "2025-03-01T00:00:00Z", 1)
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{"CheckShop": jsonBody(http.StatusOK, body)})
		st, _ := runCheck(t, testSource(srv), Config{Shop: testShop, AccessToken: testToken})
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "lacks read_all_orders")
	})

	for _, stream := range []string{"orders", "refunds", "fulfillments", "transactions"} {
		t.Run("read of "+stream+" fails with an old start", func(t *testing.T) {
			srv, h := fixtureServer(t, map[string]http.HandlerFunc{"AccessScopes": without})
			src := testSource(srv)
			out := readWith(t, src, old, catalogOf(t, src, synclet.SyncModeIncremental, stream), nil)
			require.Error(t, out.Err)
			assert.Contains(t, out.Err.Error(), "stream "+stream+": the app lacks read_all_orders")
			assert.Empty(t, h.ops("BulkRun"))
			assert.Empty(t, states(out))
		})
	}

	t.Run("read from a recent cursor needs no read_all_orders", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{"AccessScopes": without})
		src := testSource(srv)
		out := readWith(t, src, old, catalogOf(t, src, synclet.SyncModeIncremental, "orders"), streamState("orders", `{"cursor":"2026-10-01T00:00:00Z"}`))
		require.NoError(t, out.Err)
		assert.Empty(t, h.ops("AccessScopes"))
		assert.Len(t, records(out, "orders"), 2)
	})

	t.Run("read with read_all_orders", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
			"AccessScopes":                    with,
			"BulkRun-orders-20260101T000000Z": bulkRun("1004"),
		})
		src := testSource(srv)
		out := readWith(t, src, old, catalogOf(t, src, synclet.SyncModeIncremental, "orders"), nil)
		require.NoError(t, out.Err)
		assert.Len(t, records(out, "orders"), 2)
	})

	t.Run("other streams need no read_all_orders", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{"BulkRun-products-20260101T000000Z": bulkRun("1001")})
		src := testSource(srv)
		out := readWith(t, src, old, catalogOf(t, src, synclet.SyncModeIncremental, "products"), nil)
		require.NoError(t, out.Err)
		assert.Empty(t, h.ops("AccessScopes"))
	})
}
