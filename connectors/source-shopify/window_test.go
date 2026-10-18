package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// collectionStore serves Collections pages of one node in id order,
// applying the updated_at filter as Shopify would. touch runs after the
// n-th page is served, to update records during the read.
type collectionStore struct {
	t       *testing.T
	mu      sync.Mutex
	updated map[string]string
	served  int
	touch   func(n int, updated map[string]string)
	filters []string
	docs    []string
}

func (s *collectionStore) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b gqlBody
	require.NoError(s.t, json.NewDecoder(r.Body).Decode(&b))
	q, _ := b.Variables["query"].(string)
	after, _ := b.Variables["after"].(string)
	s.filters, s.docs = append(s.filters, q), append(s.docs, b.Query)
	m := windowPattern.FindStringSubmatch(q)
	require.NotNil(s.t, m, q)
	var ids []string
	for id, updated := range s.updated {
		if id > after && updated >= m[1] && (m[2] == "" || updated < m[2]) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	var nodes, cursor string
	if len(ids) > 0 {
		nodes = fmt.Sprintf(`{"id":"gid://shopify/Collection/%s","updatedAt":"%s"}`, ids[0], s.updated[ids[0]])
		cursor = `"` + ids[0] + `"`
	} else {
		cursor = "null"
	}
	jsonBody(http.StatusOK, fmt.Sprintf(`{"data":{"collections":{"pageInfo":{"hasNextPage":%t,"endCursor":%s},"nodes":[%s]}}}`,
		len(ids) > 1, cursor, nodes))(w, r)
	s.served++
	s.touch(s.served, s.updated)
}

// TestFullRefreshOneRun reads a full refresh with window_days 10. After
// the first page, collection 1, already read, and collection 3, not read
// yet, are updated past the read's end; each is emitted exactly once.
func TestFullRefreshOneRun(t *testing.T) {
	store := &collectionStore{t: t, updated: map[string]string{
		"1": "2026-10-03T00:00:00Z",
		"2": "2026-10-05T00:00:00Z",
		"3": "2026-10-12T00:00:00Z",
	}, touch: func(n int, updated map[string]string) {
		if n == 1 {
			updated["1"] = "2026-10-18T11:59:30Z"
			updated["3"] = "2026-10-18T11:59:40Z"
		}
	}}
	overrides := map[string]http.HandlerFunc{}
	for _, key := range []string{"Collections", "Collections-1", "Collections-2", "Collections-3"} {
		overrides[key] = store.handler
	}
	srv, h := fixtureServer(t, overrides)
	src := testSource(srv)
	cfg := `{"shop":"example","access_token":"` + testToken + `","start_date":"2026-10-01","window_days":10}`
	out := readWith(t, src, cfg, catalogOf(t, src, synclet.SyncModeFullRefresh, "collections", "orders"), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"gid://shopify/Collection/1", "gid://shopify/Collection/2", "gid://shopify/Collection/3"}, recordIDs(t, out, "collections"))
	for i, f := range store.filters {
		assert.Equal(t, "updated_at:>='2026-10-01T00:00:00Z'", f)
		assert.Contains(t, store.docs[i], "sortKey: ID")
	}
	runs := h.ops("BulkRun")
	require.Len(t, runs, 1, "one bulk operation per stream")
	assert.Contains(t, runs[0].vars["query"], `orders(query: "updated_at:>='2026-10-01T00:00:00Z'")`)
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
		assert.Contains(t, st.Message, "the app lacks read_all_orders, so Shopify returns only the orders created in the last 60 days and skips updates to older ones, but the read starts at 2026-01-01T00:00:00Z")
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

	t.Run("read from a recent cursor fails with an old start", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{"AccessScopes": without})
		src := testSource(srv)
		out := readWith(t, src, old, catalogOf(t, src, synclet.SyncModeIncremental, "orders"), streamState("orders", `{"cursor":"2026-10-01T00:00:00Z"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "stream orders: the app lacks read_all_orders")
		assert.Empty(t, h.ops("BulkRun"))
	})

	t.Run("read with a recent start needs no read_all_orders", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{"AccessScopes": without})
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "orders"), nil)
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
