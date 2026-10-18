package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const emptyCollections = `{"data":{"collections":{"pageInfo":{"hasNextPage":false,"endCursor":null},"nodes":[]}}}`

// bulkRun answers a BulkRun with operation id.
func bulkRun(id string) http.HandlerFunc {
	return jsonBody(http.StatusOK, `{"data":{"bulkOperationRunQuery":{"bulkOperation":{"id":"gid://shopify/BulkOperation/`+id+`","status":"CREATED"},"userErrors":[]}}}`)
}

func TestIncremental(t *testing.T) {
	const after = "updated_at:>='2026-10-10T00:00:00Z' AND updated_at:<'2026-10-18T11:59:00Z'"

	t.Run("bulk from state", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{
			"BulkRun-orders-20261010T000000Z":  bulkRun("2004"),
			"BulkRun-refunds-20261010T000000Z": bulkRun("2005"),
			"BulkPoll-2004":                    poll("2004", "COMPLETED"),
			"BulkPoll-2005":                    poll("2005", "COMPLETED"),
		})
		src := testSource(srv)
		state := `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"orders"},"stream_state":{"cursor":"2026-10-10T00:00:00Z"}}},` +
			`{"type":"STREAM","stream":{"stream_descriptor":{"name":"refunds"},"stream_state":{"cursor":"2026-10-10T00:00:00Z"}}}]`
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "orders", "refunds"), []byte(state))
		runs := h.ops("BulkRun")
		require.Len(t, runs, 2)
		for _, r := range runs {
			assert.Contains(t, r.vars["query"], `(query: "`+after+`")`)
		}
		orders, _ := findStream("orders")
		w := window{from: time.Date(2026, 10, 10, 0, 0, 0, 0, time.UTC), to: time.Date(2026, 10, 18, 11, 59, 0, 0, time.UTC)}
		assert.Equal(t, orders.bulkQuery(&w), runs[0].vars["query"])
		assert.Equal(t, []string{`{"cursor":"2026-10-18T11:59:00Z"}`, `{"cursor":"2026-10-18T11:59:00Z"}`}, states(out))
	})

	t.Run("paged from state", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{"Collections": jsonBody(http.StatusOK, emptyCollections)})
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "collections"), streamState("collections", `{"cursor":"2026-10-10T00:00:00Z"}`))
		pages := h.ops("Collections")
		require.Len(t, pages, 1)
		assert.Equal(t, after, pages[0].vars["query"])
		assert.Contains(t, pages[0].doc, "collections(first: $first, after: $after, query: $query)")
		assert.Equal(t, []string{`{"cursor":"2026-10-18T11:59:00Z"}`}, states(out))
	})

	t.Run("state after each window", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{"Collections": jsonBody(http.StatusOK, emptyCollections)})
		src := testSource(srv)
		cfg := `{"shop":"example","access_token":"` + testToken + `","start_date":"2026-10-01","window_days":7}`
		out := readWith(t, src, cfg, catalogOf(t, src, synclet.SyncModeIncremental, "collections"), nil)
		require.NoError(t, out.Err)
		var filters []any
		for _, p := range h.ops("Collections") {
			filters = append(filters, p.vars["query"])
		}
		assert.Equal(t, []any{
			"updated_at:>='2026-10-01T00:00:00Z' AND updated_at:<'2026-10-08T00:00:00Z'",
			"updated_at:>='2026-10-08T00:00:00Z' AND updated_at:<'2026-10-15T00:00:00Z'",
			"updated_at:>='2026-10-15T00:00:00Z' AND updated_at:<'2026-10-18T11:59:00Z'",
		}, filters)
		assert.Equal(t, []string{
			`{"cursor":"2026-10-08T00:00:00Z"}`, `{"cursor":"2026-10-15T00:00:00Z"}`, `{"cursor":"2026-10-18T11:59:00Z"}`,
		}, states(out))
	})

	t.Run("cursor at the end reads nothing", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		catalog := catalogOf(t, src, synclet.SyncModeIncremental, "orders")
		out := runRead(t, src, catalog, streamState("orders", `{"cursor":"2026-10-18T11:59:00Z"}`))
		assert.Empty(t, h.ops("BulkRun"))
		assert.Empty(t, records(out, "orders"))
		assert.Equal(t, []string{`{"cursor":"2026-10-18T11:59:00Z"}`}, states(out))
	})

	t.Run("full refresh saves no state", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "orders", "collections"), streamState("orders", `{"cursor":"2026-10-10T00:00:00Z"}`))
		assert.Len(t, records(out, "orders"), 2)
		assert.Empty(t, states(out))
	})

	t.Run("bad state cursor", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		out := read(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "orders"), streamState("orders", `{"cursor":"yesterday"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `state cursor "yesterday" is not an RFC 3339 time`)
	})
}
