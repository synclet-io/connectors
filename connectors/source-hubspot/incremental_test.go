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

// searches returns the bodies of the search requests of h to path.
func searches(t *testing.T, h *hits, path string) []searchBody {
	t.Helper()
	var out []searchBody
	for _, r := range h.list() {
		if r.method == http.MethodPost && r.path == path {
			var b searchBody
			require.NoError(t, json.Unmarshal(r.body, &b))
			require.NotContains(t, string(r.body), `"after"`, "a search pages by hs_object_id, never by after")
			out = append(out, b)
		}
	}
	return out
}

// searchResult answers a search with total and the given records.
func searchResult(total int, recs ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"total":%d,"results":[%s]}`, total, strings.Join(recs, ","))
	}
}

func dealRecord(id, updated string) string {
	return `{"id":"` + id + `","properties":{"dealname":"Deal ` + id + `","amount":"10","hs_lastmodifieddate":"` + updated +
		`"},"createdAt":"2026-09-01T00:00:00Z","updatedAt":"` + updated + `","archived":false}`
}

func TestIncremental(t *testing.T) {
	const midMS = "1791568770000"

	t.Run("no state, two keyset pages", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "contacts"), nil)
		assert.Equal(t, []string{"101", "102", "103"}, recordIDs(t, out, "contacts"))
		assert.Equal(t, []string{`{"cursor":"2026-10-18T11:59:00.000Z"}`}, states(out))
		bodies := searches(t, h, "crm/v3/objects/contacts/search")
		require.Len(t, bodies, 2)
		for i, last := range []string{"0", "102"} {
			b := bodies[i]
			assert.Equal(t, startMS, b.filter("lastmodifieddate", "GTE"), "contacts search on lastmodifieddate")
			assert.Equal(t, toMS, b.filter("lastmodifieddate", "LT"))
			assert.Equal(t, last, b.filter("hs_object_id", "GT"))
			assert.Equal(t, []map[string]string{{"propertyName": "hs_object_id", "direction": "ASCENDING"}}, b.Sorts)
			assert.Equal(t, 200, b.Limit)
			assert.Contains(t, b.Properties, "email")
			assert.Contains(t, b.Properties, "lastmodifieddate")
		}
		rec := records(t, out, "contacts")[0]
		assert.Equal(t, "ada@example.com", rec["properties_email"])
		assert.Equal(t, "2026-10-02T09:00:00Z", rec["updatedAt"])
	})

	t.Run("saved cursor", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{
			"crm/v3/objects/contacts/search/1791158400000-0.json": respond(t, http.StatusOK, "crm/v3/objects/contacts/search/"+startMS+"-102.json"),
		})
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "contacts"), streamState("contacts", `{"cursor":"2026-10-05T00:00:00.000Z"}`))
		assert.Equal(t, []string{"103"}, recordIDs(t, out, "contacts"))
		bodies := searches(t, h, "crm/v3/objects/contacts/search")
		require.Len(t, bodies, 1)
		assert.Equal(t, "1791158400000", bodies[0].filter("lastmodifieddate", "GTE"))
		assert.Equal(t, []string{`{"cursor":"2026-10-18T11:59:00.000Z"}`}, states(out))
	})

	t.Run("total over the cap splits the window", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{
			"crm/v3/objects/deals/search/" + startMS + "-0.json": func(w http.ResponseWriter, r *http.Request) {
				var b searchBody
				require.NoError(t, json.NewDecoder(r.Body).Decode(&b))
				if b.filter("hs_lastmodifieddate", "LT") == toMS {
					searchResult(12000, dealRecord("399", "2026-10-02T00:00:00Z"))(w, r)
					return
				}
				searchResult(1, dealRecord("301", "2026-10-02T00:00:00Z"))(w, r)
			},
			"crm/v3/objects/deals/search/" + midMS + "-0.json": searchResult(1, dealRecord("302", "2026-10-12T00:00:00Z")),
		})
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "deals"), nil)
		assert.Equal(t, []string{"301", "302"}, recordIDs(t, out, "deals"), "the records of the oversized window are not emitted")
		var windows [][2]string
		for _, b := range searches(t, h, "crm/v3/objects/deals/search") {
			windows = append(windows, [2]string{b.filter("hs_lastmodifieddate", "GTE"), b.filter("hs_lastmodifieddate", "LT")})
		}
		assert.Equal(t, [][2]string{{startMS, toMS}, {startMS, midMS}, {midMS, toMS}}, windows)
		assert.Equal(t, []string{`{"cursor":"2026-10-09T17:59:30.000Z"}`, `{"cursor":"2026-10-18T11:59:00.000Z"}`}, states(out))
	})

	t.Run("total exactly at the cap reads the window whole", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{
			"crm/v3/objects/deals/search/" + startMS + "-0.json": searchResult(10000, dealRecord("301", "2026-10-02T00:00:00Z")),
		})
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "deals"), nil)
		assert.Equal(t, []string{"301"}, recordIDs(t, out, "deals"))
		assert.Len(t, searches(t, h, "crm/v3/objects/deals/search"), 1)
	})

	t.Run("1 ms window over the cap fails", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
			"crm/v3/objects/deals/search/1792324739999-0.json": searchResult(10001, dealRecord("301", "2026-10-18T11:58:59.999Z")),
		})
		src := testSource(srv)
		out := read(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "deals"), streamState("deals", `{"cursor":"2026-10-18T11:58:59.999Z"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "stream deals")
		assert.Contains(t, out.Err.Error(), "[2026-10-18T11:58:59.999Z, 2026-10-18T11:59:00.000Z)")
		assert.Contains(t, out.Err.Error(), "10001")
		assert.Empty(t, recordIDs(t, out, "deals"))
	})

	t.Run("full refresh of an object reads no search", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "deals"), nil)
		assert.Empty(t, searches(t, h, "crm/v3/objects/deals/search"))
	})
}
