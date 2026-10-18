package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestFullRefresh(t *testing.T) {
	t.Run("contacts", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeFullRefresh, "contacts")
		h0 := len(h.list())
		out := runRead(t, src, cat, nil)
		assert.Equal(t, []string{
			"GET crm/v3/objects/contacts?archived=false&limit=100",
			"POST crm/v3/objects/contacts/batch/read?archived=false",
			"GET crm/v3/objects/contacts?after=102&archived=false&limit=100",
			"POST crm/v3/objects/contacts/batch/read?archived=false",
		}, h.lines()[h0:])
		var b batchBody
		require.NoError(t, json.Unmarshal(h.list()[h0+1].body, &b))
		assert.Len(t, b.Properties, 13, "every declared property is requested")
		assert.Contains(t, b.Properties, "date_of_birth")
		require.Len(t, b.Inputs, 2)
		assert.Equal(t, "101", b.Inputs[0].ID)
		assert.Empty(t, states(out))

		recs := records(t, out, "contacts")
		require.Len(t, recs, 3)
		assert.Equal(t, map[string]any{
			"id": "101", "createdAt": "2026-09-01T08:00:00Z", "updatedAt": "2026-10-02T09:00:00Z", "archived": false,
			"properties_email": "ada@example.com", "properties_firstname": "Ada", "properties_phone": "+1 555 0101",
			"properties_lifecyclestage": "lead", "properties_hs_tags": "alpha;beta", "properties_hs_custom_json": `{"tier":1}`,
			"properties_hs_geo": "", "properties_num_notes": float64(3), "properties_hs_is_unworked": true,
			"properties_createdate": "2026-09-01T08:00:00Z", "properties_lastmodifieddate": "2026-10-02T09:00:00Z",
			"properties_date_of_birth": "1990-05-17", "properties_hs_future_field": "x",
		}, recs[0], "records come in list order, not batch order")
		assert.Equal(t, "102", recs[1]["id"])
		assert.Equal(t, 0.5, recs[1]["properties_num_notes"])
		assert.Equal(t, false, recs[1]["properties_hs_is_unworked"])
		assert.Equal(t, "1985-12-09", recs[1]["properties_date_of_birth"], "a midnight datetime becomes a date")
		assert.Equal(t, "", recs[1]["properties_phone"], `"" stays a string`)
		assert.Nil(t, recs[1]["properties_hs_custom_json"])
		assert.Nil(t, recs[2]["properties_num_notes"], `"" is null for a number`)
		assert.Nil(t, recs[2]["properties_hs_is_unworked"])
		assert.Equal(t, "2026-09-03T10:00:00Z", recs[2]["properties_createdate"], "epoch milliseconds become RFC 3339")
		assert.Equal(t, "1984-12-10", recs[2]["properties_date_of_birth"])
	})

	t.Run("numbers are emitted verbatim", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "companies"), nil)
		recs := out.Records(synclet.StreamDescriptor{Name: "companies"})
		require.Len(t, recs, 2)
		assert.Contains(t, string(recs[0].Data), `"properties_annualrevenue":1200000`)
	})

	t.Run("unparseable value fails the read", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
			"crm/v3/objects/contacts/batch/read/103.json": func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"status":"COMPLETE","results":[{"id":"103","properties":{"num_notes":"lots"},"createdAt":"2026-09-03T08:00:00Z","updatedAt":"2026-10-04T09:00:00Z","archived":false}]}`))
			},
		})
		src := testSource(srv)
		out := read(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "contacts"), nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "stream contacts")
		assert.Contains(t, out.Err.Error(), "record 103")
		assert.Contains(t, out.Err.Error(), "property num_notes")
		assert.Contains(t, out.Err.Error(), `"lots"`)
	})

	t.Run("records missing from a batch are logged", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
			"crm/v3/objects/contacts/batch/read/103.json": func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusMultiStatus)
				_, _ = w.Write([]byte(`{"status":"COMPLETE","results":[],"numErrors":1,"errors":[{"status":"error","category":"OBJECT_NOT_FOUND","message":"Could not get some CONTACT objects, they may be deleted or not exist.","context":{"ids":["103"]}}]}`))
			},
		})
		src := testSource(srv)
		out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "contacts"), nil)
		assert.Equal(t, []string{"101", "102"}, recordIDs(t, out, "contacts"))
		var warned bool
		for _, l := range out.Logs() {
			if l.Level == synclet.LogLevelWarn && strings.Contains(l.Message, "missing=1") && strings.Contains(l.Message, "stream=contacts") {
				warned = true
			}
		}
		assert.True(t, warned, "a WARN log names the stream and the count: %v", out.Logs())
	})

	t.Run("another batch error fails the read", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
			"crm/v3/objects/contacts/batch/read/103.json": func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusMultiStatus)
				_, _ = w.Write([]byte(`{"status":"COMPLETE","results":[],"errors":[{"status":"error","category":"VALIDATION_ERROR","message":"Property values were not valid"}]}`))
			},
		})
		src := testSource(srv)
		out := read(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "contacts"), nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "VALIDATION_ERROR: Property values were not valid")
	})

	t.Run("owners and pipelines", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeFullRefresh, "owners", "deal_pipelines", "ticket_pipelines")
		h0 := len(h.list())
		out := runRead(t, src, cat, nil)
		assert.Equal(t, []string{
			"GET crm/v3/owners?archived=false&limit=100",
			"GET crm/v3/pipelines/deals",
			"GET crm/v3/pipelines/tickets",
		}, h.lines()[h0:])
		assert.Equal(t, []string{"7001", "7002"}, recordIDs(t, out, "owners"))
		owner := records(t, out, "owners")[0]
		assert.Equal(t, "owner.one@example.com", owner["email"])
		assert.Equal(t, float64(8001), owner["userId"])
		assert.Len(t, owner["teams"], 1)
		assert.Equal(t, []string{"default"}, recordIDs(t, out, "deal_pipelines"))
		assert.Equal(t, []string{"default"}, recordIDs(t, out, "ticket_pipelines"))
		assert.Len(t, records(t, out, "deal_pipelines")[0]["stages"], 2)
	})

	t.Run("custom object", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeFullRefresh, "cars")
		h0 := len(h.list())
		out := runRead(t, src, cat, nil)
		assert.Equal(t, []string{
			"GET crm/v3/schemas",
			"GET crm/v3/objects/2-1001?archived=false&limit=100",
			"POST crm/v3/objects/2-1001/batch/read?archived=false",
		}, h.lines()[h0:])
		rec := records(t, out, "cars")[0]
		assert.Equal(t, float64(2024), rec["properties_year"])
		assert.Equal(t, "2026-03-01", rec["properties_purchase_date"])
	})

	t.Run("unknown stream", func(t *testing.T) {
		srv, _ := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeFullRefresh, "deals")
		cat.Streams[0].Stream.Name = "boats"
		out := read(t, src, cat, nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `unknown stream "boats"`)
	})
}
