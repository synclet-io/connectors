package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestAssociations(t *testing.T) {
	t.Run("labels, custom objects and per-id paging", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		cat := catalogOf(t, src, synclet.SyncModeFullRefresh, "associations_contacts_companies", "associations_contacts_cars")
		h0 := len(h.list())
		out := runRead(t, src, cat, nil)
		assert.Equal(t, []map[string]any{
			{"from_id": "101", "to_id": "201", "type_id": float64(1), "category": "HUBSPOT_DEFINED", "label": nil},
			{"from_id": "101", "to_id": "201", "type_id": float64(28), "category": "USER_DEFINED", "label": "Billing contact"},
			{"from_id": "102", "to_id": "201", "type_id": float64(1), "category": "HUBSPOT_DEFINED", "label": nil},
			{"from_id": "102", "to_id": "202", "type_id": float64(28), "category": "USER_DEFINED", "label": "Billing contact"},
		}, records(t, out, "associations_contacts_companies"))
		assert.Equal(t, []map[string]any{
			{"from_id": "101", "to_id": "9001", "type_id": float64(55), "category": "USER_DEFINED", "label": "Owner"},
		}, records(t, out, "associations_contacts_cars"))
		assert.Equal(t, []string{
			"GET crm/v3/objects/contacts?archived=false&limit=100",
			"POST crm/v4/associations/contacts/companies/batch/read",
			"GET crm/v4/objects/contacts/102/associations/companies?after=MjAx&limit=500",
			"GET crm/v3/objects/contacts?after=102&archived=false&limit=100",
			"POST crm/v4/associations/contacts/companies/batch/read",
			"GET crm/v3/schemas",
			"GET crm/v3/objects/contacts?archived=false&limit=100",
			"POST crm/v4/associations/contacts/2-1001/batch/read",
			"GET crm/v3/objects/contacts?after=102&archived=false&limit=100",
			"POST crm/v4/associations/contacts/2-1001/batch/read",
		}, h.lines()[h0:])
	})

	t.Run("an unknown batch error fails the read", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
			"crm/v4/associations/contacts/companies/batch/read/103.json": respond(t, http.StatusMultiStatus, "errors/association_batch_error.json"),
		})
		src := testSource(srv)
		out := read(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh, "associations_contacts_companies"), nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "associations_contacts_companies")
		assert.Contains(t, out.Err.Error(), "crm.associations.READ_LIMIT_EXCEEDED")
		assert.Contains(t, out.Err.Error(), "Too many associations requested.")
	})
}
