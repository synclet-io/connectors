package main

import (
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// schemaOf returns a stream's JSON schema properties as raw JSON.
func schemaOf(t *testing.T, s synclet.Stream) map[string]json.RawMessage {
	t.Helper()
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	return schema.Properties
}

func TestDiscover(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	streams := discoverStreams(t, testSource(srv))

	var names []string
	for n := range streams {
		names = append(names, n)
	}
	slices.Sort(names)
	assert.Equal(t, []string{
		"associations_companies_companies", "associations_companies_deals", "associations_contacts_cars", "associations_contacts_companies",
		"cars", "companies", "contacts", "deal_pipelines", "deals", "line_items", "owners", "products",
		"ticket_pipelines", "tickets",
	}, names, "the archived custom object boats is left out")

	const (
		str      = `{"type":["null","string"]}`
		dateTime = `{"type":["null","string"],"format":"date-time"}`
	)
	want := map[string]string{
		"id":                          `{"type":"string"}`,
		"createdAt":                   dateTime,
		"updatedAt":                   dateTime,
		"archived":                    `{"type":["null","boolean"]}`,
		"properties_email":            str,
		"properties_firstname":        str,
		"properties_phone":            str,
		"properties_lifecyclestage":   str,
		"properties_hs_tags":          str,
		"properties_hs_custom_json":   str,
		"properties_hs_geo":           str,
		"properties_hs_future_field":  str,
		"properties_num_notes":        `{"type":["null","number"]}`,
		"properties_hs_is_unworked":   `{"type":["null","boolean"]}`,
		"properties_createdate":       dateTime,
		"properties_lastmodifieddate": dateTime,
		"properties_date_of_birth":    `{"type":["null","string"],"format":"date"}`,
	}
	got := schemaOf(t, streams["contacts"])
	require.Len(t, got, len(want))
	for name, w := range want {
		assert.JSONEq(t, w, string(got[name]), name)
	}

	cars := streams["cars"]
	carProps := schemaOf(t, cars)
	assert.JSONEq(t, `{"type":["null","number"]}`, string(carProps["properties_year"]))
	assert.JSONEq(t, `{"type":["null","string"],"format":"date"}`, string(carProps["properties_purchase_date"]))
	for _, s := range []synclet.Stream{streams["contacts"], cars} {
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, s.SupportedSyncModes)
		assert.True(t, s.SourceDefinedCursor)
		assert.Equal(t, []string{"updatedAt"}, s.DefaultCursorField)
		assert.Equal(t, [][]string{{"id"}}, s.SourceDefinedPrimaryKey)
	}

	assoc := streams["associations_contacts_cars"]
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, assoc.SupportedSyncModes)
	assert.Equal(t, [][]string{{"from_id"}, {"to_id"}, {"type_id"}}, assoc.SourceDefinedPrimaryKey)
	assert.JSONEq(t, `{"type":["null","string"]}`, string(schemaOf(t, assoc)["label"]))

	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, streams["owners"].SupportedSyncModes)

	lines := h.lines()
	assert.Contains(t, lines, "GET crm/v3/schemas")
	assert.Contains(t, lines, "GET crm/v3/properties/2-1001")
	assert.Contains(t, lines, "GET crm/v4/associations/contacts/2-1001/labels")
	assert.Contains(t, lines, "GET crm/v4/associations/tickets/products/labels", "pairs without labels are probed and left out")
	assert.Contains(t, lines, "GET crm/v4/associations/contacts/contacts/labels", "same-object pairs are probed too")
	assert.NotContains(t, lines, "GET crm/v3/properties/2-1002")
}
