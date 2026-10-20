package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

type schemaProp struct {
	Type        synclet.PropTypes       `json:"type"`
	Format      string                  `json:"format"`
	AirbyteType synclet.AirbytePropType `json:"airbyte_type"`
}

func schemaOf(t *testing.T, s synclet.Stream) map[string]schemaProp {
	t.Helper()
	var doc struct {
		AdditionalProperties bool                  `json:"additionalProperties"`
		Properties           map[string]schemaProp `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &doc), s.Name)
	assert.True(t, doc.AdditionalProperties, s.Name)
	return doc.Properties
}

func TestDiscover(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	got := discoverStreams(t, testSource(srv, newClock()), testConfig(t))
	assert.Equal(t, []string{"query events/properties/top?limit=1000", "query engage/properties"}, h.list())

	both := []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}
	want := map[string]struct {
		pk     [][]string
		cursor []string
		modes  []synclet.SyncMode
	}{
		streamExport:        {nil, []string{"time"}, both},
		streamEngage:        {[][]string{{"distinct_id"}}, []string{"last_seen"}, both},
		streamCohorts:       {[][]string{{"id"}}, []string{"created"}, both},
		streamCohortMembers: {[][]string{{"cohort_id"}, {"distinct_id"}}, []string{"last_seen"}, both},
		streamAnnotations:   {[][]string{{"id"}}, nil, []synclet.SyncMode{synclet.SyncModeFullRefresh}},
		streamFunnels:       {[][]string{{"funnel_id"}, {"date"}}, []string{"date"}, both},
		streamRevenue:       {[][]string{{"date"}}, []string{"date"}, both},
	}
	require.Len(t, got, len(want))
	for name, w := range want {
		s := got[name]
		assert.Equal(t, w.pk, s.SourceDefinedPrimaryKey, name)
		assert.Equal(t, w.cursor, s.DefaultCursorField, name)
		assert.Equal(t, w.cursor != nil, s.SourceDefinedCursor, name)
		assert.Equal(t, w.modes, s.SupportedSyncModes, name)
		props := schemaOf(t, s)
		for _, k := range append(w.cursor, flatten(w.pk)...) {
			assert.Contains(t, props, k, name)
		}
	}

	export := schemaOf(t, got[streamExport])
	assert.ElementsMatch(t, []string{
		"event", "time", "distinct_id", "insert_id", "_browser", "browser", "os", "plan",
		"mp_country_code", "amount", "items", "current_url", "is_trial",
	}, sortedKeys(export))
	assert.Equal(t, synclet.TimestampWithTZ, export["time"].AirbyteType)
	assert.Equal(t, synclet.PropTypes{synclet.String, synclet.Null}, export["amount"].Type)

	engage := schemaOf(t, got[streamEngage])
	assert.ElementsMatch(t, []string{
		"distinct_id", "last_seen", "created", "email", "name", "plan_value", "is_paid", "tags", "zip", "address", "legacy_score",
	}, sortedKeys(engage))
	for f, typ := range map[string]synclet.PropType{
		"plan_value": synclet.Number, "is_paid": boolean, "tags": synclet.Array, "address": synclet.Object,
		"zip": synclet.String, "legacy_score": synclet.String, "created": synclet.String,
	} {
		assert.Equal(t, synclet.PropTypes{typ, synclet.Null}, engage[f].Type, f)
	}
	assert.Equal(t, synclet.TimestampWOTZ, engage["created"].AirbyteType)
	assert.Equal(t, synclet.TimestampWOTZ, engage["last_seen"].AirbyteType)

	members := schemaOf(t, got[streamCohortMembers])
	assert.Contains(t, members, "cohort_id")
	assert.Len(t, members, len(engage)+1)
}

func flatten(pk [][]string) []string {
	var out []string
	for _, k := range pk {
		out = append(out, k...)
	}
	return out
}

func TestRenamer(t *testing.T) {
	r := exportRenamer()
	has := setOf([]string{"$browser", "browser", "$event", "event", "$os"})
	for in, want := range map[string]string{
		"$browser":   "_browser",
		"browser":    "browser",
		"$os":        "os",
		"event":      "property_event",
		"$event":     "_event",
		"$insert_id": "insert_id",
		"time":       "time",
		"$":          "$",
		"insert_id":  "property_insert_id",
	} {
		assert.Equal(t, want, r.field(in, has), in)
	}

	p := profileRenamer(cohortMembersFixed())
	none := setOf(nil)
	assert.Equal(t, "last_seen", p.field("$last_seen", none))
	assert.Equal(t, "property_last_seen", p.field("last_seen", none))
	assert.Equal(t, "property_cohort_id", p.field("cohort_id", none))
	assert.Equal(t, "property_distinct_id", p.field("$distinct_id", none))
}

func TestSchemaHints(t *testing.T) {
	schema := openSchema(exportSchema([]string{"$browser", "browser", "$os"}))
	assert.Equal(t, map[string]bool{"browser": true}, schemaHints(schema))

	// A record without browser still maps $browser as discover did.
	rec, _, err := exportRecord(rawEvent{Event: "e", Properties: map[string]any{"time": json.Number("1791741600"), "$browser": "Chrome"}},
		exportRenamer(), schemaHints(schema))
	require.NoError(t, err)
	assert.Equal(t, "Chrome", rec["_browser"])
}
