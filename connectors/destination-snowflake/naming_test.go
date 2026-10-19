package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func stream(name string, props string, mode synclet.DestinationSyncMode, pk [][]string, cursor []string) synclet.ConfiguredStream {
	return synclet.ConfiguredStream{
		Stream:              synclet.Stream{Name: name, JSONSchema: json.RawMessage(`{"type":"object","properties":` + props + `}`)},
		SyncMode:            synclet.SyncModeIncremental,
		DestinationSyncMode: mode,
		PrimaryKey:          pk,
		CursorField:         cursor,
	}
}

func TestIdent(t *testing.T) {
	assert.Equal(t, `"USERS"`, ident("USERS"))
	assert.Equal(t, `"A""B"`, ident(`A"B`))
	assert.Equal(t, `"DB"."S"."T"`, ident("DB", "S", "T"))
}

func TestPlanStreamsNames(t *testing.T) {
	plans, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{
		stream("Users", `{"Id":{"type":"integer"},"updated At":{"type":"string"}}`, synclet.DestinationSyncModeAppendDedup, [][]string{{"Id"}}, []string{"updated At"}),
		stream("orders", `{"id":{"type":"integer"}}`, synclet.DestinationSyncModeOverwrite, nil, nil),
	}})
	require.NoError(t, err)
	assert.Equal(t, "USERS", plans[0].table)
	assert.Equal(t, "", plans[0].shadow)
	assert.Equal(t, "USERS", plans[0].target())
	assert.Equal(t, []string{"ID"}, idents(plans[0].pkColumns()))
	assert.Equal(t, "UPDATED AT", plans[0].columns[plans[0].cursor].ident)
	assert.Equal(t, "_SYNCLET_TMP_ORDERS", plans[1].target())
	assert.Equal(t, -1, plans[1].cursor)
}

func TestPlanStreamsFailures(t *testing.T) {
	cases := []struct {
		streams []synclet.ConfiguredStream
		want    string
	}{
		{[]synclet.ConfiguredStream{
			stream("users", `{}`, synclet.DestinationSyncModeAppend, nil, nil),
			stream("USERS", `{}`, synclet.DestinationSyncModeAppend, nil, nil),
		}, `streams "users" and "USERS" both map to table "USERS"`},
		{[]synclet.ConfiguredStream{stream("t", `{"id":{},"ID":{}}`, synclet.DestinationSyncModeAppend, nil, nil)},
			`properties "ID" and "id" both map to column "ID"`},
		{[]synclet.ConfiguredStream{stream("t", `{"_airbyte_raw_id":{}}`, synclet.DestinationSyncModeAppend, nil, nil)},
			`property "_airbyte_raw_id" maps to column "_AIRBYTE_RAW_ID", which the connector writes`},
		{[]synclet.ConfiguredStream{stream("", `{}`, synclet.DestinationSyncModeAppend, nil, nil)}, "empty stream name"},
		{[]synclet.ConfiguredStream{stream(strings.Repeat("a", 256), `{}`, synclet.DestinationSyncModeAppend, nil, nil)}, "longer than 255"},
		{[]synclet.ConfiguredStream{stream(strings.Repeat("a", 250), `{}`, synclet.DestinationSyncModeOverwrite, nil, nil)}, "longer than 255"},
		{[]synclet.ConfiguredStream{stream("t", `{"":{}}`, synclet.DestinationSyncModeAppend, nil, nil)}, "empty property name"},
		{[]synclet.ConfiguredStream{stream("t", `{"id":{}}`, synclet.DestinationSyncModeAppendDedup, nil, nil)}, "append_dedup needs a primary key"},
		{[]synclet.ConfiguredStream{stream("t", `{"id":{}}`, synclet.DestinationSyncModeAppendDedup, [][]string{{"nope"}}, nil)}, `primary key "nope" is not a property`},
		{[]synclet.ConfiguredStream{stream("t", `{"id":{}}`, synclet.DestinationSyncModeAppendDedup, [][]string{{"a", "b"}}, nil)}, "nested primary key"},
		{[]synclet.ConfiguredStream{stream("t", `{"id":{}}`, synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"x", "y"})}, "nested cursor"},
		{[]synclet.ConfiguredStream{stream("t", `{"id":{}}`, "upsert", nil, nil)}, `destination sync mode "upsert" is not supported`},
	}
	for _, c := range cases {
		_, err := planStreams(&synclet.ConfiguredCatalog{Streams: c.streams})
		require.Error(t, err, c.want)
		assert.Contains(t, err.Error(), c.want)
	}
}

func idents(cols []column) []string {
	var out []string
	for _, c := range cols {
		out = append(out, c.ident)
	}
	return out
}
