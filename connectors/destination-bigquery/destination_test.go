package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestCatalogStreamsKeepsFormat(t *testing.T) {
	var cat synclet.ConfiguredCatalog
	require.NoError(t, json.Unmarshal([]byte(`{"streams":[{
		"stream":{"name":"events","namespace":"ns","json_schema":{"type":"object","properties":{"ts":{"type":"string","format":"date-time"}}}},
		"sync_mode":"incremental","destination_sync_mode":"append_dedup","primary_key":[["id"]],"cursor_field":["ts"]}]}`), &cat))

	streams, err := catalogStreams(&cat)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	cs := streams[0]
	assert.Equal(t, "events", cs.Stream.Name)
	assert.Equal(t, "ns", cs.Stream.Namespace)
	assert.Equal(t, "append_dedup", cs.DestinationSyncMode)
	assert.Equal(t, [][]string{{"id"}}, cs.PrimaryKey)
	assert.Equal(t, []string{"ts"}, cs.CursorField)
	ts := cs.Stream.JSONSchema["properties"].(map[string]interface{})["ts"].(map[string]interface{})
	assert.Equal(t, "date-time", ts["format"])

	bqSchema, _ := buildSchema(cs.Stream.JSONSchema)
	var tsField bool
	for _, f := range bqSchema {
		if f.Name == "ts" {
			tsField = true
			assert.Equal(t, "TIMESTAMP", string(f.Type))
		}
	}
	assert.True(t, tsField)
}

func TestWriteMessagesEchoesStatesAfterFlush(t *testing.T) {
	ldr := &mockLoader{}
	w := NewBigQueryWriter(newMockOps(), ldr, testConfig(), testCatalog())
	input := strings.Join([]string{
		`{"type":"RECORD","record":{"stream":"users","data":{"id":1,"name":"Alice","age":30},"emitted_at":1}}`,
		`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":1}}}}`,
		`{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"binlog_pos":7},"stream_states":[{"stream_descriptor":{"name":"users"},"stream_state":{"done":true}}]}}}`,
	}, "\n")
	var out bytes.Buffer

	require.NoError(t, writeMessages(t.Context(), w, synclet.NewMessageReader(strings.NewReader(input)), synclet.NewEmitter(&out)))

	assert.Equal(t, 1, ldr.loadCallCount)
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	require.Len(t, lines, 2)
	var stream, global synclet.Message
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &stream))
	require.NoError(t, json.Unmarshal([]byte(lines[1]), &global))
	assert.Equal(t, synclet.StateTypeStream, stream.State.Type)
	assert.JSONEq(t, `{"cursor":1}`, string(stream.State.Stream.StreamState))
	require.Equal(t, synclet.StateTypeGlobal, global.State.Type)
	assert.JSONEq(t, `{"binlog_pos":7}`, string(global.State.Global.SharedState))
	require.Len(t, global.State.Global.StreamStates, 1)
	assert.Equal(t, synclet.StreamDescriptor{Name: "users"}, global.State.Global.StreamStates[0].StreamDescriptor)
}

func TestWriteMessagesRejectsInvalidInput(t *testing.T) {
	w := NewBigQueryWriter(newMockOps(), &mockLoader{}, testConfig(), testCatalog())
	err := writeMessages(t.Context(), w, synclet.NewMessageReader(strings.NewReader("not json\n")), synclet.NewEmitter(&bytes.Buffer{}))
	require.Error(t, err)
}

func TestWriteMessagesKeepsNumbersExact(t *testing.T) {
	ldr := &mockLoader{}
	schema := map[string]interface{}{"properties": map[string]interface{}{
		"id":     map[string]interface{}{"type": "integer"},
		"amount": map[string]interface{}{"type": "number"},
	}}
	w := NewBigQueryWriter(newMockOps(), ldr, testConfig(), testCatalogWithMode("payments", "", "append", nil, nil, schema))
	input := `{"type":"RECORD","record":{"stream":"payments","data":{"id":9007199254740993,"amount":12345678901234567.89},"emitted_at":1}}`

	require.NoError(t, writeMessages(t.Context(), w, synclet.NewMessageReader(strings.NewReader(input)), synclet.NewEmitter(&bytes.Buffer{})))

	require.Len(t, ldr.loadCalls, 1)
	rec := ldr.loadCalls[0].records[0]
	assert.Equal(t, int64(9007199254740993), rec["id"])
	assert.Equal(t, json.Number("12345678901234567.89"), rec["amount"])
	encoded, err := json.Marshal(rec["amount"])
	require.NoError(t, err)
	assert.Equal(t, "12345678901234567.89", string(encoded))
}
