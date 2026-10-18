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

func TestWriteMessages(t *testing.T) {
	input := strings.Join([]string{
		`{"type":"RECORD","record":{"stream":"users","data":{"name":"Alice","age":30},"emitted_at":1}}`,
		`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":1}}}}`,
		`{"type":"RECORD","record":{"stream":"orders","data":{"id":"o1"},"emitted_at":2}}`,
		`{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"pos":1},"stream_states":[]}}}`,
		`{"type":"LOG","log":{"level":"INFO","message":"hi"}}`,
	}, "\n")
	mock := &mockSheetOps{}
	var out bytes.Buffer

	err := writeMessages(t.Context(), newSheetWriterWithOps(mock, 100),
		map[string]string{"users": "overwrite"},
		synclet.NewMessageReader(strings.NewReader(input)), synclet.NewEmitter(&out))
	require.NoError(t, err)

	assert.Equal(t, []string{"users"}, mock.clearedSheets)
	assert.ElementsMatch(t, []string{"users", "orders"}, mock.ensuredSheets)
	rows := map[string][][]interface{}{}
	for _, c := range mock.appendedCalls {
		rows[c.sheetTitle] = append(rows[c.sheetTitle], c.rows...)
	}
	assert.Equal(t, [][]interface{}{{"age", "name"}, {float64(30), "Alice"}}, rows["users"])
	assert.Equal(t, [][]interface{}{{"id"}, {"o1"}}, rows["orders"])

	var echoed synclet.Message
	require.NoError(t, json.Unmarshal(out.Bytes(), &echoed))
	require.Equal(t, synclet.MessageTypeState, echoed.Type)
	assert.Equal(t, synclet.StateTypeStream, echoed.State.Type)
	assert.Equal(t, synclet.StreamDescriptor{Name: "users"}, echoed.State.Stream.StreamDescriptor)
	assert.JSONEq(t, `{"cursor":1}`, string(echoed.State.Stream.StreamState))
}

func TestWriteMessagesRejectsInvalidInput(t *testing.T) {
	err := writeMessages(t.Context(), newSheetWriterWithOps(&mockSheetOps{}, 100), nil,
		synclet.NewMessageReader(strings.NewReader("not json\n")), synclet.NewEmitter(&bytes.Buffer{}))
	require.Error(t, err)
}
