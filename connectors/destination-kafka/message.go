package main

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// envelope is the message value, the shape Airbyte's destination-kafka
// writes.
type envelope struct {
	ID        string          `json:"_airbyte_ab_id"`
	Stream    string          `json:"_airbyte_stream"`
	EmittedAt int64           `json:"_airbyte_emitted_at"`
	Data      json.RawMessage `json:"_airbyte_data"`
}

func encodeValue(id string, rec *synclet.RecordMessage) ([]byte, error) {
	data := rec.Data
	if len(bytes.TrimSpace(data)) == 0 {
		data = json.RawMessage("null")
	}
	b, err := json.Marshal(envelope{ID: id, Stream: rec.Stream, EmittedAt: rec.EmittedAt, Data: data})
	if err != nil {
		return nil, fmt.Errorf("encode record of stream %s: %w", streamLabel(rec.Namespace, rec.Stream), err)
	}
	return b, nil
}

func newID() string { return uuid.NewString() }

// messageKey is the key of a record of a stream with primary key pk: the
// value of a one-field key as text (a string without its quotes, anything
// else as JSON), or a JSON array of the values of a composite key. It is nil
// when pk is empty or every key field is missing or null.
func messageKey(pk [][]string, data json.RawMessage) ([]byte, error) {
	if len(pk) == 0 {
		return nil, nil
	}
	values := make([]json.RawMessage, len(pk))
	present := false
	for i, path := range pk {
		v := lookup(data, path)
		if v == nil {
			v = json.RawMessage("null")
		} else {
			present = true
		}
		values[i] = v
	}
	if !present {
		return nil, nil
	}
	if len(values) == 1 {
		var s string
		if values[0][0] == '"' && json.Unmarshal(values[0], &s) == nil {
			return []byte(s), nil
		}
		return compact(values[0])
	}
	return json.Marshal(values)
}

// lookup returns the value at path in data, or nil when a field on the way
// is missing or null.
func lookup(data json.RawMessage, path []string) json.RawMessage {
	cur := data
	for _, field := range path {
		var obj map[string]json.RawMessage
		if json.Unmarshal(cur, &obj) != nil {
			return nil
		}
		v, ok := obj[field]
		if !ok {
			return nil
		}
		cur = v
	}
	cur = bytes.TrimSpace(cur)
	if len(cur) == 0 || bytes.Equal(cur, []byte("null")) {
		return nil
	}
	return cur
}

func compact(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
