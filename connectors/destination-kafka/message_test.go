package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestEncodeValue(t *testing.T) {
	b, err := encodeValue("0b8c8f4e-0000-4000-8000-000000000001", &synclet.RecordMessage{
		Stream: "users", Namespace: "public", Data: json.RawMessage(`{"id": 1, "big": 12345678901234567890}`), EmittedAt: 1700000000000,
	})
	require.NoError(t, err)
	assert.Equal(t,
		`{"_airbyte_ab_id":"0b8c8f4e-0000-4000-8000-000000000001","_airbyte_stream":"users","_airbyte_emitted_at":1700000000000,"_airbyte_data":{"id":1,"big":12345678901234567890}}`,
		string(b))

	b, err = encodeValue("id", &synclet.RecordMessage{Stream: "users"})
	require.NoError(t, err)
	assert.JSONEq(t, `{"_airbyte_ab_id":"id","_airbyte_stream":"users","_airbyte_emitted_at":0,"_airbyte_data":null}`, string(b))

	_, err = encodeValue("id", &synclet.RecordMessage{Stream: "users", Data: json.RawMessage(`{bad`)})
	assert.Error(t, err)
}

func TestMessageKey(t *testing.T) {
	for _, c := range []struct {
		name string
		pk   [][]string
		data string
		want string // "<nil>" for no key
	}{
		{"no primary key", nil, `{"id":1}`, "<nil>"},
		{"integer", [][]string{{"id"}}, `{"id":42}`, "42"},
		{"string unquoted", [][]string{{"id"}}, `{"id":"usr_1"}`, "usr_1"},
		{"escaped string", [][]string{{"id"}}, `{"id":"a\"b"}`, `a"b`},
		{"big number kept as written", [][]string{{"id"}}, `{"id":12345678901234567890}`, "12345678901234567890"},
		{"bool", [][]string{{"ok"}}, `{"ok":true}`, "true"},
		{"object compacted", [][]string{{"k"}}, `{"k":{ "a" : 1 }}`, `{"a":1}`},
		{"composite", [][]string{{"tenant"}, {"id"}}, `{"id":7,"tenant":"acme"}`, `["acme",7]`},
		{"composite with a missing field", [][]string{{"tenant"}, {"id"}}, `{"id":7}`, `[null,7]`},
		{"nested path", [][]string{{"user", "id"}}, `{"user":{"id":3}}`, "3"},
		{"missing", [][]string{{"id"}}, `{"name":"x"}`, "<nil>"},
		{"null", [][]string{{"id"}}, `{"id":null}`, "<nil>"},
		{"through a non-object", [][]string{{"user", "id"}}, `{"user":5}`, "<nil>"},
		{"all composite fields missing", [][]string{{"a"}, {"b"}}, `{}`, "<nil>"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := messageKey(c.pk, json.RawMessage(c.data))
			require.NoError(t, err)
			if c.want == "<nil>" {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, c.want, string(got))
		})
	}
}
