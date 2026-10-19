package manifest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// schemalessYAML has a stream without schema whose primary key, entries
// fields, add key and cursor field all need one, and a stream with one.
const schemalessYAML = `
version: 1
connector: {name: source-acme}
base_url: https://api.example.com/v1/
spec:
  required: [api_key]
  properties:
    api_key: {type: string, secret: true}
auth: {type: bearer, token: "{{ config.api_key }}"}
check: {stream: rates}
streams:
  - name: rates
    path: rates
    primary_key: [code]
    records: {path: $.rates, mode: entries, key_field: code, value_field: rate, add: {base: $.base}}
    incremental: {cursor_field: day, cursor_format: date, start: "2026-10-01"}
  - name: tags
    path: tags
    schema: {type: object, properties: {id: {type: string}}}
`

func schemalessDoc(t *testing.T) map[string]any {
	t.Helper()
	doc, err := DecodeYAML([]byte(schemalessYAML))
	require.NoError(t, err)
	return doc
}

func TestLoadDocumentNeedsSchema(t *testing.T) {
	_, err := LoadDocument(schemalessDoc(t), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "streams[0].schema: required key is missing")
}

func TestLoadDocumentRelaxed(t *testing.T) {
	m, err := LoadDocumentRelaxed(schemalessDoc(t))
	require.NoError(t, err)
	assert.Nil(t, m.Streams[0].Schema)
	assert.JSONEq(t, `{"type":"object","properties":{"id":{"type":"string"}}}`, string(m.Streams[1].Schema))
}

func TestLoadDocumentRelaxedKeepsOtherRules(t *testing.T) {
	doc := schemalessDoc(t)
	doc["streams"].([]any)[0].(map[string]any)["pagination"] = map[string]any{"type": "pages"}
	_, err := LoadDocumentRelaxed(doc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "streams[0].pagination")

	doc = schemalessDoc(t)
	doc["streams"].([]any)[1].(map[string]any)["schema"] = "schemas/tags.json"
	_, err = LoadDocumentRelaxed(doc)
	require.Error(t, err)
	assert.Equal(t, "streams[1].schema: a file path is not allowed in an injected manifest; inline the schema", err.Error())
}

func TestLoadDocumentRelaxedChecksGivenSchemas(t *testing.T) {
	doc := schemalessDoc(t)
	doc["streams"].([]any)[1].(map[string]any)["primary_key"] = []any{"missing"}
	_, err := LoadDocumentRelaxed(doc)
	require.Error(t, err)
	assert.Equal(t, `streams[1].primary_key[0]: field "missing" is not in the stream's schema`, err.Error())
}

func TestSchemaFieldErrors(t *testing.T) {
	m, err := LoadDocumentRelaxed(schemalessDoc(t))
	require.NoError(t, err)
	draft := json.RawMessage(`{"type":"object","properties":{"code":{"type":"string"},"rate":{"type":"number"}}}`)
	var got []string
	for _, e := range SchemaFieldErrors(m, 0, draft) {
		got = append(got, e.Error())
	}
	assert.Equal(t, []string{
		`streams[0].incremental.cursor_field: field "day" is not in the stream's schema`,
		`streams[0].records.add.base: field "base" is not in the stream's schema`,
	}, got)
	full := json.RawMessage(`{"type":"object","properties":{"code":{},"rate":{},"base":{},"day":{}}}`)
	assert.Empty(t, SchemaFieldErrors(m, 0, full))
	empty := json.RawMessage(`{"type":"object","properties":{}}`)
	assert.Len(t, SchemaFieldErrors(m, 0, empty), 5)
	assert.True(t, strings.HasPrefix(SchemaFieldErrors(m, 0, empty)[0].Path, "streams[0]."))
}
