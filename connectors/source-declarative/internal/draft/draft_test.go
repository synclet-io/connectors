package draft

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func records(t *testing.T, lines ...string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, l := range lines {
		d := json.NewDecoder(bytes.NewReader([]byte(l)))
		d.UseNumber()
		var m map[string]any
		require.NoError(t, d.Decode(&m))
		out = append(out, m)
	}
	return out
}

func TestSchema(t *testing.T) {
	recs := records(t,
		`{"id":1,"amount":10,"name":"a","day":"2026-10-01","at":"2026-10-01T10:00:00Z","note":null,"meta":{"k":"v","n":1},"tags":[{"x":1},{"y":"s"}],"mixed":"s","ok":true}`,
		`{"id":2,"amount":10.5,"name":"b","day":"2026-10-02","at":"2026-10-02T10:00:00.5+03:00","meta":{"k":"w"},"tags":[],"mixed":3,"big":1e3}`,
	)
	want := `{
		"type": "object",
		"additionalProperties": true,
		"properties": {
			"amount": {"type": "number"},
			"at": {"type": "string", "format": "date-time"},
			"big": {"type": ["null", "integer"]},
			"day": {"type": "string", "format": "date"},
			"id": {"type": "integer"},
			"meta": {"type": "object", "additionalProperties": true, "properties": {
				"k": {"type": "string"},
				"n": {"type": ["null", "integer"]}
			}},
			"mixed": {"type": ["integer", "string"]},
			"name": {"type": "string"},
			"note": {"type": "null"},
			"ok": {"type": ["null", "boolean"]},
			"tags": {"type": "array", "items": {"type": "object", "additionalProperties": true, "properties": {
				"x": {"type": ["null", "integer"]},
				"y": {"type": ["null", "string"]}
			}}}
		}
	}`
	got := Schema(recs)
	assert.JSONEq(t, want, string(got))
}

func TestSchemaFormatsNeedEverySample(t *testing.T) {
	got := Schema(records(t, `{"d":"2026-10-01","t":"2026-10-01T00:00:00Z","s":"2026-10-01"}`, `{"d":"2026-10-02","t":"later","s":"2026-10-01T00:00:00Z"}`))
	assert.JSONEq(t, `{"type":"object","additionalProperties":true,"properties":{
		"d":{"type":"string","format":"date"},
		"s":{"type":"string"},
		"t":{"type":"string"}}}`, string(got))
}

func TestSchemaArraysOfScalars(t *testing.T) {
	got := Schema(records(t, `{"a":[1,2.5,null],"b":[]}`, `{"a":["x"]}`))
	assert.JSONEq(t, `{"type":"object","additionalProperties":true,"properties":{
		"a":{"type":"array","items":{"type":["null","number","string"]}},
		"b":{"type":["null","array"],"items":{}}}}`, string(got))
}

func TestSchemaEmpty(t *testing.T) {
	assert.JSONEq(t, `{"type":"object","properties":{},"additionalProperties":true}`, string(Schema(nil)))
}

func TestSchemaDeterministic(t *testing.T) {
	a := records(t, `{"z":1,"a":{"q":"2026-10-01","b":[{"c":1}]}}`, `{"m":null,"a":{"b":[{"d":"x"}]}}`)
	b := records(t, `{"z":1,"a":{"q":"2026-10-01","b":[{"c":1}]}}`, `{"m":null,"a":{"b":[{"d":"x"}]}}`)
	first := string(Schema(a))
	for range 20 {
		require.Equal(t, first, string(Schema(b)))
	}
	assert.Less(t, bytes.Index([]byte(first), []byte(`"a"`)), bytes.Index([]byte(first), []byte(`"m"`)))
	assert.Less(t, bytes.Index([]byte(first), []byte(`"m"`)), bytes.Index([]byte(first), []byte(`"z"`)))
}
