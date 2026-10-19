package manifest

import (
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const baseYAML = `
version: 1
connector: {name: source-acme}
base_url: https://api.example.com/v1/
spec:
  title: Acme
  required: [api_key, start_date]
  properties:
    api_key: {type: string, secret: true}
    start_date: {type: string, format: date-time}
    accounts: {type: array, items: {type: string}}
    region: {type: string}
auth: {type: bearer, token: "{{ config.api_key }}"}
check: {stream: customers}
streams:
  - name: customers
    path: customers
    primary_key: [id]
    schema: schemas/customers.json
    records: {path: $.data}
    pagination: {type: cursor, cursor_from: "last:id", cursor_param: starting_after, has_more: $.has_more}
    incremental: {cursor_field: created, cursor_format: unix_s, start: "{{ config.start_date }}", lookback: 1h}
  - name: orders
    path: "customers/{{ parent.id }}/orders"
    partition: {parent: customers}
    schema: schemas/orders.json
    records: {path: $.data, add: {customer_id: "{{ parent.id }}"}}
`

const customersSchema = `{"type":"object","properties":{"id":{"type":"string"},"created":{"type":"integer"},"name":{"type":["string","null"]}}}`
const ordersSchema = `{"type":"object","properties":{"id":{"type":"string"},"customer_id":{"type":"string"}}}`

func baseFS(yaml string) fstest.MapFS {
	return fstest.MapFS{
		FileName:                 {Data: []byte(yaml)},
		"schemas/customers.json": {Data: []byte(customersSchema)},
		"schemas/orders.json":    {Data: []byte(ordersSchema)},
	}
}

func baseDoc(t *testing.T) map[string]any {
	t.Helper()
	doc, err := DecodeYAML([]byte(baseYAML))
	require.NoError(t, err)
	return doc
}

func streamOf(doc map[string]any, i int) map[string]any {
	return doc["streams"].([]any)[i].(map[string]any)
}

// paths returns the paths of every error in err.
func paths(err error) []string {
	var out []string
	for _, e := range List(err) {
		out = append(out, e.Path)
	}
	return out
}

func requireError(t *testing.T, err error, path, contains string) {
	t.Helper()
	require.Error(t, err)
	for _, e := range List(err) {
		if e.Path == path && strings.Contains(e.Message, contains) {
			return
		}
	}
	t.Fatalf("no error at %s containing %q in:\n%v", path, contains, err)
}

func TestLoadBase(t *testing.T) {
	m, err := LoadFS(baseFS(baseYAML))
	require.NoError(t, err)
	assert.Equal(t, "source-acme", m.Connector.Name)
	require.Len(t, m.Streams, 2)
	assert.Equal(t, CursorLast, m.Streams[0].Pagination.Cursor.Kind)
	assert.JSONEq(t, customersSchema, string(m.Streams[0].Schema))
}

func TestLoadSchemaErrors(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(doc map[string]any)
		path     string
		contains string
	}{
		{"unknown key", func(d map[string]any) { streamOf(d, 0)["foo"] = 1 }, "streams[0].foo", "unknown key"},
		{"wrong type", func(d map[string]any) {
			streamOf(d, 0)["pagination"].(map[string]any)["size"] = "ten"
		}, "streams[0].pagination.size", "wrong type: got string, want integer"},
		{"version", func(d map[string]any) { d["version"] = 2 }, "version", "must be 1"},
		{"missing streams", func(d map[string]any) { delete(d, "streams") }, "streams", "required key is missing"},
		{"unknown auth key", func(d map[string]any) { d["auth"].(map[string]any)["in"] = "header" }, "auth.in", "unknown key"},
		{"unknown auth type", func(d map[string]any) { d["auth"] = map[string]any{"type": "digest"} }, "auth.type", "must be one of"},
		{"pagination key of another type", func(d map[string]any) {
			streamOf(d, 0)["pagination"].(map[string]any)["page_param"] = "p"
		}, "streams[0].pagination.page_param", "unknown key"},
		{"odd key", func(d map[string]any) { streamOf(d, 0)["params"] = map[string]any{"created[gte]": map[string]any{}} }, "streams[0].params['created[gte]']", ""},
		{"bad duration", func(d map[string]any) {
			streamOf(d, 0)["incremental"].(map[string]any)["lookback"] = "an hour"
		}, "streams[0].incremental.lookback", "pattern"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := baseDoc(t)
			tt.mutate(doc)
			_, err := LoadDocument(doc, baseFS(""))
			requireError(t, err, tt.path, tt.contains)
		})
	}
}

func TestLoadReportsEveryError(t *testing.T) {
	doc := baseDoc(t)
	streamOf(doc, 0)["foo"] = 1
	streamOf(doc, 1)["bar"] = 1
	_, err := LoadDocument(doc, baseFS(""))
	assert.Equal(t, []string{"streams[0].foo", "streams[1].bar"}, paths(err))
	assert.Contains(t, err.Error(), "streams[0].foo: unknown key")
}

func TestLoadSchemaWithoutProperties(t *testing.T) {
	fsys := baseFS(baseYAML)
	fsys["schemas/customers.json"] = &fstest.MapFile{Data: []byte(`{"type":"object"}`)}
	_, err := LoadFS(fsys)
	requireError(t, err, "streams[0].schema (schemas/customers.json)", `needs "properties"`)

	doc := baseDoc(t)
	streamOf(doc, 1)["schema"] = map[string]any{"type": "object"}
	_, err = LoadDocument(doc, baseFS(""))
	requireError(t, err, "streams[1].schema", `needs "properties"`)

	doc = baseDoc(t)
	streamOf(doc, 1)["schema"] = map[string]any{"type": "array", "properties": map[string]any{"id": map[string]any{}}}
	_, err = LoadDocument(doc, baseFS(""))
	requireError(t, err, "streams[1].schema", "not an object schema")
}

func TestLoadMissingSchemaFile(t *testing.T) {
	fsys := baseFS(baseYAML)
	delete(fsys, "schemas/orders.json")
	_, err := LoadFS(fsys)
	requireError(t, err, "streams[1].schema", "schemas/orders.json")
}

func TestLoadInlineMatchesFile(t *testing.T) {
	file, err := LoadFS(baseFS(baseYAML))
	require.NoError(t, err)

	doc := baseDoc(t)
	for i, s := range []string{customersSchema, ordersSchema} {
		var v map[string]any
		require.NoError(t, json.Unmarshal([]byte(s), &v))
		streamOf(doc, i)["schema"] = v
	}
	inline, err := LoadDocument(doc, nil)
	require.NoError(t, err)
	assert.Equal(t, file, inline)

	inlineYAML := strings.Replace(strings.Replace(baseYAML,
		"schema: schemas/customers.json", "schema: {type: object, properties: {id: {type: string}, created: {type: integer}, name: {type: [string, \"null\"]}}}", 1),
		"schema: schemas/orders.json", "schema: {type: object, properties: {id: {type: string}, customer_id: {type: string}}}", 1)
	fromYAML, err := LoadFS(fstest.MapFS{FileName: {Data: []byte(inlineYAML)}})
	require.NoError(t, err)
	assert.Equal(t, file, fromYAML)
}

func TestLoadInjectedFilePath(t *testing.T) {
	_, err := LoadDocument(baseDoc(t), nil)
	requireError(t, err, "streams[0].schema", "inline the schema")
	requireError(t, err, "streams[1].schema", "inline the schema")
}

func TestLoadAnchorsAndMergeKeys(t *testing.T) {
	yaml := strings.Replace(baseYAML, "  - name: customers\n", "  - &list\n    name: customers\n", 1) +
		"  - {<<: *list, name: invoices, path: invoices}\n"
	m, err := LoadFS(baseFS(yaml))
	require.NoError(t, err)
	require.Len(t, m.Streams, 3)
	inv := m.Streams[2]
	assert.Equal(t, "invoices", inv.Name)
	assert.Equal(t, "invoices", inv.Path.Raw)
	assert.Equal(t, "created", inv.Incremental.CursorField.Raw)

	bad := yaml + "  - {<<: *list, name: refunds, path: refunds, foo: 1}\n"
	_, err = LoadFS(baseFS(bad))
	requireError(t, err, "streams[3].foo", "unknown key")
}

func TestLint(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(doc map[string]any)
		path     string
		contains string
	}{
		{"unknown root", func(d map[string]any) { streamOf(d, 0)["path"] = "{{ env.HOME }}" }, "streams[0].path", "unknown root"},
		{"unclosed template", func(d map[string]any) { streamOf(d, 0)["path"] = "{{ config.region" }, "streams[0].path", "without }}"},
		{"unknown config property", func(d map[string]any) { streamOf(d, 0)["params"] = map[string]any{"r": "{{ config.zone }}"} }, "streams[0].params.r", `no "zone"`},
		{"config array without join", func(d map[string]any) { streamOf(d, 0)["params"] = map[string]any{"a": "{{ config.accounts }}"} }, "streams[0].params.a", "join"},
		{"join on a string", func(d map[string]any) {
			streamOf(d, 0)["params"] = map[string]any{"a": `{{ config.region | join(",") }}`}
		}, "streams[0].params.a", "needs an array"},
		{"since outside incremental", func(d map[string]any) { streamOf(d, 1)["params"] = map[string]any{"s": "{{ since }}"} }, "streams[1].params.s", "needs incremental"},
		{"parent without a parent", func(d map[string]any) { streamOf(d, 0)["path"] = "x/{{ parent.id }}" }, "streams[0].path", "needs partition.parent"},
		{"partition.value without values", func(d map[string]any) { streamOf(d, 0)["path"] = "x/{{ partition.value }}" }, "streams[0].path", "needs partition.values"},
		{"since in base_url", func(d map[string]any) { d["base_url"] = "https://x/{{ since }}/" }, "base_url", "config only"},
		{"base_url slash", func(d map[string]any) { d["base_url"] = "https://api.example.com/v1" }, "base_url", "must end with /"},
		{"unknown parent", func(d map[string]any) { streamOf(d, 1)["partition"] = map[string]any{"parent": "users"} }, "streams[1].partition.parent", `no stream named "users"`},
		{"own parent", func(d map[string]any) { streamOf(d, 1)["partition"] = map[string]any{"parent": "orders"} }, "streams[1].partition.parent", "its own parent"},
		{"partitioned parent", func(d map[string]any) {
			streamOf(d, 0)["partition"] = map[string]any{"values": []any{"a"}}
		}, "streams[1].partition.parent", "one level only"},
		{"values not an array", func(d map[string]any) {
			streamOf(d, 0)["partition"] = map[string]any{"values": "{{ config.region }}"}
		}, "streams[0].partition.values", "not an array"},
		{"reserved spec property", func(d map[string]any) {
			d["spec"].(map[string]any)["properties"].(map[string]any)["__x"] = map[string]any{"type": "string"}
		}, "spec.properties.__x", "reserved"},
		{"required unknown", func(d map[string]any) { d["spec"].(map[string]any)["required"] = []any{"nope"} }, "spec.required[0]", `no property named "nope"`},
		{"cursor field not in schema", func(d map[string]any) {
			streamOf(d, 0)["incremental"].(map[string]any)["cursor_field"] = "updated"
		}, "streams[0].incremental.cursor_field", `"updated" is not in the stream's schema`},
		{"primary key not in schema", func(d map[string]any) { streamOf(d, 0)["primary_key"] = []any{"uid"} }, "streams[0].primary_key[0]", "not in the stream's schema"},
		{"add field not in schema", func(d map[string]any) {
			streamOf(d, 1)["records"].(map[string]any)["add"] = map[string]any{"account": "x"}
		}, "streams[1].records.add.account", "not in the stream's schema"},
		{"key_field not in schema", func(d map[string]any) {
			streamOf(d, 1)["records"] = map[string]any{"mode": "entries", "key_field": "code"}
		}, "streams[1].records.key_field", "not in the stream's schema"},
		{"entries without key_field", func(d map[string]any) {
			streamOf(d, 1)["records"] = map[string]any{"mode": "entries"}
		}, "streams[1].records.key_field", "needs key_field"},
		{"value_field outside entries", func(d map[string]any) {
			streamOf(d, 1)["records"] = map[string]any{"value_field": "id"}
		}, "streams[1].records.value_field", "only mode entries"},
		{"last cursor not in schema", func(d map[string]any) {
			streamOf(d, 0)["pagination"].(map[string]any)["cursor_from"] = "last:uid"
		}, "streams[0].pagination.cursor_from", "not in the stream's schema"},
		{"check stream", func(d map[string]any) { d["check"] = map[string]any{"stream": "nope"} }, "check.stream", `no stream named "nope"`},
		{"size without size_param", func(d map[string]any) { streamOf(d, 0)["pagination"].(map[string]any)["size"] = 10 }, "streams[0].pagination", "size and size_param"},
		{"short_page without size", func(d map[string]any) {
			streamOf(d, 1)["pagination"] = map[string]any{"type": "page", "short_page": true}
		}, "streams[1].pagination.short_page", "needs size"},
		{"bad path", func(d map[string]any) { streamOf(d, 0)["records"].(map[string]any)["path"] = "$.data[x]" }, "streams[0].records.path", "not an index"},
		{"zero lookback", func(d map[string]any) {
			streamOf(d, 0)["incremental"].(map[string]any)["lookback"] = "0s"
		}, "streams[0].incremental.lookback", "must be positive"},
		{"zero window step", func(d map[string]any) {
			streamOf(d, 0)["incremental"].(map[string]any)["window"] = map[string]any{"step": "0d"}
		}, "streams[0].incremental.window.step", "must be positive"},
		{"cursor format", func(d map[string]any) {
			streamOf(d, 0)["incremental"].(map[string]any)["cursor_format"] = "iso"
		}, "streams[0].incremental.cursor_format", "Go time layout"},
		{"missing start", func(d map[string]any) { delete(streamOf(d, 0)["incremental"].(map[string]any), "start") }, "streams[0].incremental.start", "missing"},
		{"child incremental of a full-refresh parent", func(d map[string]any) {
			delete(streamOf(d, 0), "incremental")
			streamOf(d, 1)["incremental"] = map[string]any{"cursor_field": "id", "cursor_format": "rfc3339"}
		}, "streams[1].incremental", "only when its parent"},
		{"child with its own start", func(d map[string]any) {
			streamOf(d, 1)["incremental"] = map[string]any{"cursor_field": "id", "cursor_format": "rfc3339", "start": "{{ config.start_date }}"}
		}, "streams[1].incremental.start", "parent's"},
		{"duplicate stream", func(d map[string]any) { streamOf(d, 1)["name"] = "customers" }, "streams[1].name", "also streams[0]"},
		{"bad wait", func(d map[string]any) {
			d["retry"] = map[string]any{"rate_limit": map[string]any{"wait": []any{"soon"}}}
		}, "retry.rate_limit.wait[0]", "neither"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := baseDoc(t)
			tt.mutate(doc)
			_, err := LoadDocument(doc, baseFS(""))
			requireError(t, err, tt.path, tt.contains)
		})
	}
}

func TestTemplateRender(t *testing.T) {
	cfg := map[string]any{"region": "eu west/1", "accounts": []any{"a", "b"}, "n": json.Number("7")}
	tests := []struct {
		tmpl   string
		escape func(string) string
		want   string
	}{
		{"x{{ config.missing }}y", nil, "xy"},
		{`{{ config.accounts | join(",") }}`, nil, "a,b"},
		{`{{config.accounts|join('|')}}`, nil, "a|b"},
		{"repos/{{ config.region }}/issues", url.PathEscape, "repos/eu%20west%2F1/issues"},
		{"n={{ config.n }}", nil, "n=7"},
	}
	for _, tt := range tests {
		tp, err := ParseTemplate(tt.tmpl)
		require.NoError(t, err, tt.tmpl)
		got, err := tp.Render(Scope{Config: cfg}, tt.escape)
		require.NoError(t, err, tt.tmpl)
		assert.Equal(t, tt.want, got, tt.tmpl)
	}

	tp, err := ParseTemplate("{{ parent.id }}")
	require.NoError(t, err)
	_, err = tp.Render(Scope{Parent: map[string]any{"name": "x"}}, nil)
	assert.ErrorContains(t, err, "the parent record has no id")

	tp, err = ParseTemplate("{{ config.accounts }}")
	require.NoError(t, err)
	_, err = tp.Render(Scope{Config: cfg}, nil)
	assert.ErrorContains(t, err, "join")
}

func TestPathGet(t *testing.T) {
	var doc any
	require.NoError(t, json.Unmarshal([]byte(`{"list":[{"customer":{"id":"c1"}},{"customer":{"id":"c2"}}],"a b":{"c":[1,2]}}`), &doc))
	tests := []struct {
		path  string
		want  any
		found bool
	}{
		{"$.list[*].customer.id", []any{"c1", "c2"}, true},
		{"$['a b'].c[1]", float64(2), true},
		{"$.list[5]", nil, false},
		{"$.missing", nil, false},
		{"$", doc, true},
	}
	for _, tt := range tests {
		p, err := ParsePath(tt.path)
		require.NoError(t, err, tt.path)
		got, found := p.Get(doc)
		assert.Equal(t, tt.found, found, tt.path)
		assert.Equal(t, tt.want, got, tt.path)
	}
}

func TestLoadSchemaRefusesExternalRefs(t *testing.T) {
	f := t.TempDir() + "/secret.json"
	require.NoError(t, os.WriteFile(f, []byte(`{"type":"string","description":"TOPSECRET"}`), 0o600))
	for _, ref := range []string{"file://" + f, "file://" + f + ".missing", "http://127.0.0.1:1/schema.json"} {
		t.Run(ref, func(t *testing.T) {
			doc := baseDoc(t)
			streamOf(doc, 0)["schema"] = map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"$ref": ref}}}
			_, err := LoadDocument(doc, baseFS(""))
			requireError(t, err, "streams[0].schema", "not a valid JSON Schema")
			assert.NotContains(t, err.Error(), "TOPSECRET")
			assert.NotContains(t, err.Error(), "no such file")
			assert.NotContains(t, err.Error(), "connection refused")
		})
	}
}
