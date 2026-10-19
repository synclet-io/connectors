package engine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"

	"source-declarative/internal/manifest"
)

const fileSchema = `{"type":"object","properties":{"id":{"type":"string"},"updated":{"type":"integer"}},"additionalProperties":true}`

// twinYAML is a manifest whose first stream's schema is a file and second's
// inline; inlineYAML is the same document with both inline.
var twinYAML = manifestYAML("", `  - name: items
    path: items
    params: {"updated[gte]": "{{ since }}"}
    primary_key: [id]
    schema: schemas/items.json
    records: {path: $.data}
    pagination: {type: cursor, cursor_from: "last:id", cursor_param: after, has_more: $.has_more}
    incremental: {cursor_field: updated, cursor_format: unix_s, start: "{{ config.start_date }}", lookback: 1h}
  - name: tags
    path: tags
    primary_key: [id]
    schema: {type: object, properties: {id: {type: string}}}
`)

var inlineYAML = strings.Replace(twinYAML, "schema: schemas/items.json", "schema: "+fileSchema, 1)

func twinAPI(t *testing.T) *fakeAPI {
	return newAPI(t).
		on("/v1/items",
			reply{status: 200, body: fmt.Sprintf(`{"data":[{"id":"a","updated":%d}],"has_more":true}`, startUnix+10)},
			reply{status: 200, body: fmt.Sprintf(`{"data":[{"id":"b","updated":%d}],"has_more":false}`, startUnix+20)}).
		json("/v1/tags", `[{"id":"t1"}]`)
}

func bakedSource(t *testing.T, a *fakeAPI) synclet.Source[json.RawMessage] {
	t.Helper()
	m, err := manifest.LoadFS(fstest.MapFS{
		manifest.FileName:    {Data: []byte(twinYAML)},
		"schemas/items.json": {Data: []byte(fileSchema)},
	})
	require.NoError(t, err)
	return New(m, a.options(newClock()))
}

// withManifest returns config with yaml's document as __manifest.
func withManifest(t *testing.T, config, yaml string) string {
	t.Helper()
	doc, err := manifest.DecodeYAML([]byte(yaml))
	require.NoError(t, err)
	var cfg map[string]any
	require.NoError(t, json.Unmarshal([]byte(config), &cfg))
	cfg["__manifest"] = doc
	b, err := json.Marshal(cfg)
	require.NoError(t, err)
	return string(b)
}

// run is the parsed outcome of one read: records then states per stream.
func outcome(t *testing.T, out *testkit.Output) map[string][]any {
	t.Helper()
	require.NoError(t, out.Err)
	got := map[string][]any{}
	for _, m := range out.Messages {
		switch m.Type {
		case synclet.MessageTypeRecord:
			var v any
			require.NoError(t, json.Unmarshal(m.Record.Data, &v))
			got[m.Record.Stream] = append(got[m.Record.Stream], v)
		case synclet.MessageTypeState:
			var v any
			require.NoError(t, json.Unmarshal(m.State.Stream.StreamState, &v))
			got[m.State.Stream.StreamDescriptor.Name] = append(got[m.State.Stream.StreamDescriptor.Name], map[string]any{"state": v})
		}
	}
	return got
}

func TestBakedVsInjected(t *testing.T) {
	baked := bakedSource(t, twinAPI(t))
	generic := NewGeneric(twinAPI(t).options(newClock()))
	injected := withManifest(t, testConfig, inlineYAML)

	bakedCat := runCmd(t, baked, synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(testConfig)}).Catalog()
	injectedCat := runCmd(t, generic, synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(injected)}).Catalog()
	require.NotNil(t, bakedCat)
	require.NotNil(t, injectedCat)
	assert.Equal(t, bakedCat, injectedCat)

	state := fmt.Sprintf(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"items"},"stream_state":{"updated":%d}}}]`, startUnix+3615)
	for _, mode := range []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental} {
		for _, st := range []string{"", state} {
			t.Run(fmt.Sprintf("%s state=%t", mode, st != ""), func(t *testing.T) {
				cat := catalog(t, baked, testConfig, mode, "items")
				cat.Streams = append(cat.Streams, catalog(t, baked, testConfig, synclet.SyncModeFullRefresh, "tags").Streams...)
				want := outcome(t, read(t, bakedSource(t, twinAPI(t)), testConfig, cat, st))
				got := outcome(t, read(t, NewGeneric(twinAPI(t).options(newClock())), injected, cat, st))
				assert.NotEmpty(t, want["items"])
				assert.Equal(t, want, got)
			})
		}
	}
}

func TestInjected(t *testing.T) {
	generic := func(t *testing.T) synclet.Source[json.RawMessage] { return NewGeneric(twinAPI(t).options(newClock())) }

	t.Run("a schema file path", func(t *testing.T) {
		cfg := withManifest(t, testConfig, twinYAML)
		st := checkStatus(t, generic(t), cfg)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "__manifest.streams[0].schema: a file path is not allowed in an injected manifest; inline the schema")

		cat := catalog(t, bakedSource(t, twinAPI(t)), testConfig, synclet.SyncModeFullRefresh, "items")
		out := read(t, generic(t), cfg, cat, "")
		msg, ft := traceMessage(out)
		assert.Equal(t, synclet.FailureTypeConfigError, ft)
		assert.Contains(t, msg, "inline the schema")
		assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: "items"}))
	})

	t.Run("on a catalog image", func(t *testing.T) {
		st := checkStatus(t, bakedSource(t, twinAPI(t)), withManifest(t, testConfig, inlineYAML))
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "this connector's manifest is built into its image; remove `__manifest`")
	})

	t.Run("missing on the generic image", func(t *testing.T) {
		st := checkStatus(t, generic(t), testConfig)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "`__manifest` is required on the generic source-declarative image")
	})

	t.Run("over 512 KiB", func(t *testing.T) {
		big := strings.Replace(inlineYAML, "  title: \n", "", 1)
		big = strings.Replace(big, "spec:\n", "spec:\n  description: "+strings.Repeat("x", 512*1024)+"\n", 1)
		st := checkStatus(t, generic(t), withManifest(t, testConfig, big))
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "KiB")
		assert.Contains(t, st.Message, "at most 512 KiB")
		assert.Less(t, len(st.Message), 1000)
	})

	t.Run("not an object", func(t *testing.T) {
		st := checkStatus(t, generic(t), `{"api_key":"k1","__manifest":"version: 1"}`)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "`__manifest` must be a JSON object")
	})

	t.Run("every problem at discover", func(t *testing.T) {
		bad := strings.Replace(inlineYAML, "    path: tags\n", "    path: tags\n    foo: 1\n", 1)
		bad = strings.Replace(bad, "    path: items\n", "    path: items\n    bar: 2\n", 1)
		out := runCmd(t, generic(t), synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(withManifest(t, testConfig, bad))})
		msg, ft := traceMessage(out)
		assert.Equal(t, synclet.FailureTypeConfigError, ft)
		assert.Contains(t, msg, "__manifest.streams[0].bar: unknown key")
		assert.Contains(t, msg, "__manifest.streams[1].foo: unknown key")
		assert.Nil(t, out.Catalog())
	})

	t.Run("a reserved spec property", func(t *testing.T) {
		bad := strings.Replace(inlineYAML, "    region: {type: string}\n", "    region: {type: string}\n    __x: {type: string}\n", 1)
		st := checkStatus(t, generic(t), withManifest(t, testConfig, bad))
		assert.Contains(t, st.Message, "__manifest.spec.properties.__x: names starting with __ are reserved")
	})

	t.Run("the manifest never reaches a log line", func(t *testing.T) {
		marked := strings.Replace(inlineYAML, "connector: {name: source-test}", "connector: {name: source-test, documentation_url: \"https://example.com/MARKER-7d1f\"}", 1)
		cfg := withManifest(t, testConfig, marked)
		cat := catalog(t, generic(t), cfg, synclet.SyncModeIncremental, "items")
		for _, src := range []synclet.Source[json.RawMessage]{generic(t)} {
			out := read(t, src, cfg, cat, "")
			require.NoError(t, out.Err)
			for _, m := range out.Messages {
				if m.Type == synclet.MessageTypeRecord {
					continue
				}
				b, _ := json.Marshal(m)
				assert.NotContains(t, string(b), "MARKER-7d1f")
				assert.NotContains(t, string(b), "__manifest")
			}
		}
		badCfg := strings.Replace(cfg, `"items"`, `"items","oops":1`, 1)
		out := runCmd(t, generic(t), synclet.Request{Command: synclet.CommandDiscover, Config: json.RawMessage(badCfg)})
		msg, _ := traceMessage(out)
		assert.NotContains(t, msg, "MARKER-7d1f")
	})

	t.Run("the generic spec", func(t *testing.T) {
		spec := runCmd(t, generic(t), synclet.Request{Command: synclet.CommandSpec}).Spec()
		require.NotNil(t, spec)
		assert.Equal(t, []synclet.PropertyName{"__manifest"}, spec.ConnectionSpecification.Required)
	})
}
