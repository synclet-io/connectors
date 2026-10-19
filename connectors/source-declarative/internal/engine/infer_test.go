package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// inferOut is the parsed stdout of one infer.
type inferOut struct {
	raw  string
	msgs []*synclet.Message
	err  error
}

func infer(t *testing.T, src synclet.Source[json.RawMessage], req InferRequest) *inferOut {
	t.Helper()
	if req.Limit == 0 {
		req.Limit = 100
	}
	if req.Pages == 0 {
		req.Pages = 3
	}
	var buf bytes.Buffer
	err := Infer(t.Context(), src, req, &buf)
	out := &inferOut{raw: buf.String(), err: err}
	r := synclet.NewMessageReader(strings.NewReader(buf.String()))
	for {
		m, rerr := r.Next()
		if errors.Is(rerr, io.EOF) {
			break
		}
		require.NoError(t, rerr)
		out.msgs = append(out.msgs, m)
	}
	return out
}

func (o *inferOut) records(stream string) []string {
	var out []string
	for _, m := range o.msgs {
		if m.Type == synclet.MessageTypeRecord && m.Record.Stream == stream {
			out = append(out, string(m.Record.Data))
		}
	}
	return out
}

func (o *inferOut) logs(level synclet.LogLevel) []string {
	var out []string
	for _, m := range o.msgs {
		if m.Type == synclet.MessageTypeLog && m.Log.Level == level {
			out = append(out, m.Log.Message)
		}
	}
	return out
}

func (o *inferOut) traces() []*synclet.ErrorTrace {
	var out []*synclet.ErrorTrace
	for _, m := range o.msgs {
		if m.Type == synclet.MessageTypeTrace && m.Trace.Error != nil {
			out = append(out, m.Trace.Error)
		}
	}
	return out
}

// catalog returns the CATALOG, which must be the last message.
func (o *inferOut) catalog(t *testing.T) *synclet.Catalog {
	t.Helper()
	require.NotEmpty(t, o.msgs)
	last := o.msgs[len(o.msgs)-1]
	require.Equal(t, synclet.MessageTypeCatalog, last.Type, "last message: %s", o.raw)
	for _, m := range o.msgs[:len(o.msgs)-1] {
		require.NotEqual(t, synclet.MessageTypeCatalog, m.Type)
	}
	return last.Catalog
}

func streamOf(t *testing.T, cat *synclet.Catalog, name string) synclet.Stream {
	t.Helper()
	for _, s := range cat.Streams {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("stream %s not in catalog", name)
	return synclet.Stream{}
}

// schemalessStreams has two streams without schema: items pages by cursor
// and is incremental, tags is a plain list.
const schemalessStreams = `  - name: items
    path: items
    params: {"updated[gte]": "{{ since }}"}
    primary_key: [id]
    records: {path: $.data}
    pagination: {type: cursor, cursor_from: "last:id", cursor_param: after, has_more: $.has_more}
    incremental: {cursor_field: updated, cursor_format: unix_s, start: "{{ config.start_date }}"}
  - name: tags
    path: tags
    primary_key: [id]
    records: {add: {source: "{{ config.region }}"}}
`

func itemPage(ids ...string) string {
	var recs []string
	for i, id := range ids {
		recs = append(recs, fmt.Sprintf(`{"id":%q,"updated":%d,"amount":%d.5,"at":"2026-10-0%dT10:00:00Z"}`, id, startUnix+int64(i), i, i+1))
	}
	return fmt.Sprintf(`{"data":[%s],"has_more":true}`, strings.Join(recs, ","))
}

func pagedAPI(t *testing.T) *fakeAPI {
	return newAPI(t).
		on("/v1/items",
			reply{status: 200, body: itemPage("a", "b")},
			reply{status: 200, body: itemPage("c", "d")},
			reply{status: 200, body: itemPage("e", "f")},
			reply{status: 200, body: itemPage("g", "h")}).
		json("/v1/tags", `[{"id":"t1","n":1},{"id":"t2","n":null}]`)
}

func generic(a *fakeAPI) synclet.Source[json.RawMessage] { return NewGeneric(a.options(newClock())) }

const inferConfig = `{"api_key":"k1","start_date":"2026-10-01T00:00:00Z","region":"eu"}`

func TestInferSamplesAndDrafts(t *testing.T) {
	a := pagedAPI(t)
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, inferConfig, manifestYAML("", schemalessStreams))), Limit: 5, Pages: 3})
	require.NoError(t, out.err)
	assert.Empty(t, out.traces())

	assert.Len(t, out.records("items"), 5)
	assert.Len(t, out.records("tags"), 2)
	assert.JSONEq(t, `{"id":"t1","n":1,"source":"eu"}`, out.records("tags")[0])

	var itemReqs []string
	for _, k := range a.keys() {
		if strings.HasPrefix(k, "/v1/items") {
			itemReqs = append(itemReqs, k)
		}
	}
	assert.Len(t, itemReqs, 3, "pages cap the requests")
	assert.Contains(t, itemReqs[0], fmt.Sprintf("updated%%5Bgte%%5D=%d", startUnix), "full refresh from start")

	var reqLogs int
	for _, l := range out.logs(synclet.LogLevelInfo) {
		if strings.Contains(l, "GET /v1/items") {
			reqLogs++
		}
	}
	assert.Equal(t, 3, reqLogs, "a LOG per request")

	cat := out.catalog(t)
	require.Len(t, cat.Streams, 2)
	items := streamOf(t, cat, "items")
	assert.JSONEq(t, `{"type":"object","additionalProperties":true,"properties":{
		"amount":{"type":"number"},
		"at":{"type":"string","format":"date-time"},
		"id":{"type":"string"},
		"updated":{"type":"integer"}}}`, string(items.JSONSchema))
	assert.Equal(t, [][]string{{"id"}}, items.SourceDefinedPrimaryKey)
	assert.Equal(t, []string{"updated"}, items.DefaultCursorField)
	assert.True(t, items.SourceDefinedCursor)
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}, items.SupportedSyncModes)
	assert.JSONEq(t, `{"type":"object","additionalProperties":true,"properties":{
		"id":{"type":"string"},"n":{"type":["null","integer"]},"source":{"type":"string"}}}`, string(streamOf(t, cat, "tags").JSONSchema))
}

func TestInferLimitStopsRequests(t *testing.T) {
	a := pagedAPI(t)
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, inferConfig, manifestYAML("", schemalessStreams))), Streams: []string{"items"}, Limit: 3, Pages: 10})
	require.NoError(t, out.err)
	assert.Len(t, out.records("items"), 3)
	assert.Empty(t, out.records("tags"))
	assert.Equal(t, []string{"items"}, func() []string {
		var n []string
		for _, s := range out.catalog(t).Streams {
			n = append(n, s.Name)
		}
		return n
	}())
	assert.Len(t, a.keys(), 2, "no request after the limit is reached")
}

func TestInferSameSamplesSameDraft(t *testing.T) {
	cfg := json.RawMessage(withManifest(t, inferConfig, manifestYAML("", schemalessStreams)))
	first := infer(t, generic(pagedAPI(t)), InferRequest{Config: cfg}).catalog(t)
	for range 5 {
		assert.Equal(t, first, infer(t, generic(pagedAPI(t)), InferRequest{Config: cfg}).catalog(t))
	}
}

func TestInferCatalogImage(t *testing.T) {
	a := pagedAPI(t)
	for name, cfg := range map[string]string{
		"without __manifest": inferConfig,
		"with __manifest":    withManifest(t, inferConfig, manifestYAML("", schemalessStreams)),
	} {
		t.Run(name, func(t *testing.T) {
			out := infer(t, newSource(t, manifestYAML("", `  - name: items
    path: items
    schema: `+itemSchema+"\n"), a, newClock()), InferRequest{Config: json.RawMessage(cfg)})
			require.Error(t, out.err)
			require.Len(t, out.traces(), 1)
			assert.Equal(t, synclet.FailureTypeConfigError, out.traces()[0].FailureType)
			assert.Contains(t, out.traces()[0].Message, "generic source-declarative image")
			assert.Empty(t, out.records("items"))
			assert.Empty(t, a.keys())
		})
	}
}

func TestInferConfigErrors(t *testing.T) {
	cfg := withManifest(t, inferConfig, manifestYAML("", schemalessStreams))
	for name, c := range map[string]struct {
		req  InferRequest
		want string
	}{
		"no __manifest":    {InferRequest{Config: json.RawMessage(inferConfig)}, "`__manifest` is required"},
		"unknown stream":   {InferRequest{Config: json.RawMessage(cfg), Streams: []string{"nope"}}, `unknown stream "nope"`},
		"invalid config":   {InferRequest{Config: json.RawMessage(withManifest(t, `{"start_date":"x"}`, manifestYAML("", schemalessStreams)))}, "config: missing property 'api_key'"},
		"invalid manifest": {InferRequest{Config: json.RawMessage(withManifest(t, inferConfig, strings.Replace(manifestYAML("", schemalessStreams), "type: cursor", "type: pages", 1)))}, "__manifest.streams[0].pagination"},
	} {
		t.Run(name, func(t *testing.T) {
			a := pagedAPI(t)
			out := infer(t, generic(a), c.req)
			require.Error(t, out.err)
			require.Len(t, out.traces(), 1)
			assert.Equal(t, synclet.FailureTypeConfigError, out.traces()[0].FailureType)
			assert.Contains(t, out.traces()[0].Message, c.want)
			assert.Empty(t, a.keys())
			for _, m := range out.msgs {
				assert.NotEqual(t, synclet.MessageTypeCatalog, m.Type)
			}
		})
	}
}

func TestInferSchemalessOnlyInInfer(t *testing.T) {
	cfg := json.RawMessage(withManifest(t, inferConfig, manifestYAML("", schemalessStreams)))
	src := generic(pagedAPI(t))
	st := checkStatus(t, src, string(cfg))
	assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
	assert.Contains(t, st.Message, "__manifest.streams[0].schema: required key is missing")
	out := runCmd(t, src, synclet.Request{Command: synclet.CommandDiscover, Config: cfg})
	msg, ft := traceMessage(out)
	assert.Equal(t, synclet.FailureTypeConfigError, ft)
	assert.Contains(t, msg, "__manifest.streams[1].schema: required key is missing")
}

func TestInferRelaxedLintWarnings(t *testing.T) {
	a := newAPI(t).
		on("/v1/items", reply{status: 200, body: `{"data":[{"id":"a","updated":` + fmt.Sprint(startUnix) + `}],"has_more":false}`}).
		json("/v1/tags", `[]`)
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, inferConfig, manifestYAML("", schemalessStreams)))})
	require.NoError(t, out.err)
	assert.Equal(t, []string{
		"tags: no records sampled; the draft schema is empty",
		`draft schema of tags: __manifest.streams[1].primary_key[0]: field "id" is not in the stream's schema`,
		`draft schema of tags: __manifest.streams[1].records.add.source: field "source" is not in the stream's schema`,
	}, out.logs(synclet.LogLevelWarn))
	assert.Len(t, out.catalog(t).Streams, 2)
}

func TestInferRecordWithoutPrimaryKeyFailsItsStream(t *testing.T) {
	streams := strings.Replace(schemalessStreams, "primary_key: [id]\n    records: {add", "primary_key: [code]\n    records: {add", 1)
	out := infer(t, generic(pagedAPI(t)), InferRequest{Config: json.RawMessage(withManifest(t, inferConfig, manifestYAML("", streams)))})
	require.NoError(t, out.err)
	require.Len(t, out.traces(), 1)
	assert.Equal(t, "tags", out.traces()[0].StreamDescriptor.Name)
	assert.Contains(t, out.traces()[0].Message, "code")
	cat := out.catalog(t)
	require.Len(t, cat.Streams, 1)
	assert.Equal(t, "items", cat.Streams[0].Name)
}

func TestInferEmptyStream(t *testing.T) {
	a := newAPI(t).json("/v1/items", `{"data":[],"has_more":false}`).json("/v1/tags", `[]`)
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, inferConfig, manifestYAML("", schemalessStreams)))})
	require.NoError(t, out.err)
	cat := out.catalog(t)
	assert.JSONEq(t, `{"type":"object","properties":{},"additionalProperties":true}`, string(streamOf(t, cat, "items").JSONSchema))
	warns := strings.Join(out.logs(synclet.LogLevelWarn), "\n")
	assert.Contains(t, warns, "items: no records sampled; the draft schema is empty")
	assert.Contains(t, warns, "tags: no records sampled; the draft schema is empty")
}

func TestInferFailingStream(t *testing.T) {
	a := newAPI(t).
		on("/v1/items", reply{status: 400, body: `{"error":"bad request"}`}).
		json("/v1/tags", `[{"id":"t1"}]`)
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, inferConfig, manifestYAML("", schemalessStreams)))})
	require.NoError(t, out.err, "a stream failure is not a command failure")
	tr := out.traces()
	require.Len(t, tr, 1)
	require.NotNil(t, tr[0].StreamDescriptor)
	assert.Equal(t, "items", tr[0].StreamDescriptor.Name)
	assert.Contains(t, tr[0].Message, "HTTP 400")
	assert.Equal(t, synclet.FailureTypeSystemError, tr[0].FailureType)
	assert.Len(t, out.records("tags"), 1)
	cat := out.catalog(t)
	require.Len(t, cat.Streams, 1)
	assert.Equal(t, "tags", cat.Streams[0].Name)
}

func TestInferPartitionsAndParents(t *testing.T) {
	streams := `  - name: items
    path: items
    primary_key: [id]
    records: {path: $.data}
  - name: per_account
    path: "accounts/{{ partition.value }}/items"
    partition: {values: "{{ config.accounts }}"}
    records: {path: $.data, add: {account: "{{ partition.value }}"}}
  - name: children
    path: "items/{{ parent.id }}/children"
    partition: {parent: items}
    records: {path: $.data, add: {parent_id: "{{ parent.id }}"}}
`
	a := newAPI(t).
		json("/v1/items", `{"data":[{"id":"p1"},{"id":"p2"},{"id":"p3"}]}`).
		json("/v1/accounts/acc1/items", `{"data":[{"id":"x"}]}`).
		json("/v1/items/p1/children", `{"data":[{"id":"c1"},{"id":"c2"}]}`).
		json("/v1/items/p2/children", `{"data":[{"id":"c3"},{"id":"c4"}]}`)
	cfg := `{"api_key":"k1","accounts":["acc1","acc2"]}`
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, cfg, manifestYAML("", streams))), Limit: 3})
	require.NoError(t, out.err)
	assert.Empty(t, out.traces())
	assert.Equal(t, []string{`{"account":"acc1","id":"x"}`}, out.records("per_account"), "the first partition value only")
	assert.Equal(t, []string{
		`{"id":"c1","parent_id":"p1"}`, `{"id":"c2","parent_id":"p1"}`, `{"id":"c3","parent_id":"p2"}`,
	}, out.records("children"), "the first parents up to the limit")
	assert.NotContains(t, a.keys(), "/v1/items/p3/children")
	assert.NotContains(t, a.keys(), "/v1/accounts/acc2/items")
	assert.Len(t, out.records("items"), 3)
	assert.Len(t, out.catalog(t).Streams, 3)
}

func TestInferFirstWindowOnly(t *testing.T) {
	streams := strings.Replace(strings.Replace(dayStream, "    schema: "+itemSchema+"\n", "", 1), ", closed_only: true", "", 1)
	a := newAPI(t).json("/v1/days/2026-10-15.json", `{"data":[{"id":"x"}]}`)
	cfg := `{"api_key":"k1","start_date":"2026-10-15"}`
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, cfg, strings.Replace(manifestYAML("check: {stream: days}\n", streams), "check: {stream: items}\n", "", 1)))})
	require.NoError(t, out.err)
	assert.Equal(t, []string{"/v1/days/2026-10-15.json"}, a.keys())
	assert.Equal(t, []string{`{"day":"2026-10-15","id":"x"}`}, out.records("days"))
}

func TestInferRecordsMatchRead(t *testing.T) {
	cat := catalog(t, bakedSource(t, twinAPI(t)), testConfig, synclet.SyncModeFullRefresh, "items", "tags")
	want := outcome(t, read(t, bakedSource(t, twinAPI(t)), testConfig, cat, ""))
	out := infer(t, generic(twinAPI(t)), InferRequest{Config: json.RawMessage(withManifest(t, testConfig, inlineYAML))})
	require.NoError(t, out.err)
	got := map[string][]any{}
	for _, name := range []string{"items", "tags"} {
		for _, r := range out.records(name) {
			var v any
			require.NoError(t, json.Unmarshal([]byte(r), &v))
			got[name] = append(got[name], v)
		}
	}
	assert.Equal(t, want, got)
}

func TestInferNeverShowsSecrets(t *testing.T) {
	const secret = "s3cr3t-Value-42"
	streams := `  - name: items
    path: items
    records: {path: $.data}
    pagination: {type: cursor, cursor_from: "last:id", cursor_param: after, has_more: $.has_more}
  - name: broken
    path: broken
    records: {path: $.data}
`
	top := `auth: {type: api_key, in: query, name: key, value: "{{ config.api_key }}"}` + "\n"
	a := newAPI(t).
		on("/v1/items",
			reply{status: 200, body: `{"data":[{"id":"a"}],"has_more":true}`},
			reply{status: 200, body: `{"data":[{"id":"b"}],"has_more":false}`}).
		on("/v1/broken", reply{status: 403, body: `{"error":"forbidden"}`})
	cfg := fmt.Sprintf(`{"api_key":%q}`, secret)
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, cfg, manifestYAML(top, streams)))})
	require.NoError(t, out.err)
	assert.Len(t, out.records("items"), 2)
	require.Len(t, out.traces(), 1)
	assert.NotContains(t, out.raw, secret)
	for _, l := range out.logs(synclet.LogLevelInfo) {
		if strings.Contains(l, "GET /v1/items") {
			assert.NotContains(t, l, "?", "no query when the key travels in it")
		}
	}
	assert.Contains(t, a.keys()[0], "key="+secret, "the key is sent")
}

func TestInferChildStopsAfterLimitParents(t *testing.T) {
	streams := `  - name: items
    path: items
    primary_key: [id]
    records: {path: $.data}
  - name: children
    path: "items/{{ parent.id }}/children"
    partition: {parent: items}
    records: {path: $.data}
`
	var parents []string
	a := newAPI(t)
	for i := range 50 {
		parents = append(parents, fmt.Sprintf(`{"id":"p%d"}`, i))
		a.json(fmt.Sprintf("/v1/items/p%d/children", i), `{"data":[]}`)
	}
	a.json("/v1/items", `{"data":[`+strings.Join(parents, ",")+`]}`)
	out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, inferConfig, manifestYAML("", streams))), Streams: []string{"children"}, Limit: 5, Pages: 1})
	require.NoError(t, out.err)
	assert.Empty(t, out.traces())
	var child int
	for _, k := range a.keys() {
		if strings.HasSuffix(k, "/children") {
			child++
		}
	}
	assert.LessOrEqual(t, child, 5)
}

func TestInferPageCap(t *testing.T) {
	streams := `  - name: items
    path: items
    primary_key: [id]
    records: {path: $.data}
    pagination: {type: cursor, cursor_from: "last:id", cursor_param: after, has_more: $.has_more}
  - name: children
    path: "items/{{ parent.id }}/children"
    partition: {parent: items}
    records: {path: $.data}
    pagination: {type: cursor, cursor_from: "last:id", cursor_param: after, has_more: $.has_more}
`
	a := newAPI(t).
		on("/v1/items",
			reply{status: 200, body: `{"data":[{"id":"p1"}],"has_more":true}`},
			reply{status: 200, body: `{"data":[{"id":"p2"}],"has_more":true}`},
			reply{status: 200, body: `{"data":[{"id":"p3"}],"has_more":false}`})
	for _, p := range []string{"p1", "p2", "p3"} {
		a.on("/v1/items/"+p+"/children",
			reply{status: 200, body: `{"data":[{"id":"` + p + `a"}],"has_more":true}`},
			reply{status: 200, body: `{"data":[{"id":"` + p + `b"}],"has_more":false}`})
	}
	cfg := json.RawMessage(withManifest(t, inferConfig, manifestYAML("", streams)))

	t.Run("stream", func(t *testing.T) {
		out := infer(t, generic(a), InferRequest{Config: cfg, Streams: []string{"items"}, Pages: 2})
		require.NoError(t, out.err)
		assert.Equal(t, []string{`{"id":"p1"}`, `{"id":"p2"}`}, out.records("items"))
	})
	t.Run("parent and child", func(t *testing.T) {
		b := newAPI(t)
		b.replies = a.replies
		out := infer(t, generic(b), InferRequest{Config: cfg, Streams: []string{"children"}, Pages: 1})
		require.NoError(t, out.err)
		assert.Equal(t, []string{`{"id":"p1a"}`}, out.records("children"), "one parent page, one page per child")
	})
}
