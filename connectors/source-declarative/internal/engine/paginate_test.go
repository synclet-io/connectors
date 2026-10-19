package engine

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func paged(pagination string) string {
	return strings.Replace(itemsStream, "records: {path: $.data}", "records: {path: $.data}\n    pagination: "+pagination, 1)
}

func readPaged(t *testing.T, pagination string, a *fakeAPI) (ids []string, msg string) {
	t.Helper()
	src := newSource(t, manifestYAML("", paged(pagination)), a, newClock())
	out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
	msg, _ = traceMessage(out)
	return values(out, "items", "id"), msg
}

func TestPagination(t *testing.T) {
	abc := []string{`"a"`, `"b"`, `"c"`}
	tests := []struct {
		name       string
		pagination string
		api        func(a *fakeAPI)
		want       []string
		keys       []string
	}{
		{"page until an empty page", "{type: page, size: 1, size_param: per_page}", func(a *fakeAPI) {
			a.json("/v1/items?page=1&per_page=1", `{"data":[{"id":"a"}]}`).
				json("/v1/items?page=2&per_page=1", `{"data":[{"id":"b"}]}`).
				json("/v1/items?page=3&per_page=1", `{"data":[{"id":"c"}]}`).
				json("/v1/items?page=4&per_page=1", `{"data":[]}`)
		}, abc, []string{"/v1/items?page=1&per_page=1", "/v1/items?page=2&per_page=1", "/v1/items?page=3&per_page=1", "/v1/items?page=4&per_page=1"}},
		{"page from 0 with is_last", "{type: page, page_param: p, first: 0, is_last: $.last}", func(a *fakeAPI) {
			a.json("/v1/items?p=0", `{"data":[{"id":"a"}],"last":false}`).
				json("/v1/items?p=1", `{"data":[{"id":"b"}],"last":false}`).
				json("/v1/items?p=2", `{"data":[{"id":"c"}],"last":true}`)
		}, abc, nil},
		{"page with short_page", "{type: page, size: 2, size_param: n, short_page: true}", func(a *fakeAPI) {
			a.json("/v1/items?n=2&page=1", `{"data":[{"id":"a"},{"id":"b"}]}`).
				json("/v1/items?n=2&page=2", `{"data":[{"id":"c"}]}`)
		}, abc, nil},
		{"offset with total", "{type: offset, offset_param: startAt, size: 1, size_param: maxResults, total: $.total}", func(a *fakeAPI) {
			a.json("/v1/items?maxResults=1&startAt=0", `{"data":[{"id":"a"}],"total":3}`).
				json("/v1/items?maxResults=1&startAt=1", `{"data":[{"id":"b"}],"total":3}`).
				json("/v1/items?maxResults=1&startAt=2", `{"data":[{"id":"c"}],"total":3}`)
		}, abc, nil},
		{"cursor from the body", "{type: cursor, cursor_from: $.meta.next, cursor_param: after}", func(a *fakeAPI) {
			a.json("/v1/items", `{"data":[{"id":"a"}],"meta":{"next":"c2"}}`).
				json("/v1/items?after=c2", `{"data":[{"id":"b"}],"meta":{"next":"c3"}}`).
				json("/v1/items?after=c3", `{"data":[{"id":"c"}],"meta":{"next":null}}`)
		}, abc, nil},
		{"cursor from the last record with has_more", `{type: cursor, cursor_from: "last:id", cursor_param: starting_after, has_more: $.has_more, size: 1, size_param: limit}`, func(a *fakeAPI) {
			a.json("/v1/items?limit=1", `{"data":[{"id":"a"}],"has_more":true}`).
				json("/v1/items?limit=1&starting_after=a", `{"data":[{"id":"b"}],"has_more":true}`).
				json("/v1/items?limit=1&starting_after=b", `{"data":[{"id":"c"}],"has_more":false}`)
		}, abc, nil},
		{"cursor from a header", `{type: cursor, cursor_from: "header:X-Next", cursor_param: cursor}`, func(a *fakeAPI) {
			a.on("/v1/items", reply{status: 200, body: `{"data":[{"id":"a"}]}`, headers: map[string]string{"X-Next": "n2"}}).
				on("/v1/items?cursor=n2", reply{status: 200, body: `{"data":[{"id":"b"}]}`, headers: map[string]string{"X-Next": "n3"}}).
				json("/v1/items?cursor=n3", `{"data":[{"id":"c"}]}`)
		}, abc, nil},
		{"next_url relative with params merged", "{type: next_url, url_from: $.next_page, size: 2, size_param: page_size}", func(a *fakeAPI) {
			a.json("/v1/items?page_size=2", `{"data":[{"id":"a"}],"next_page":"/v1/items?starting_after=a"}`).
				json("/v1/items?starting_after=a", `{"data":[{"id":"b"}],"next_page":"items?starting_after=b"}`).
				json("/v1/items?starting_after=b", `{"data":[{"id":"c"}],"next_page":""}`)
		}, abc, nil},
		{"link header", "{type: link}", func(a *fakeAPI) {
			a.on("/v1/items", reply{status: 200, body: `{"data":[{"id":"a"}]}`, headers: map[string]string{"Link": `</v1/items?page=2>; rel="next", </v1/items?page=3>; rel="last"`}}).
				on("/v1/items?page=2", reply{status: 200, body: `{"data":[{"id":"b"}]}`, headers: map[string]string{"Link": `</v1/items?page=3>; rel="next"`}}).
				json("/v1/items?page=3", `{"data":[{"id":"c"}]}`)
		}, abc, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t)
			tt.api(a)
			ids, msg := readPaged(t, tt.pagination, a)
			require.Empty(t, msg)
			assert.Equal(t, tt.want, ids)
			if tt.keys != nil {
				assert.Equal(t, tt.keys, a.keys())
			}
		})
	}
}

func TestPaginationParamsMergeOnNextURL(t *testing.T) {
	a := newAPI(t).
		json("/v1/items?limit=100", `{"data":[{"id":"a"}],"next_page":"/v1/items?starting_after=a"}`).
		json("/v1/items?limit=100&starting_after=a", `{"data":[{"id":"b"}]}`)
	stream := strings.Replace(paged("{type: next_url, url_from: $.next_page}"), "    records:", "    params: {limit: 100}\n    records:", 1)
	src := newSource(t, manifestYAML("", stream), a, newClock())
	out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"/v1/items?limit=100", "/v1/items?limit=100&starting_after=a"}, a.keys())
}

func TestPaginationGuards(t *testing.T) {
	tests := []struct {
		name       string
		pagination string
		api        func(a *fakeAPI)
		msg        string
	}{
		{"repeated cursor", "{type: cursor, cursor_from: $.next, cursor_param: after}", func(a *fakeAPI) {
			a.json("/v1/items", `{"data":[{"id":"a"}],"next":"x"}`).
				json("/v1/items?after=x", `{"data":[{"id":"b"}],"next":"x"}`)
		}, `pagination repeated cursor "x" (GET /v1/items?after=x)`},
		{"identical page", "{type: page}", func(a *fakeAPI) {
			a.json("/v1/items?page=1", `{"data":[{"id":"a"}]}`).
				json("/v1/items?page=2", `{"data":[{"id":"a"}]}`)
		}, "page 2 repeated page 1 (GET /v1/items?page=2)"},
		{"repeated records", "{type: page}", func(a *fakeAPI) {
			a.json("/v1/items?page=1", `{"data":[{"id":"a","n":1}]}`).
				json("/v1/items?page=2", `{"data":[{"id":"a","n":2}]}`)
		}, "page 2 repeats the records of page 1 (GET /v1/items?page=2)"},
		{"max_pages", "{type: page, max_pages: 2}", func(a *fakeAPI) {
			a.json("/v1/items?page=1", `{"data":[{"id":"a"}]}`).
				json("/v1/items?page=2", `{"data":[{"id":"b"}]}`)
		}, "max_pages 2 reached (GET /v1/items?page=2)"},
		{"has_more missing", "{type: cursor, cursor_from: \"last:id\", cursor_param: after, has_more: $.has_more}", func(a *fakeAPI) {
			a.json("/v1/items", `{"data":[{"id":"a"}]}`)
		}, "has_more $.has_more is not in the response (GET /v1/items)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t)
			tt.api(a)
			_, msg := readPaged(t, tt.pagination, a)
			assert.Contains(t, msg, tt.msg)
		})
	}

	t.Run("a partial overlap passes", func(t *testing.T) {
		a := newAPI(t).
			json("/v1/items?page=1", `{"data":[{"id":"a"},{"id":"b"}]}`).
			json("/v1/items?page=2", `{"data":[{"id":"b"},{"id":"c"}]}`).
			json("/v1/items?page=3", `{"data":[]}`)
		ids, msg := readPaged(t, "{type: page}", a)
		require.Empty(t, msg)
		assert.Equal(t, []string{`"a"`, `"b"`, `"b"`, `"c"`}, ids)
	})
}

func TestRecords(t *testing.T) {
	tests := []struct {
		name    string
		records string
		body    string
		field   string
		want    []string
		msg     string
	}{
		{"wrapped elements", `{path: "$.list[*].customer"}`, `{"list":[{"customer":{"id":"a"}},{"customer":{"id":"b"}}]}`, "id", []string{`"a"`, `"b"`}, ""},
		{"top-level array", `{}`, `[{"id":"a"}]`, "id", []string{`"a"`}, ""},
		{"one object", `{path: $.item, mode: object}`, `{"item":{"id":"a"}}`, "id", []string{`"a"`}, ""},
		{"entries with scalars sorted by key", `{path: $.rates, mode: entries, key_field: id, value_field: value, add: {base: $.base}}`,
			`{"base":"USD","rates":{"EUR":0.912345678901234567,"AED":3.6725}}`, "value", []string{"3.6725", "0.912345678901234567"}, ""},
		{"entries with objects", `{path: $.m, mode: entries, key_field: id}`, `{"m":{"b":{"name":"B"},"a":{"name":"A"}}}`, "name", []string{`"A"`, `"B"`}, ""},
		{"exact numbers", `{path: $.data}`, `{"data":[{"id":"a","value":12345678901234567890}]}`, "value", []string{"12345678901234567890"}, ""},
		{"add only when missing or null", `{path: $.data, add: {name: $.default, account: "{{ config.api_key }}", day: 7}}`,
			`{"default":"D","data":[{"id":"a","name":"kept"},{"id":"b","name":null},{"id":"c"}]}`, "name", []string{`"kept"`, `"D"`, `"D"`}, ""},
		{"missing path", `{path: $.data}`, `{"items":[]}`, "id", nil, "records path $.data is not in the response (GET /v1/items)"},
		{"null element", `{path: $.data}`, `{"data":[{"id":"a"},null]}`, "id", nil, "record 1 is null, not an object (GET /v1/items)"},
		{"not an array", `{path: $.data}`, `{"data":{"id":"a"}}`, "id", nil, "records at $.data are not an array (GET /v1/items)"},
		{"null body", `{}`, `null`, "id", nil, "records at $ are not an array"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t).json("/v1/items", tt.body)
			stream := strings.Replace(itemsStream, "records: {path: $.data}", "records: "+tt.records, 1)
			src := newSource(t, manifestYAML("", stream), a, newClock())
			out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
			msg, _ := traceMessage(out)
			if tt.msg != "" {
				assert.Contains(t, msg, tt.msg)
				assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: "items"}))
				return
			}
			require.Empty(t, msg)
			assert.Equal(t, tt.want, values(out, "items", tt.field))
		})
	}

	t.Run("entries add the envelope and constants", func(t *testing.T) {
		a := newAPI(t).json("/v1/items", `{"base":"USD","rates":{"EUR":1}}`)
		stream := strings.Replace(itemsStream, "records: {path: $.data}", `records: {path: $.rates, mode: entries, key_field: id, value_field: value, add: {base: $.base, account: "{{ config.api_key }}", day: 7}}`, 1)
		src := newSource(t, manifestYAML("", stream), a, newClock())
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
		recs := out.Records(synclet.StreamDescriptor{Name: "items"})
		require.Len(t, recs, 1)
		assert.JSONEq(t, `{"id":"EUR","value":1,"base":"USD","account":"k1","day":7}`, string(recs[0].Data))
	})
}
