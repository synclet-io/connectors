package engine

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"

	"source-declarative/internal/manifest"
)

func TestAuthQueryKeepsKeyOutOfTransportErrors(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	c := newClock()
	m := load(t, manifestYAML(`auth: {type: api_key, in: query, name: key, value: "{{ config.api_key }}"}`+"\n", itemsStream))
	src := New(m, Options{BaseURL: "http://" + addr + "/v1/", Now: c.Now, Sleep: c.Sleep})
	out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
	msg, _ := traceMessage(out)
	require.NotEmpty(t, msg)
	assert.Contains(t, msg, "GET /v1/items")
	assert.NotContains(t, msg, "k1")
	assert.NotContains(t, msg, "key=")
}

func TestNextURLStaysOnBaseHost(t *testing.T) {
	var hits atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"data":[]}`))
	}))
	t.Cleanup(other.Close)
	tests := []struct {
		name       string
		pagination string
		api        func(a *fakeAPI)
	}{
		{"next_url", "{type: next_url, url_from: $.next}", func(a *fakeAPI) {
			a.json("/v1/items", `{"data":[{"id":"a"}],"next":"`+other.URL+`/v1/items?page=2"}`)
		}},
		{"link header", "{type: link}", func(a *fakeAPI) {
			a.on("/v1/items", reply{status: 200, body: `{"data":[{"id":"a"}]}`, headers: map[string]string{"Link": `<` + other.URL + `/v1/items?page=2>; rel="next"`}})
		}},
		{"other scheme", "{type: next_url, url_from: $.next}", func(a *fakeAPI) {
			a.json("/v1/items", `{"data":[{"id":"a"}],"next":"`+strings.Replace(a.srv.URL, "http://", "https://", 1)+`/v1/items?page=2"}`)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t)
			tt.api(a)
			_, msg := readPaged(t, tt.pagination, a)
			assert.Contains(t, msg, "is not on the base URL's scheme and host")
			assert.Equal(t, int32(0), hits.Load())
			assert.Len(t, a.keys(), 1)
		})
	}
}

func TestMoreWithoutNextValueFails(t *testing.T) {
	tests := []struct {
		name       string
		pagination string
		body       string
		msg        string
	}{
		{"last record without the cursor field", `{type: cursor, cursor_from: "last:id", cursor_param: after, has_more: $.has_more}`,
			`{"data":[{"name":"x"}],"has_more":true}`, "the page says more exist but has no next value (GET /v1/items)"},
		{"missing body cursor with is_last false", "{type: cursor, cursor_from: $.next, cursor_param: after, is_last: $.last}",
			`{"data":[{"id":"a"}],"last":false}`, "the page says more exist but has no next value"},
		{"missing next_url with has_more", "{type: next_url, url_from: $.next, has_more: $.more}",
			`{"data":[{"id":"a"}],"more":true}`, "the page says more exist but has no next value"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t).json("/v1/items", tt.body)
			_, msg := readPaged(t, tt.pagination, a)
			assert.Contains(t, msg, tt.msg)
		})
	}

	t.Run("no has-more signal stops", func(t *testing.T) {
		a := newAPI(t).json("/v1/items", `{"data":[{"id":"a"}]}`)
		ids, msg := readPaged(t, "{type: cursor, cursor_from: $.next, cursor_param: after}", a)
		require.Empty(t, msg)
		assert.Equal(t, []string{`"a"`}, ids)
	})
}

func TestDuplicatePartitionValues(t *testing.T) {
	cfg := `{"api_key":"k1","start_date":"2026-10-01T00:00:00Z","accounts":["a1","a2","a1"]}`
	a := newAPI(t).
		json("/v1/accounts/a1/items", `{"data":[]}`).
		json("/v1/accounts/a2/items", `{"data":[]}`)
	src := newSource(t, manifestYAML("", valuesStream), a, newClock())

	st := checkStatus(t, src, cfg)
	assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
	assert.Contains(t, st.Message, `config.accounts lists "a1" twice`)

	out := read(t, src, cfg, catalog(t, src, testConfig, synclet.SyncModeIncremental, "items"), "")
	msg, ft := traceMessage(out)
	assert.Equal(t, synclet.FailureTypeConfigError, ft)
	assert.Contains(t, msg, `config.accounts lists "a1" twice`)
	assert.Empty(t, a.keys())
}

func TestRetryAfterCap(t *testing.T) {
	a := newAPI(t).on("/v1/items", reply{status: 429, headers: map[string]string{"Retry-After": "3600"}})
	c := newClock()
	msg, _, ids := readItems(t, "", a, c)
	assert.Empty(t, c.waits())
	assert.Empty(t, ids)
	assert.Contains(t, msg, "GET /v1/items (HTTP 429)")
	assert.Contains(t, msg, "asks to wait 1h0m0s, more than the 10m0s limit")
}

func TestParseCursorUnixIsExact(t *testing.T) {
	tests := []struct {
		v      any
		format string
		want   time.Time
	}{
		{json.Number("1700000000123"), manifest.FormatUnixMS, time.UnixMilli(1700000000123).UTC()},
		{"1700000000999", manifest.FormatUnixMS, time.UnixMilli(1700000000999).UTC()},
		{json.Number("1700000000"), manifest.FormatUnixS, time.Unix(1700000000, 0).UTC()},
		{json.Number("1700000000.5"), manifest.FormatUnixS, time.Unix(1700000000, 5e8).UTC()},
	}
	for _, tt := range tests {
		got, err := parseCursor(tt.v, tt.format)
		require.NoError(t, err)
		assert.True(t, tt.want.Equal(got), "%v %s: got %s, want %s", tt.v, tt.format, got.Format(time.RFC3339Nano), tt.want.Format(time.RFC3339Nano))
	}
}
