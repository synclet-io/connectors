package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// poll answers a BulkPoll of operation id with status and the given
// fields of the operation, for example `"objectCount":"5"`.
func poll(id, status string, fields ...string) http.HandlerFunc {
	body := `{"id":"gid://shopify/BulkOperation/` + id + `","status":"` + status + `","errorCode":null,"objectCount":"0","rootObjectCount":"0","url":null,"partialDataUrl":null`
	var b strings.Builder
	b.WriteString(`{"data":{"bulkOperation":{`)
	seen := map[string]bool{}
	for _, f := range fields {
		b.WriteString(f + ",")
		seen[f[:strings.Index(f, ":")]] = true
	}
	for _, f := range strings.Split(strings.TrimPrefix(body, "{"), ",") {
		if !seen[f[:strings.Index(f, ":")]] {
			b.WriteString(f + ",")
		}
	}
	return func(w http.ResponseWriter, r *http.Request) {
		s := strings.TrimSuffix(b.String(), ",") + `}}}`
		s = strings.ReplaceAll(s, "{{base_url}}", "http://"+r.Host)
		jsonBody(http.StatusOK, s)(w, r)
	}
}

// ordersOutput is a completed poll of the orders operation 1004 with the
// counts of testdata/bulk/orders.jsonl and url pointing at file.
func ordersOutput(file string) http.HandlerFunc {
	return poll("1004", "COMPLETED", `"objectCount":"5"`, `"rootObjectCount":"2"`, `"url":"{{base_url}}/bulk/`+file+`"`)
}

func TestBulk(t *testing.T) {
	const pollKey = "BulkPoll-1004"
	tests := []struct {
		name      string
		overrides map[string]http.HandlerFunc
		// want is what the read error contains, empty when it succeeds.
		want    []string
		records int
		state   []string
		polls   int
	}{
		{name: "complete", records: 2, state: []string{`{"cursor":"2026-10-18T11:59:00Z"}`}, polls: 1},
		{
			name: "running then completed", records: 2, state: []string{`{"cursor":"2026-10-18T11:59:00Z"}`}, polls: 3,
			overrides: map[string]http.HandlerFunc{pollKey: sequence(poll("1004", "CREATED"), poll("1004", "RUNNING"), ordersOutput("orders.jsonl"))},
		},
		{
			name: "failed with partial data",
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "FAILED", `"errorCode":"TIMEOUT"`, `"objectCount":"3"`,
				`"partialDataUrl":"{{base_url}}/bulk/orders.jsonl"`)},
			want: []string{"stream orders: window [2026-10-01T00:00:00Z, 2026-10-18T11:59:00Z): bulk operation gid://shopify/BulkOperation/1004 status FAILED errorCode TIMEOUT: did not complete; its partial data is not read"},
		},
		{
			name:      "canceled",
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "CANCELED")},
			want:      []string{"bulk operation gid://shopify/BulkOperation/1004 status CANCELED: did not complete"},
		},
		{
			name:      "canceling",
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "CANCELING")},
			want:      []string{"status CANCELING: did not complete"},
		},
		{
			name:      "expired",
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "EXPIRED")},
			want:      []string{"status EXPIRED: did not complete"},
		},
		{
			name: "completed with partial data",
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "COMPLETED", `"objectCount":"5"`, `"rootObjectCount":"2"`,
				`"url":"{{base_url}}/bulk/orders.jsonl"`, `"partialDataUrl":"{{base_url}}/bulk/orders.jsonl"`)},
			want: []string{"status COMPLETED: completed with partial data only"},
		},
		{
			name: "fewer lines than objectCount",
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "COMPLETED", `"objectCount":"6"`, `"rootObjectCount":"2"`,
				`"url":"{{base_url}}/bulk/orders.jsonl"`)},
			want: []string{"status COMPLETED: output has 5 objects, 2 of them roots; the operation reports objectCount 6 and rootObjectCount 2"},
		},
		{
			name: "fewer roots than rootObjectCount",
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "COMPLETED", `"objectCount":"5"`, `"rootObjectCount":"3"`,
				`"url":"{{base_url}}/bulk/orders.jsonl"`)},
			want: []string{"output has 5 objects, 2 of them roots; the operation reports objectCount 5 and rootObjectCount 3"},
		},
		{
			name: "truncated last line",
			overrides: map[string]http.HandlerFunc{
				"bulk/orders.jsonl": func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"id":"gid://shopify/Order/6001","updatedAt":"2026-10-02T09:00:00Z"}` + "\n" + `{"id":"gid://shopify/Order/6002","upd`))
				},
				pollKey: poll("1004", "COMPLETED", `"objectCount":"2"`, `"rootObjectCount":"2"`, `"url":"{{base_url}}/bulk/orders.jsonl"`),
			},
			want: []string{"output line 2 has no final newline; the output is truncated"},
		},
		{
			name: "line not JSON",
			overrides: map[string]http.HandlerFunc{
				"bulk/orders.jsonl": func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("not json\n")) },
				pollKey:             poll("1004", "COMPLETED", `"objectCount":"1"`, `"rootObjectCount":"1"`, `"url":"{{base_url}}/bulk/orders.jsonl"`),
			},
			want: []string{"output line 1 is not a JSON object"},
		},
		{
			name: "orphan child",
			overrides: map[string]http.HandlerFunc{
				"bulk/orders.jsonl": func(w http.ResponseWriter, _ *http.Request) {
					_, _ = w.Write([]byte(`{"id":"gid://shopify/Order/6001","updatedAt":"2026-10-02T09:00:00Z"}` + "\n" +
						`{"id":"gid://shopify/LineItem/7009","__parentId":"gid://shopify/Order/6999"}` + "\n"))
				},
				pollKey: poll("1004", "COMPLETED", `"objectCount":"2"`, `"rootObjectCount":"1"`, `"url":"{{base_url}}/bulk/orders.jsonl"`),
			},
			want: []string{`output line 2 is a child of gid://shopify/Order/6999, which is not the root before it ("gid://shopify/Order/6001")`},
		},
		{
			name: "no url and no objects", records: 0, state: []string{`{"cursor":"2026-10-18T11:59:00Z"}`}, polls: 1,
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "COMPLETED")},
		},
		{
			name:      "no url for objects",
			overrides: map[string]http.HandlerFunc{pollKey: poll("1004", "COMPLETED", `"objectCount":"5"`, `"rootObjectCount":"2"`)},
			want:      []string{"status COMPLETED: no output url for 5 objects"},
		},
		{
			name: "operation in progress",
			overrides: map[string]http.HandlerFunc{"BulkRun-orders-20261001T000000Z": jsonBody(http.StatusOK,
				`{"data":{"bulkOperationRunQuery":{"bulkOperation":null,"userErrors":[{"field":null,"message":"A bulk query operation for this app and shop is already in progress: gid://shopify/BulkOperation/999.","code":"OPERATION_IN_PROGRESS"}]}}}`)},
			want: []string{"stream orders: window [2026-10-01T00:00:00Z, 2026-10-18T11:59:00Z): bulk operation not started: OPERATION_IN_PROGRESS: A bulk query operation for this app and shop is already in progress"},
		},
		{
			name: "download fails",
			overrides: map[string]http.HandlerFunc{
				"bulk/orders.jsonl": jsonBody(http.StatusForbidden, `<Error>AccessDenied</Error>`),
			},
			want: []string{"status COMPLETED: download output: GET output (HTTP 403)"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.overrides)
			src := testSource(srv)
			out := read(t, src, catalogOf(t, src, synclet.SyncModeIncremental, "orders"), nil)
			if len(tt.want) > 0 {
				require.Error(t, out.Err)
				for _, w := range tt.want {
					assert.Contains(t, out.Err.Error(), w)
				}
				assert.Empty(t, records(out, "orders"), "no record of a failed operation")
				assert.Empty(t, states(out), "a failed operation saves no state")
				return
			}
			require.NoError(t, out.Err)
			assert.Len(t, records(out, "orders"), tt.records)
			assert.Equal(t, tt.state, states(out))
			assert.Len(t, h.ops("BulkPoll"), tt.polls)
			for _, r := range h.list() {
				if strings.HasPrefix(r.path, "/bulk/") {
					assert.Empty(t, r.token, "the output download carries no Shopify token")
				}
			}
		})
	}
}

// TestBulkSecondWindowFails reads two windows; the second operation fails,
// so only the first window's records and state are emitted.
func TestBulkSecondWindowFails(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
		"BulkRun-orders-20261011T000000Z": jsonBody(http.StatusOK,
			`{"data":{"bulkOperationRunQuery":{"bulkOperation":{"id":"gid://shopify/BulkOperation/1104","status":"CREATED"},"userErrors":[]}}}`),
		"BulkPoll-1104": poll("1104", "FAILED", `"errorCode":"INTERNAL_SERVER_ERROR"`),
	})
	src := testSource(srv)
	cfg := `{"shop":"example","access_token":"` + testToken + `","start_date":"2026-10-01","window_days":10}`
	out := readWith(t, src, cfg, catalogOf(t, src, synclet.SyncModeIncremental, "orders"), nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "stream orders: window [2026-10-11T00:00:00Z, 2026-10-18T11:59:00Z): bulk operation gid://shopify/BulkOperation/1104 status FAILED errorCode INTERNAL_SERVER_ERROR")
	assert.Equal(t, []string{"gid://shopify/Order/6001", "gid://shopify/Order/6002"}, recordIDs(t, out, "orders"))
	assert.Equal(t, []string{`{"cursor":"2026-10-11T00:00:00Z"}`}, states(out))
}

// TestBulkCancel cancels the running operation when the read is
// cancelled while it polls.
func TestBulkCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	srv, h := fixtureServer(t, map[string]http.HandlerFunc{
		"BulkPoll-1004": func(w http.ResponseWriter, r *http.Request) {
			cancel()
			poll("1004", "RUNNING")(w, r)
		},
	})
	src := testSource(srv)
	raw, err := json.Marshal(catalogOf(t, src, synclet.SyncModeIncremental, "orders"))
	require.NoError(t, err)
	out, err := testkit.RunSource(ctx, src, synclet.Request{Command: synclet.CommandRead, Config: json.RawMessage(testConfig), Catalog: raw})
	if err == nil {
		err = out.Err
	}
	require.ErrorIs(t, err, context.Canceled)
	assert.Len(t, h.ops("BulkCancel"), 1)
}
