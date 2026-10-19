package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"

	"source-declarative/declarativetest"
)

// proofNow is the fixed clock of the Go connectors' tests.
var proofNow = time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)

// read is one snapshot of testdata/<proof>/expected: a stream read in a
// mode from a state.
type proofRead struct {
	stream, name string
	mode         synclet.SyncMode
	state        string
}

type proof struct {
	suite declarativetest.Suite
	reads []proofRead
}

func fullRefresh(streams ...string) []proofRead {
	var out []proofRead
	for _, s := range streams {
		out = append(out, proofRead{stream: s, name: "full_refresh", mode: synclet.SyncModeFullRefresh})
	}
	return out
}

var proofs = map[string]proof{
	"source-openexchangerates": {
		suite: declarativetest.Suite{
			Dir:          "testdata/source-openexchangerates",
			Fixtures:     "../source-openexchangerates/testdata",
			BasePath:     "/api/",
			Route:        func(p string, _ url.Values) string { return p },
			Unauthorized: "errors/invalid_app_id.json",
			Config:       json.RawMessage(`{"app_id":"test-app-id","start_date":"2026-10-14"}`),
			BrokenConfig: json.RawMessage(`{"app_id":"wrong-app-id","start_date":"2026-10-14"}`),
			Auth:         declarativetest.Header("Authorization", "Token test-app-id"),
			Now:          proofNow,
		},
		reads: append(fullRefresh("currencies", "latest_rates", "historical_rates"),
			proofRead{stream: "historical_rates", name: "incremental", mode: synclet.SyncModeIncremental},
			proofRead{stream: "historical_rates", name: "incremental_resumed", mode: synclet.SyncModeIncremental,
				state: `[{"type":"STREAM","stream":{"stream_descriptor":{"name":"historical_rates"},"stream_state":{"date":"2026-10-15"}}}]`},
		),
	},
	"source-revenuecat": {
		suite: declarativetest.Suite{
			Dir:      "testdata/source-revenuecat",
			Fixtures: "../source-revenuecat/testdata",
			BasePath: "/v2/",
			Route: func(p string, q url.Values) string {
				if q.Has("starting_after") {
					return p + "/after-" + q.Get("starting_after") + ".json"
				}
				return p + ".json"
			},
			Unauthorized: "errors/unauthorized.json",
			Config:       json.RawMessage(`{"api_key":"test-secret-key","project_id":"proj_test1","environment":"production"}`),
			BrokenConfig: json.RawMessage(`{"api_key":"wrong-key","project_id":"proj_test1"}`),
			Auth:         declarativetest.Header("Authorization", "Bearer test-secret-key"),
			Now:          proofNow,
			MinPages:     2,
		},
		reads: fullRefresh("projects", "apps", "products", "entitlements", "offerings", "customers",
			"customer_subscriptions", "customer_purchases", "customer_active_entitlements", "metrics_overview"),
	},
}

// unexpressed lists, per proof, the Go connector's streams the manifest
// cannot express, with the missing feature.
var unexpressed = map[string]map[string]string{}

func decode(t *testing.T, b []byte) any {
	t.Helper()
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	var v any
	require.NoError(t, d.Decode(&v), string(b))
	return v
}

// lines is a read as the snapshots hold it: RECORD data and STATE
// messages in emission order.
func lines(t *testing.T, out *testkit.Output) []any {
	t.Helper()
	var got []any
	for _, m := range out.Messages {
		var b []byte
		var err error
		switch m.Type {
		case synclet.MessageTypeRecord:
			b, err = json.Marshal(map[string]any{"type": "RECORD", "data": m.Record.Data})
		case synclet.MessageTypeState:
			b, err = json.Marshal(map[string]any{"type": "STATE", "state": m.State})
		default:
			continue
		}
		require.NoError(t, err)
		got = append(got, decode(t, b))
	}
	return got
}

func snapshot(t *testing.T, path string) []any {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	var want []any
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			want = append(want, decode(t, sc.Bytes()))
		}
	}
	require.NoError(t, sc.Err())
	return want
}

func TestProof(t *testing.T) {
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for k := range proofs {
			if !yield(k) {
				return
			}
		}
	}) {
		p := proofs[name]
		t.Run(name, func(t *testing.T) {
			h := declarativetest.New(t, p.suite)
			dir := p.suite.Dir

			var want synclet.Catalog
			require.NoError(t, json.Unmarshal(mustRead(t, filepath.Join(dir, "expected", "catalog.json")), &want))
			got := h.Catalog(t, synclet.SyncModeFullRefresh)
			gotByName := map[string]synclet.Stream{}
			for _, cs := range got.Streams {
				gotByName[cs.Stream.Name] = cs.Stream
			}
			var equal, missing []string
			for _, ws := range want.Streams {
				gs, ok := gotByName[ws.Name]
				if !ok {
					missing = append(missing, ws.Name+" ("+unexpressed[name][ws.Name]+")")
					continue
				}
				assert.Equal(t, decode(t, ws.JSONSchema), decode(t, gs.JSONSchema), "%s: json_schema", ws.Name)
				ws.JSONSchema, gs.JSONSchema = nil, nil
				assert.Equal(t, ws, gs, "%s: catalog entry", ws.Name)
				ok = true
				for _, rd := range p.reads {
					if rd.stream != ws.Name {
						continue
					}
					cat := h.Catalog(t, rd.mode, rd.stream)
					var state json.RawMessage
					if rd.state != "" {
						state = json.RawMessage(rd.state)
					}
					out := h.Read(t, cat, state)
					require.NoError(t, out.Err, "%s %s", rd.stream, rd.name)
					wantLines := snapshot(t, filepath.Join(dir, "expected", rd.stream+"."+rd.name+".jsonl"))
					require.NotEmpty(t, wantLines)
					if !assert.Equal(t, wantLines, lines(t, out), "%s %s", rd.stream, rd.name) {
						ok = false
					}
				}
				if ok {
					equal = append(equal, ws.Name)
				}
			}
			t.Logf("%s: %d/%d streams equal to the Go connector; unexpressed: %s", name, len(equal), len(want.Streams), orNone(missing))
			assert.Empty(t, missing, "streams the manifest does not express")
			assert.Len(t, equal, len(want.Streams))
		})
	}
}

func orNone(xs []string) string {
	if len(xs) == 0 {
		return "none"
	}
	return strings.Join(xs, ", ")
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	require.NoError(t, err)
	return b
}

func TestConformance(t *testing.T) {
	for name, p := range proofs {
		t.Run(name, func(t *testing.T) {
			declarativetest.Run(t, p.suite)
		})
	}
}
