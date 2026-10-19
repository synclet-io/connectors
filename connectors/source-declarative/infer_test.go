package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func inferManifest(base string) map[string]any {
	return map[string]any{
		"version":   1,
		"connector": map[string]any{"name": "source-builder-test"},
		"base_url":  base + "/",
		"spec": map[string]any{
			"required":   []any{"api_key"},
			"properties": map[string]any{"api_key": map[string]any{"type": "string", "secret": true}},
		},
		"auth":  map[string]any{"type": "bearer", "token": "{{ config.api_key }}"},
		"check": map[string]any{"stream": "items"},
		"streams": []any{
			map[string]any{"name": "items", "path": "items", "primary_key": []any{"id"}, "records": map[string]any{"path": "$.data"}},
			map[string]any{"name": "tags", "path": "tags"},
		},
	}
}

func configFile(t *testing.T, cfg map[string]any) string {
	t.Helper()
	b, err := json.Marshal(cfg)
	require.NoError(t, err)
	p := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(p, b, 0o600))
	return p
}

func messages(t *testing.T, out string) []*synclet.Message {
	t.Helper()
	var msgs []*synclet.Message
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var m synclet.Message
		require.NoError(t, json.Unmarshal([]byte(line), &m), line)
		msgs = append(msgs, &m)
	}
	return msgs
}

func TestInferCommand(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/items":
			_, _ = w.Write([]byte(`{"data":[{"id":1},{"id":2},{"id":3}]}`))
		case "/tags":
			_, _ = w.Write([]byte(`[{"name":"a"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	cfg := configFile(t, map[string]any{"api_key": "k", "__manifest": inferManifest(srv.URL)})

	var stdout, stderr bytes.Buffer
	code := runInfer(t.Context(), resolve(t.TempDir()), []string{"--config", cfg, "--streams", "items", "--limit", "2", "--pages", "1"}, &stdout, &stderr)
	require.Equal(t, 0, code, stderr.String())
	msgs := messages(t, stdout.String())
	var records int
	for _, m := range msgs {
		if m.Type == synclet.MessageTypeRecord {
			records++
			assert.Equal(t, "items", m.Record.Stream)
		}
	}
	assert.Equal(t, 2, records)
	last := msgs[len(msgs)-1]
	require.Equal(t, synclet.MessageTypeCatalog, last.Type)
	require.Len(t, last.Catalog.Streams, 1)
	assert.JSONEq(t, `{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":true}`, string(last.Catalog.Streams[0].JSONSchema))
}

func TestInferCommandCatalogImage(t *testing.T) {
	cfg := configFile(t, map[string]any{"api_key": "k", "start_date": "2026-10-01T00:00:00Z"})
	var stdout, stderr bytes.Buffer
	code := runInfer(t.Context(), resolve("example/source-acme"), []string{"--config", cfg}, &stdout, &stderr)
	assert.Equal(t, 1, code)
	msgs := messages(t, stdout.String())
	require.Len(t, msgs, 1)
	require.Equal(t, synclet.MessageTypeTrace, msgs[0].Type)
	assert.Equal(t, synclet.FailureTypeConfigError, msgs[0].Trace.Error.FailureType)
	assert.Contains(t, msgs[0].Trace.Error.Message, "generic source-declarative image")
}

func TestInferCommandUsage(t *testing.T) {
	cfg := configFile(t, map[string]any{"__manifest": inferManifest("https://api.invalid")})
	for name, args := range map[string][]string{
		"no config":      {},
		"missing file":   {"--config", filepath.Join(t.TempDir(), "nope.json")},
		"limit zero":     {"--config", cfg, "--limit", "0"},
		"limit too high": {"--config", cfg, "--limit", "1001"},
		"pages zero":     {"--config", cfg, "--pages", "0"},
		"pages too high": {"--config", cfg, "--pages", "11"},
		"empty stream":   {"--config", cfg, "--streams", "items,"},
		"extra argument": {"--config", cfg, "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runInfer(t.Context(), resolve(t.TempDir()), args, &stdout, &stderr)
			assert.Equal(t, 2, code)
			assert.Empty(t, stdout.String())
			assert.Contains(t, stderr.String(), "usage: infer --config FILE")
		})
	}
}
