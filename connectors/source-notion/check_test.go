package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runCheck(t *testing.T, src Source, cfg Config) *synclet.ConnectionStatus {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: raw})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out.ConnectionStatus()
}

func paths(hs []hit) []string {
	var out []string
	for _, h := range hs {
		out = append(out, h.method+" "+h.path)
	}
	return out
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		override map[string]fixture
		message  string
		requests []string
	}{
		{
			name: "invalid token", cfg: Config{AccessToken: "ntn_wrong"},
			message:  "integration token is invalid (HTTP 401): unauthorized: API token is invalid.",
			requests: []string{"GET users/me"},
		},
		{
			name: "nothing shared", cfg: Config{AccessToken: testToken},
			override: map[string]fixture{"search/all": {status: http.StatusOK, file: "search/empty.json"}},
			message:  "nothing is shared with the integration: add it to pages or databases under ⋯ → Connections in Notion",
			requests: []string{"GET users/me", "POST search"},
		},
		{
			name: "invalid config", cfg: Config{AccessToken: testToken, MaxBlockDepth: intp(0)},
			message: "max_block_depth must be between 1 and 100, got 0",
		},
		{
			name: "ok", cfg: Config{AccessToken: testToken},
			requests: []string{"GET users/me", "POST search"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.override)
			st := runCheck(t, testSource(srv), tt.cfg)
			if tt.message == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
			} else {
				assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
				assert.Equal(t, tt.message, st.Message)
			}
			assert.Equal(t, tt.requests, paths(h.list()))
		})
	}
}

func TestCheckSearchesOneResult(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	runCheck(t, testSource(srv), Config{AccessToken: testToken})
	searches := h.to("search")
	require.Len(t, searches, 1)
	assert.Equal(t, map[string]any{"page_size": float64(1)}, searches[0].body)
}
