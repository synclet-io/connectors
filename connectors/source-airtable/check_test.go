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

const missingBase = "appBase0000000009"

func TestCheck(t *testing.T) {
	withBase := Config{PersonalAccessToken: testToken, BaseIDs: []string{missingBase}}
	tests := []struct {
		name     string
		cfg      Config
		override map[string]fixture
		message  string
		requests []string
	}{
		{
			name: "invalid token", cfg: Config{PersonalAccessToken: "patWrong.token"},
			message:  "invalid personal access token (HTTP 401): AUTHENTICATION_REQUIRED: Authentication required",
			requests: []string{"meta/whoami"},
		},
		{
			name: "missing schema scope", cfg: testCfg,
			override: map[string]fixture{"meta/whoami": {status: http.StatusOK, file: "errors/whoami-records-scope-only.json"}},
			message:  "token lacks scope(s) schema.bases:read",
			requests: []string{"meta/whoami"},
		},
		{
			name: "no scope", cfg: testCfg,
			override: map[string]fixture{"meta/whoami": {status: http.StatusOK, file: "errors/whoami-no-scopes.json"}},
			message:  "token lacks scope(s) data.records:read, schema.bases:read",
			requests: []string{"meta/whoami"},
		},
		{
			name: "base not shared, 403", cfg: withBase,
			override: map[string]fixture{"meta/bases/" + missingBase + "/tables": {status: http.StatusForbidden, file: "errors/forbidden.json"}},
			message:  "base appBase0000000009 is not shared with the token or does not exist: GET /v0/meta/bases/appBase0000000009/tables (HTTP 403): INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND: Invalid permissions, or the requested model was not found. Check that both your user and your token have the required permissions, and that the model names and/or ids are correct.",
			requests: []string{"meta/whoami", "meta/bases/" + missingBase + "/tables"},
		},
		{
			name: "base not shared, 404", cfg: withBase,
			override: map[string]fixture{"meta/bases/" + missingBase + "/tables": {status: http.StatusNotFound, file: "errors/not-found.json"}},
			message:  "base appBase0000000009 is not shared with the token or does not exist: GET /v0/meta/bases/appBase0000000009/tables (HTTP 404): NOT_FOUND",
			requests: []string{"meta/whoami", "meta/bases/" + missingBase + "/tables"},
		},
		{
			name: "bases refused without a scope list", cfg: testCfg,
			override: map[string]fixture{
				"meta/whoami": {status: http.StatusOK, file: "errors/whoami-no-scope-list.json"},
				"meta/bases":  {status: http.StatusForbidden, file: "errors/forbidden.json"},
			},
			message:  "the token cannot list bases, most likely it lacks the scope schema.bases:read: GET /v0/meta/bases (HTTP 403): INVALID_PERMISSIONS_OR_MODEL_NOT_FOUND: Invalid permissions, or the requested model was not found. Check that both your user and your token have the required permissions, and that the model names and/or ids are correct.",
			requests: []string{"meta/whoami", "meta/bases"},
		},
		{
			name: "no base shared", cfg: testCfg,
			override: map[string]fixture{"meta/bases": {status: http.StatusOK, file: "errors/bases-empty.json"}},
			message:  "no base is shared with the token",
			requests: []string{"meta/whoami", "meta/bases"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.override)
			status := runCheck(t, testSource(srv), tt.cfg)
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Equal(t, tt.message, status.Message)
			assert.Equal(t, tt.requests, h.list(), "a failed check is not retried")
		})
	}
}

func TestCheckSucceeds(t *testing.T) {
	t.Run("every base", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		status := runCheck(t, testSource(srv), testCfg)
		assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
		assert.Equal(t, []string{"meta/whoami", "meta/bases", "meta/bases?offset=itrBases2"}, h.list())
	})

	t.Run("configured bases", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		status := runCheck(t, testSource(srv), Config{PersonalAccessToken: testToken, BaseIDs: []string{base1, base3}})
		assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
		assert.Equal(t, []string{"meta/whoami", "meta/bases/" + base1 + "/tables", "meta/bases/" + base3 + "/tables"}, h.list())
	})

	t.Run("token without scopes in whoami", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string]fixture{"meta/whoami": {status: http.StatusOK, file: "errors/whoami-no-scope-list.json"}})
		status := runCheck(t, testSource(srv), testCfg)
		assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
	})
}

func TestCheckConfig(t *testing.T) {
	status := runCheck(t, Source{}, Config{PersonalAccessToken: testToken, BaseIDs: []string{"Projects"}})
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, `base id "Projects" must be "app" followed by 14 letters or digits`)
}
