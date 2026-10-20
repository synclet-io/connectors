package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func runCheck(t *testing.T, src Source, config string) *synclet.ConnectionStatus {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(config)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out.ConnectionStatus()
}

func TestCheckSucceeds(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	status := runCheck(t, testSource(srv), testConfig)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
	assert.Len(t, h.of("2/annotations"), 1)
	assert.Len(t, h.all(), 1)
}

func TestCheckInvalidCredentials(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	cfg := `{"api_key":"wrong-api-key","secret_key":"wrong-secret","start_date":"2026-10-16T00:00:00Z"}`
	status := runCheck(t, testSource(srv), cfg)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "invalid api_key or secret_key, or a data_region the project is not in (HTTP 401)")
	assert.NotContains(t, status.Message, "wrong-secret")
	assert.Len(t, h.all(), 1, "a 401 is not retried")
}

func TestCheckInvalidConfig(t *testing.T) {
	status := runCheck(t, Source{}, `{"api_key":"a","secret_key":"s","start_date":"soon"}`)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, `start_date "soon"`)
}
