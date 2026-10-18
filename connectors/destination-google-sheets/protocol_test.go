package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const brokenAuthConfig = `{"spreadsheet_id":"abc","credentials":{"auth_type":"Unknown"}}`

func TestProtocolSpec(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), NewGoogleSheetsDestination(), synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	assert.Equal(t, "object", spec.ConnectionSpecification.Type)
	assert.NotEmpty(t, spec.SupportedDestinationSyncModes)
}

func TestProtocolCheckFailure(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), NewGoogleSheetsDestination(), synclet.Request{
		Command: synclet.CommandCheck,
		Config:  json.RawMessage(brokenAuthConfig),
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "unsupported auth_type")
}

func TestProtocolWriteFailure(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), NewGoogleSheetsDestination(), synclet.Request{
		Command: synclet.CommandWrite,
		Config:  json.RawMessage(brokenAuthConfig),
		Catalog: json.RawMessage(`{"streams":[]}`),
		Input:   strings.NewReader(""),
	})
	require.NoError(t, err)
	require.Error(t, out.Err)
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Contains(t, traces[0].Message, "unsupported auth_type")
	assert.Empty(t, out.States())
}
