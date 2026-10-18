package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"project_id":"p"}`), &cfg))
	cfg.applyDefaults()
	require.Equal(t, authApplicationDefault, cfg.Credentials.AuthType)
	require.NoError(t, cfg.validate())
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string
	}{
		{"missing project", `{}`, "project_id is required"},
		{"empty dataset", `{"project_id":"p","datasets":[""]}`, "empty name"},
		{"service without key", `{"project_id":"p","credentials":{"auth_type":"Service"}}`, "service_account_info is required"},
		{"unknown auth", `{"project_id":"p","credentials":{"auth_type":"Client"}}`, `auth_type "Client"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			require.NoError(t, json.Unmarshal([]byte(tc.json), &cfg))
			cfg.applyDefaults()
			require.ErrorContains(t, cfg.validate(), tc.want)
		})
	}
}

func TestSpec(t *testing.T) {
	out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	require.True(t, spec.SupportsIncremental)
	props := spec.ConnectionSpecification.Properties.Properties
	for _, key := range []string{"project_id", "datasets", "location", "credentials"} {
		require.Contains(t, props, synclet.PropertyName(key))
	}
	require.Len(t, props["credentials"].OneOf, 2)
	require.Equal(t, []synclet.PropertyName{"project_id"}, spec.ConnectionSpecification.Required)
}
