package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"host":"db","database":"app","username":"u"}`), &cfg))
	cfg.applyDefaults()
	require.Equal(t, 5432, cfg.Port)
	require.Equal(t, []string{"public"}, cfg.Schemas)
	require.Equal(t, sslPrefer, cfg.SSLMode.Mode)
	require.Equal(t, tunnelNone, cfg.TunnelMethod.TunnelMethod)
	require.NoError(t, cfg.validate())
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		json string
		want string
	}{
		{"missing host", `{"database":"app","username":"u"}`, "host is required"},
		{"missing database", `{"host":"db","username":"u"}`, "database is required"},
		{"missing username", `{"host":"db","database":"app"}`, "username is required"},
		{"bad port", `{"host":"db","database":"app","username":"u","port":70000}`, "out of range"},
		{"unknown ssl mode", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"sometimes"}}`, `ssl_mode "sometimes"`},
		{"verify-ca without CA", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"verify-ca"}}`, "verify-ca needs ca_certificate"},
		{"verify-full without CA", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"verify-full"}}`, "verify-full needs ca_certificate"},
		{"client cert without key", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"require","client_certificate":"x"}}`, "go together"},
		{"unknown tunnel", `{"host":"db","database":"app","username":"u","tunnel_method":{"tunnel_method":"VPN"}}`, `tunnel_method "VPN"`},
		{"key tunnel without key", `{"host":"db","database":"app","username":"u","tunnel_method":{"tunnel_method":"SSH_KEY_AUTH","tunnel_host":"b","tunnel_user":"u"}}`, "ssh_key is required"},
		{"password tunnel without host", `{"host":"db","database":"app","username":"u","tunnel_method":{"tunnel_method":"SSH_PASSWORD_AUTH","tunnel_user":"u","tunnel_user_password":"p"}}`, "tunnel_host is required"},
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
	for _, key := range []string{"host", "port", "database", "schemas", "username", "password", "ssl_mode", "tunnel_method"} {
		require.Contains(t, props, synclet.PropertyName(key))
	}
	require.Len(t, props["ssl_mode"].OneOf, len(sslModes))
	require.Len(t, props["tunnel_method"].OneOf, 3)
}
