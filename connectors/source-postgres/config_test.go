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
		{"prefer with CA", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"prefer","ca_certificate":"x"}}`, "ssl_mode prefer does not use ca_certificate"},
		{"allow with CA", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"allow","ca_certificate":"x"}}`, "ssl_mode allow does not use ca_certificate"},
		{"invalid host key", `{"host":"db","database":"app","username":"u","tunnel_method":{"tunnel_method":"SSH_PASSWORD_AUTH","tunnel_host":"b","tunnel_user":"u","tunnel_user_password":"p","tunnel_host_key":"not a key"}}`, "tunnel_host_key"},
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

func setHostilePGEnv(t *testing.T) {
	t.Helper()
	for k, v := range map[string]string{
		"PGHOST": "env.invalid", "PGPORT": "1", "PGDATABASE": "envdb", "PGUSER": "envuser", "PGPASSWORD": "envpass",
		"PGPASSFILE": "/nonexistent/pgpass", "PGAPPNAME": "env-app", "PGCONNECT_TIMEOUT": "never",
		"PGSSLMODE": "verify-full", "PGSSLROOTCERT": "/nonexistent/root.crt", "PGSSLCERT": "/nonexistent/client.crt",
		"PGSSLKEY": "/nonexistent/client.key", "PGSSLPASSWORD": "envsslpass", "PGSSLSNI": "0", "PGSSLNEGOTIATION": "direct",
		"PGTARGETSESSIONATTRS": "standby", "PGTZ": "Asia/Tokyo", "PGOPTIONS": "-c search_path=env",
		"PGMINPROTOCOLVERSION": "3.2", "PGMAXPROTOCOLVERSION": "3.2", "PGCHANNELBINDING": "require", "PGREQUIREAUTH": "gss",
	} {
		t.Setenv(k, v)
	}
}

func TestPGConfigIgnoresEnv(t *testing.T) {
	setHostilePGEnv(t)
	cfg := Config{Host: "db.example.com", Port: 6543, Database: "app", Username: "reader", Password: "secret", SSLMode: SSLMode{Mode: sslRequire}}
	pc, err := pgConfig(cfg)
	require.NoError(t, err)
	require.Equal(t, "db.example.com", pc.Host)
	require.Equal(t, uint16(6543), pc.Port)
	require.Equal(t, "app", pc.Database)
	require.Equal(t, "reader", pc.User)
	require.Equal(t, "secret", pc.Password)
	require.Equal(t, connectTimeout, pc.ConnectTimeout)
	require.Equal(t, map[string]string{
		"application_name": "synclet-source-postgres",
		"TimeZone":         "UTC",
		"DateStyle":        "ISO, MDY",
		"IntervalStyle":    "iso_8601",
	}, pc.RuntimeParams)
	require.Nil(t, pc.ValidateConnect)
	require.Equal(t, "postgres", pc.SSLNegotiation)
	require.Equal(t, "3.0", pc.MinProtocolVersion)
	require.Equal(t, "3.0", pc.MaxProtocolVersion)
	require.Equal(t, "prefer", pc.ChannelBinding)
	require.Empty(t, pc.RequireAuth)
	require.Empty(t, pc.Fallbacks)
	require.NotNil(t, pc.TLSConfig)
	require.True(t, pc.TLSConfig.InsecureSkipVerify)
	require.Empty(t, pc.TLSConfig.Certificates)
	require.Empty(t, pc.TLSConfig.NextProtos)

	cfg.Password = ""
	pc, err = pgConfig(cfg)
	require.NoError(t, err)
	require.Empty(t, pc.Password)
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
