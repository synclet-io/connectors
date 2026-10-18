package main

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"host":"db","database":"app","username":"u"}`), &cfg))
	cfg.applyDefaults()
	require.Equal(t, 3306, cfg.Port)
	require.Equal(t, sslPreferred, cfg.SSLMode.Mode)
	require.Equal(t, tunnelNone, cfg.TunnelMethod.TunnelMethod)
	require.Equal(t, 22, cfg.TunnelMethod.TunnelPort)
	require.Equal(t, replicationStandard, cfg.ReplicationMethod.Method)
	require.Equal(t, defaultCheckpointSeconds, cfg.ReplicationMethod.CheckpointSeconds)
	require.Equal(t, zeroDateNull, cfg.ZeroDateHandling)
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
		{"verify_ca without CA", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"verify_ca"}}`, "verify_ca needs ca_certificate"},
		{"verify_identity without CA", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"verify_identity"}}`, "verify_identity needs ca_certificate"},
		{"preferred with CA", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"preferred","ca_certificate":"x"}}`, "ssl_mode preferred does not use ca_certificate"},
		{"client cert without key", `{"host":"db","database":"app","username":"u","ssl_mode":{"mode":"required","client_certificate":"x"}}`, "go together"},
		{"invalid host key", `{"host":"db","database":"app","username":"u","tunnel_method":{"tunnel_method":"SSH_PASSWORD_AUTH","tunnel_host":"b","tunnel_user":"u","tunnel_user_password":"p","tunnel_host_key":"not a key"}}`, "tunnel_host_key"},
		{"unknown tunnel", `{"host":"db","database":"app","username":"u","tunnel_method":{"tunnel_method":"VPN"}}`, `tunnel_method "VPN"`},
		{"key tunnel without key", `{"host":"db","database":"app","username":"u","tunnel_method":{"tunnel_method":"SSH_KEY_AUTH","tunnel_host":"b","tunnel_user":"u"}}`, "ssh_key is required"},
		{"password tunnel without host", `{"host":"db","database":"app","username":"u","tunnel_method":{"tunnel_method":"SSH_PASSWORD_AUTH","tunnel_user":"u","tunnel_user_password":"p"}}`, "tunnel_host is required"},
		{"unknown replication", `{"host":"db","database":"app","username":"u","replication_method":{"method":"LOGICAL"}}`, `replication_method "LOGICAL"`},
		{"unknown zero date handling", `{"host":"db","database":"app","username":"u","zero_date_handling":"epoch"}`, `zero_date_handling "epoch"`},
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

// TestConfigLegacyKeys decodes a configuration in the keys of earlier
// versions and the same configuration in Airbyte's keys to the same Config.
func TestConfigLegacyKeys(t *testing.T) {
	cases := []struct {
		name, legacy, current string
	}{
		{"0.2 keys",
			`{"host":"db","database":"app","username":"u",
			"ssl":{"mode":"verify_ca","ca_certificate":"CA"},
			"tunnel":{"method":"ssh_key","host":"bastion","port":2222,"user":"deploy","ssh_key":"KEY"},
			"replication":{"method":"CDC","cdc":{"server_id":5500,"initial_wait_seconds":600,"checkpoint_seconds":60}}}`,
			`{"host":"db","database":"app","username":"u",
			"ssl_mode":{"mode":"verify_ca","ca_certificate":"CA"},
			"tunnel_method":{"tunnel_method":"SSH_KEY_AUTH","tunnel_host":"bastion","tunnel_port":2222,"tunnel_user":"deploy","ssh_key":"KEY"},
			"replication_method":{"method":"CDC","server_id":5500,"initial_waiting_seconds":600,"checkpoint_seconds":60}}`},
		{"0.2 password tunnel",
			`{"tunnel":{"method":"ssh_password","host":"bastion","user":"deploy","password":"PW"}}`,
			`{"tunnel_method":{"tunnel_method":"SSH_PASSWORD_AUTH","tunnel_host":"bastion","tunnel_user":"deploy","tunnel_user_password":"PW"}}`},
		{"0.2 spec.json keys",
			`{"tunnel_method":{"method":"ssh_password","tunnel_host":"bastion","tunnel_user":"deploy","tunnel_password":"PW"},
			"replication_method":{"method":"CDC","initial_wait_seconds":600}}`,
			`{"tunnel_method":{"tunnel_method":"SSH_PASSWORD_AUTH","tunnel_host":"bastion","tunnel_user":"deploy","tunnel_user_password":"PW"},
			"replication_method":{"method":"CDC","initial_waiting_seconds":600}}`},
		{"Airbyte keys win",
			`{"ssl":{"mode":"required"},"ssl_mode":{"mode":"verify_identity","ca_certificate":"CA"},
			"tunnel":{"method":"ssh_key"},"tunnel_method":{"tunnel_method":"NO_TUNNEL"}}`,
			`{"ssl_mode":{"mode":"verify_identity","ca_certificate":"CA"},"tunnel_method":{"tunnel_method":"NO_TUNNEL"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var legacy, current Config
			require.NoError(t, json.Unmarshal([]byte(tc.legacy), &legacy))
			require.NoError(t, json.Unmarshal([]byte(tc.current), &current))
			require.Equal(t, current, legacy)
		})
	}
}

// TestConfigFromSpec builds a configuration from every oneOf option of
// spec.json, each property set, and checks that Config keeps every value.
func TestConfigFromSpec(t *testing.T) {
	raw, err := os.ReadFile("spec.json")
	require.NoError(t, err)
	var spec synclet.ConnectorSpecification
	require.NoError(t, json.Unmarshal(raw, &spec))
	props := spec.ConnectionSpecification.Properties.Properties

	for _, key := range []synclet.PropertyName{"ssl_mode", "tunnel_method", "replication_method"} {
		require.NotEmpty(t, props[key].OneOf, key)
		for _, option := range props[key].OneOf {
			t.Run(fmt.Sprintf("%s %s", key, option.Title), func(t *testing.T) {
				obj := map[string]any{}
				for name, p := range option.Properties {
					switch {
					case p.Const != nil:
						obj[string(name)] = p.Const
					case len(p.Type) > 0 && p.Type[0] == synclet.Integer:
						obj[string(name)] = 7
					default:
						obj[string(name)] = "value of " + string(name)
					}
				}
				var cfg Config
				require.NoError(t, json.Unmarshal(mustJSON(t, map[string]any{string(key): obj}), &cfg))
				var top map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(mustJSON(t, cfg), &top))
				var back map[string]any
				require.NoError(t, json.Unmarshal(top[string(key)], &back))
				for name, want := range obj {
					require.Equal(t, fmt.Sprint(want), fmt.Sprint(back[name]), "%s.%s", key, name)
				}
			})
		}
	}

	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"table_filter":{"schema_patterns":["s%"],"table_patterns":["t%"]},"zero_date_handling":"fail"}`), &cfg))
	require.Equal(t, TableFilter{SchemaPatterns: []string{"s%"}, TablePatterns: []string{"t%"}}, cfg.TableFilter)
	require.Equal(t, zeroDateFail, cfg.ZeroDateHandling)
	var choices []any
	for _, o := range props["zero_date_handling"].OneOf {
		choices = append(choices, o.Const)
	}
	require.Equal(t, []any{zeroDateNull, zeroDateFail}, choices, "zero_date_handling offers exactly the values validate accepts")
	for _, key := range []synclet.PropertyName{"host", "port", "database", "username", "password", "table_filter", "zero_date_handling"} {
		require.Contains(t, props, key)
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
	require.Len(t, props["ssl_mode"].OneOf, len(sslModes))
	require.Len(t, props["tunnel_method"].OneOf, 3)
	require.Len(t, props["replication_method"].OneOf, 2)
}

func TestMySQLConfig(t *testing.T) {
	cfg := Config{Host: "db.example.com", Port: 3307, Database: "app", Username: "reader", Password: "secret"}
	cfg.applyDefaults()
	mc, err := mysqlConfig(cfg)
	require.NoError(t, err)
	require.Equal(t, "db.example.com:3307", mc.Addr)
	require.True(t, mc.InterpolateParams, "every query runs on the text protocol")
	require.False(t, mc.ParseTime)
	require.Equal(t, map[string]string{"time_zone": "'+00:00'", "net_write_timeout": "3600"}, mc.Params)
	require.Contains(t, mc.FormatDSN(), "charset=utf8mb4")
	require.True(t, mc.TLS.InsecureSkipVerify)
	require.True(t, mc.AllowFallbackToPlaintext, "preferred connects without TLS when the server has none")

	cfg.SSLMode = SSLMode{Mode: sslRequired}
	mc, err = mysqlConfig(cfg)
	require.NoError(t, err)
	require.False(t, mc.AllowFallbackToPlaintext)
}
