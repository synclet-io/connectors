package main

import (
	"encoding/json"
	"testing"

	"github.com/microsoft/go-mssqldb/msdsn"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{"host":"db","database":"app","username":"u"}`), &cfg))
	cfg.applyDefaults()
	require.Equal(t, 1433, cfg.Port)
	require.Equal(t, []string{"dbo"}, cfg.Schemas)
	require.Equal(t, authSQL, cfg.Authentication)
	require.Equal(t, sslTrust, cfg.SSLMethod.SSLMethod)
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
		{"unknown authentication", `{"host":"db","database":"app","username":"u","authentication":"Kerberos"}`, `authentication "Kerberos"`},
		{"NTLM without domain", `{"host":"db","database":"app","username":"u","authentication":"NTLM"}`, `NTLM needs username as DOMAIN\user`},
		{"NTLM with empty user", `{"host":"db","database":"app","username":"CORP\\","authentication":"NTLM"}`, `NTLM needs username as DOMAIN\user`},
		{"SQL with a Windows account", `{"host":"db","database":"app","username":"CORP\\u"}`, "set authentication to NTLM"},
		{"unknown ssl method", `{"host":"db","database":"app","username":"u","ssl_method":{"ssl_method":"sometimes"}}`, `ssl_method "sometimes"`},
		{"trust with a certificate", `{"host":"db","database":"app","username":"u","ssl_method":{"ssl_method":"encrypted_trust_server_certificate","certificate":"x"}}`, "takes no certificate"},
		{"unencrypted with a host name", `{"host":"db","database":"app","username":"u","ssl_method":{"ssl_method":"unencrypted","hostNameInCertificate":"x"}}`, "takes no certificate"},
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

func TestMSConfig(t *testing.T) {
	cfg := Config{Host: "db.example.com", Port: 14330, Database: "app", Username: "reader", Password: "secret"}
	cfg.applyDefaults()
	mc, err := msConfig(cfg, cfg.Database)
	require.NoError(t, err)
	require.Equal(t, "db.example.com", mc.Host)
	require.Equal(t, uint64(14330), mc.Port)
	require.Equal(t, "app", mc.Database)
	require.Equal(t, "reader", mc.User)
	require.Equal(t, "secret", mc.Password)
	require.Equal(t, "synclet-source-mssql", mc.AppName)
	require.Equal(t, []string{"tcp"}, mc.Protocols)
	require.NotContains(t, mc.Parameters, "authenticator", "SQL authentication uses no integrated authenticator")
	require.Equal(t, msdsn.Encryption(msdsn.EncryptionRequired), mc.Encryption)
	require.True(t, mc.TLSConfig.InsecureSkipVerify)

	ntlm := cfg
	ntlm.Authentication, ntlm.Username = authNTLM, `CORP\reader`
	mc, err = msConfig(ntlm, ntlm.Database)
	require.NoError(t, err)
	require.Equal(t, "ntlm", mc.Parameters["authenticator"])
	require.Equal(t, `CORP\reader`, mc.User)

	plain := cfg
	plain.SSLMethod = SSLMethod{SSLMethod: sslUnencrypted}
	mc, err = msConfig(plain, plain.Database)
	require.NoError(t, err)
	require.Equal(t, msdsn.Encryption(msdsn.EncryptionOff), mc.Encryption)
	require.True(t, mc.TLSConfig.InsecureSkipVerify, "the login is encrypted without checking the certificate")

	verify := cfg
	verify.SSLMethod = SSLMethod{SSLMethod: sslVerify, HostNameInCertificate: "sql.internal", Certificate: newTestCA(t).certPEM}
	mc, err = msConfig(verify, verify.Database)
	require.NoError(t, err)
	require.Equal(t, msdsn.Encryption(msdsn.EncryptionRequired), mc.Encryption)
	require.False(t, mc.TLSConfig.InsecureSkipVerify)
	require.Equal(t, "sql.internal", mc.TLSConfig.ServerName)
	require.True(t, mc.HostInCertificateProvided)
	require.NotNil(t, mc.TLSConfig.RootCAs)

	verify.SSLMethod.Certificate = "not a certificate"
	_, err = msConfig(verify, verify.Database)
	require.ErrorContains(t, err, "holds no PEM certificate")
}

func TestSpec(t *testing.T) {
	out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	require.True(t, spec.SupportsIncremental)
	props := spec.ConnectionSpecification.Properties.Properties
	for _, key := range []string{"host", "port", "database", "schemas", "username", "password", "authentication", "ssl_method", "tunnel_method"} {
		require.Contains(t, props, synclet.PropertyName(key))
	}
	require.Len(t, props["ssl_method"].OneOf, len(sslMethods))
	require.Len(t, props["tunnel_method"].OneOf, 3)
}
