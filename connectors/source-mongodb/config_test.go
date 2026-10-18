package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func validConfig() Config {
	return Config{DatabaseConfig: DatabaseConfig{
		ClusterType:      clusterSelfManaged,
		ConnectionString: "mongodb://h1:27017,h2:27017/?replicaSet=rs0",
		Database:         "app",
		Username:         "reader",
		Password:         "secret-pw",
	}}
}

func TestConfigDefaults(t *testing.T) {
	var c Config
	c.applyDefaults()
	require.Equal(t, clusterSelfManaged, c.DatabaseConfig.ClusterType)
	require.Equal(t, 10_000, c.DiscoverSampleSize)
	require.Equal(t, tunnelNone, c.TunnelMethod.TunnelMethod)
	require.Equal(t, 22, c.TunnelMethod.TunnelPort)
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"valid", func(*Config) {}, ""},
		{"srv", func(c *Config) { c.DatabaseConfig.ConnectionString = "mongodb+srv://cluster0.example.net/" }, ""},
		{"no auth", func(c *Config) { c.DatabaseConfig.Username, c.DatabaseConfig.Password = "", "" }, ""},
		{"missing connection string", func(c *Config) { c.DatabaseConfig.ConnectionString = "" }, "connection_string is required"},
		{"wrong scheme", func(c *Config) { c.DatabaseConfig.ConnectionString = "postgres://h" }, "must start with mongodb:// or mongodb+srv://"},
		{"missing database", func(c *Config) { c.DatabaseConfig.Database = "" }, "database is required"},
		{"password without user", func(c *Config) { c.DatabaseConfig.Username = "" }, "password is set without"},
		{"cluster type", func(c *Config) { c.DatabaseConfig.ClusterType = "SHARDED" }, `cluster_type "SHARDED" is not one of`},
		{"sample size", func(c *Config) { c.DiscoverSampleSize = 100_001 }, "discover_sample_size 100001 is out of range"},
		{"ca", func(c *Config) { c.TLSCACertificate = "not pem" }, "tls_ca_certificate holds no PEM certificate"},
		{"tunnel without host", func(c *Config) {
			c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelUser: "u", TunnelUserPassword: "p"}
		}, "tunnel_host is required"},
		{"tunnel key missing", func(c *Config) {
			c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelKey, TunnelHost: "b", TunnelUser: "u"}
		}, "ssh_key is required"},
		{"tunnel method", func(c *Config) { c.TunnelMethod.TunnelMethod = "VPN" }, `tunnel_method "VPN" is not one of`},
		{"host key", func(c *Config) {
			c.TunnelMethod = TunnelMethod{TunnelMethod: tunnelPassword, TunnelHost: "b", TunnelUser: "u", TunnelUserPassword: "p", TunnelHostKey: "junk"}
		}, "tunnel_host_key line 1 is not a public key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.edit(&c)
			c.applyDefaults()
			err := c.validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestClientOptions(t *testing.T) {
	t.Run("config credentials override the URI and default to admin", func(t *testing.T) {
		c := validConfig()
		c.DatabaseConfig.ConnectionString = "mongodb://other:pw@h1/?authMechanism=SCRAM-SHA-256"
		opts, err := Source{}.clientOptions(c)
		require.NoError(t, err)
		require.Equal(t, "reader", opts.Auth.Username)
		require.Equal(t, "secret-pw", opts.Auth.Password)
		require.Equal(t, "SCRAM-SHA-256", opts.Auth.AuthMechanism)
		require.Equal(t, "admin", opts.Auth.AuthSource)
	})
	t.Run("authSource of the URI unless auth_source is set", func(t *testing.T) {
		c := validConfig()
		c.DatabaseConfig.ConnectionString = "mongodb://h1/?authSource=users"
		opts, err := Source{}.clientOptions(c)
		require.NoError(t, err)
		require.Equal(t, "users", opts.Auth.AuthSource)
		c.DatabaseConfig.AuthSource = "admin2"
		opts, err = Source{}.clientOptions(c)
		require.NoError(t, err)
		require.Equal(t, "admin2", opts.Auth.AuthSource)
	})
	t.Run("no user, no credential", func(t *testing.T) {
		c := validConfig()
		c.DatabaseConfig.Username, c.DatabaseConfig.Password = "", ""
		opts, err := Source{}.clientOptions(c)
		require.NoError(t, err)
		require.Nil(t, opts.Auth)
	})
	t.Run("CA turns TLS on", func(t *testing.T) {
		c := validConfig()
		c.TLSCACertificate = newTestCA(t).certPEM
		opts, err := Source{}.clientOptions(c)
		require.NoError(t, err)
		require.NotNil(t, opts.TLSConfig)
		require.NotNil(t, opts.TLSConfig.RootCAs)
	})
}

func TestConnectErrorRedacts(t *testing.T) {
	c := validConfig()
	c.DatabaseConfig.ConnectionString = "mongodb://u:uri-pw@db1:27017,db2:27017/app?replicaSet=rs0"
	cl := &client{cfg: c, user: "reader"}
	err := cl.connectError(errors.New("server selection error: dial db1 with secret-pw and uri-pw"))
	require.EqualError(t, err, "cannot reach db1:27017,db2:27017: server selection error: dial db1 with **** and ****")
	require.Equal(t, "cluster0.example.net", uriHosts("mongodb+srv://u:p@ss@cluster0.example.net/db?tls=true"))
	require.Equal(t, "p@ss", uriPassword("mongodb+srv://u:p@ss@cluster0.example.net/db"))
}

func TestSpec(t *testing.T) {
	out, err := testkit.RunSource(t.Context(), Source{}, synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	require.True(t, spec.SupportsIncremental)
	props := spec.ConnectionSpecification.Properties.Properties
	for _, key := range []string{"database_config", "discover_sample_size", "tls_ca_certificate", "tunnel_method"} {
		require.Contains(t, props, synclet.PropertyName(key))
	}
	require.Len(t, props["database_config"].OneOf, 2)
	for _, o := range props["database_config"].OneOf {
		for _, key := range []string{"cluster_type", "connection_string", "database", "username", "password", "auth_source"} {
			require.Contains(t, o.Properties, synclet.PropertyName(key))
		}
		require.True(t, o.Properties["password"].IsSecret)
	}
	require.Len(t, props["tunnel_method"].OneOf, 3)
}
