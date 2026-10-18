package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	c := Config{Host: "h", Database: "d", Username: "u"}
	c.applyDefaults()
	assert.Equal(t, 3306, c.Port)
	assert.Equal(t, sslPreferred, c.SSLMode.Mode)
	require.NoError(t, c.validate())
}

func TestConfigValidate(t *testing.T) {
	cases := []struct {
		cfg  Config
		want string
	}{
		{Config{Database: "d", Username: "u"}, "host is required"},
		{Config{Host: "h", Username: "u"}, "database is required"},
		{Config{Host: "h", Database: "d"}, "username is required"},
		{Config{Host: "h", Database: "d", Username: "u", Port: 70000}, "out of range"},
		{Config{Host: "h", Database: strings.Repeat("d", 65), Username: "u"}, "longer than 64 characters"},
		{Config{Host: "h", Database: "d", Username: "u", SSLMode: SSLMode{Mode: "bogus"}}, `ssl_mode "bogus"`},
		{Config{Host: "h", Database: "d", Username: "u", SSLMode: SSLMode{Mode: sslVerifyIdentity}}, "needs ca_certificate"},
		{Config{Host: "h", Database: "d", Username: "u", SSLMode: SSLMode{Mode: sslPreferred, CACertificate: "x"}}, "does not use ca_certificate"},
		{Config{Host: "h", Database: "d", Username: "u", SSLMode: SSLMode{Mode: sslRequired, ClientKey: "k"}}, "go together"},
	}
	for _, c := range cases {
		c.cfg.applyDefaults()
		err := c.cfg.validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), c.want)
	}
}

func TestConfigDriver(t *testing.T) {
	base := Config{Host: "db.internal", Database: "d", Username: "u"}
	base.applyDefaults()

	mc, err := driverConfig(base)
	require.NoError(t, err)
	assert.Equal(t, "db.internal:3306", mc.Addr)
	assert.True(t, mc.InterpolateParams)
	assert.Equal(t, "'+00:00'", mc.Params["time_zone"])
	assert.Contains(t, mc.Params["sql_mode"], "STRICT_ALL_TABLES")
	require.NotNil(t, mc.TLS)
	assert.True(t, mc.TLS.InsecureSkipVerify)
	assert.True(t, mc.AllowFallbackToPlaintext, "preferred falls back to plain text")

	disabled := base
	disabled.SSLMode.Mode = sslDisabled
	mc, err = driverConfig(disabled)
	require.NoError(t, err)
	assert.Nil(t, mc.TLS)

	required := base
	required.SSLMode.Mode = sslRequired
	mc, err = driverConfig(required)
	require.NoError(t, err)
	require.NotNil(t, mc.TLS)
	assert.False(t, mc.AllowFallbackToPlaintext)

	bad := base
	bad.SSLMode = SSLMode{Mode: sslVerifyCA, CACertificate: "not pem"}
	_, err = driverConfig(bad)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ca_certificate holds no PEM certificate")
}
