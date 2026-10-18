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
	assert.Equal(t, 5432, c.Port)
	assert.Equal(t, "public", c.Schema)
	assert.Equal(t, sslPrefer, c.SSLMode.Mode)
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
		{Config{Host: "h", Database: "d", Username: "u", Schema: strings.Repeat("s", 64)}, "longer than 63 bytes"},
		{Config{Host: "h", Database: "d", Username: "u", SSLMode: SSLMode{Mode: "bogus"}}, `ssl_mode "bogus"`},
		{Config{Host: "h", Database: "d", Username: "u", SSLMode: SSLMode{Mode: sslVerifyFull}}, "needs ca_certificate"},
		{Config{Host: "h", Database: "d", Username: "u", SSLMode: SSLMode{Mode: sslRequire, ClientKey: "k"}}, "go together"},
	}
	for _, c := range cases {
		c.cfg.applyDefaults()
		err := c.cfg.validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), c.want)
	}
}
