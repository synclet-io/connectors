package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	cases := []struct {
		cfg  Config
		port int
	}{
		{Config{}, 9000},
		{Config{TLS: TLS{Enabled: true}}, 9440},
		{Config{Protocol: protocolHTTP}, 8123},
		{Config{Protocol: protocolHTTP, TLS: TLS{Enabled: true}}, 8443},
		{Config{Port: 19000}, 19000},
	}
	for _, c := range cases {
		cfg := c.cfg
		cfg.Host, cfg.Database, cfg.Username = "h", "d", "u"
		cfg.applyDefaults()
		assert.Equal(t, c.port, cfg.Port)
		require.NoError(t, cfg.validate())
	}
	c := Config{}
	c.applyDefaults()
	assert.Equal(t, protocolNative, c.Protocol)
	assert.False(t, c.OptimizeFinal)
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
		{Config{Host: "h", Database: "d", Username: "u", Protocol: "grpc"}, `protocol "grpc"`},
		{Config{Host: "h", Database: "d", Username: "u", TLS: TLS{CACertificate: "x"}}, "tls.enabled is false"},
		{Config{Host: "h", Database: "d", Username: "u", TLS: TLS{Enabled: true, ClientKey: "k"}}, "go together"},
	}
	for _, c := range cases {
		c.cfg.applyDefaults()
		err := c.cfg.validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), c.want)
	}
}

func TestConfigTLS(t *testing.T) {
	cfg := Config{Host: "h", Database: "d", Username: "u", TLS: TLS{Enabled: true, CACertificate: "not pem"}}
	cfg.applyDefaults()
	_, err := chOptions(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ca_certificate holds no PEM certificate")

	cfg.TLS.CACertificate = ""
	opts, err := chOptions(cfg)
	require.NoError(t, err)
	require.NotNil(t, opts.TLS)
	assert.Equal(t, "h", opts.TLS.ServerName)
	assert.False(t, opts.TLS.InsecureSkipVerify)
	assert.Equal(t, 0, opts.Settings["async_insert"])
}
