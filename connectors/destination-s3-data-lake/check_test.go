package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckSucceeds(t *testing.T) {
	e := startLake(t)
	require.NoError(t, NewDestination().Check(t.Context(), e.config(t)))
}

func TestCheckWrongS3Credentials(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	cfg.S3.Credentials.SecretAccessKey = "wrong-secret"
	err := NewDestination().Check(t.Context(), cfg)
	require.Error(t, err)
	assert.Regexp(t, `^invalid S3 credentials`, err.Error())
}

func TestCheckCatalogUnreachable(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	cfg.Catalog.URI = fmt.Sprintf("http://127.0.0.1:%d", closedPort(t))
	err := NewDestination().Check(t.Context(), cfg)
	require.Error(t, err)
	assert.Regexp(t, `^cannot reach catalog at `+cfg.Catalog.URI, err.Error())
}

func TestCheckCatalogUnauthorized(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	proxy := newCatalogProxy(t, e.uri)
	proxy.token = "right-token"
	cfg.Catalog.URI = proxy.URL

	cfg.Catalog.Token = "wrong-token"
	err := NewDestination().Check(t.Context(), cfg)
	require.Error(t, err)
	assert.Regexp(t, `^catalog rejected the credentials`, err.Error())

	cfg.Catalog.Token = "right-token"
	require.NoError(t, NewDestination().Check(t.Context(), cfg))
}

func TestCheckNamespaceMissing(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	cfg.Namespace = "missing"
	err := NewDestination().Check(t.Context(), cfg)
	require.Error(t, err)
	assert.Regexp(t, `^namespace "missing" does not exist`, err.Error())
}

func TestCheckInvalidConfig(t *testing.T) {
	base := func() Config {
		cfg := Config{
			Catalog:   CatalogConfig{URI: "http://catalog:8181"},
			Namespace: "lake",
			S3:        S3Config{WarehouseLocation: "s3://bucket/wh", Credentials: Credentials{AccessKeyID: "k", SecretAccessKey: "s"}},
		}
		cfg.applyDefaults()
		return cfg
	}
	for name, tc := range map[string]struct {
		edit func(*Config)
		want string
	}{
		"catalog type": {func(c *Config) { c.Catalog.CatalogType = "glue" }, `catalog: unsupported catalog_type "glue"`},
		"uri":          {func(c *Config) { c.Catalog.URI = "catalog:8181" }, `is not an http or https URL`},
		"namespace":    {func(c *Config) { c.Namespace = "a..b" }, `namespace "a..b" has an empty level`},
		"location":     {func(c *Config) { c.S3.WarehouseLocation = "bucket/wh" }, `must start with s3://`},
		"keys":         {func(c *Config) { c.S3.Credentials.SecretAccessKey = "" }, `access_key_id and secret_access_key are required`},
		"number type":  {func(c *Config) { c.NumberType = "float" }, `number_type must be`},
		"scale":        {func(c *Config) { s := 39; c.DecimalScale = &s }, `decimal_scale must be between 0 and 38`},
		"auth":         {func(c *Config) { c.Catalog.Token, c.Catalog.Credential = "t", "c" }, `set credential or token, not both`},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := base()
			tc.edit(&cfg)
			err := NewDestination().Check(t.Context(), cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}
