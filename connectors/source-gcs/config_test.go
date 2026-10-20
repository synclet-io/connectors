package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{
		"bucket":"b",
		"credentials":{},
		"streams":[
			{"name":"c","glob":"a/*.csv","format":{"filetype":"csv"}},
			{"name":"t","glob":"a/*.tsv","format":{"filetype":"tsv"}},
			{"name":"j","glob":"a/*.jsonl","format":{"filetype":"jsonl"},"compression":"zstd","schema_sample_files":3}
		]}`), &cfg))
	cfg.applyDefaults()
	require.NoError(t, cfg.validate())
	require.Equal(t, authApplicationDefault, cfg.Credentials.AuthType)
	c, tsv, j := cfg.Streams[0], cfg.Streams[1], cfg.Streams[2]
	require.Equal(t, ",", *c.Format.Delimiter)
	require.Equal(t, `"`, *c.Format.QuoteChar)
	require.True(t, *c.Format.Header)
	require.Equal(t, []string{""}, *c.Format.NullValues)
	require.Equal(t, compressionAuto, c.Compression)
	require.Equal(t, defaultSampleFiles, c.SchemaSampleFiles)
	require.Equal(t, "\t", *tsv.Format.Delimiter)
	require.Empty(t, *tsv.Format.QuoteChar)
	require.Equal(t, compressionZstd, j.Compression)
	require.Equal(t, 3, j.SchemaSampleFiles)
}

func TestConfigValidate(t *testing.T) {
	good := func() Config {
		return testConfig(StreamConfig{Name: "s", Glob: "a/**/*.csv", Format: Format{Filetype: formatCSV}})
	}
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"bucket", func(c *Config) { c.Bucket = "" }, "bucket is required"},
		{"auth type", func(c *Config) { c.Credentials.AuthType = "magic" }, `auth_type "magic" is not one of`},
		{"no key", func(c *Config) { c.Credentials = Credentials{AuthType: authService} }, "service_account_info is required"},
		{"key not json", func(c *Config) {
			c.Credentials = Credentials{AuthType: authService, ServiceAccountInfo: "not json"}
		}, "not a JSON service account key"},
		{"key of another type", func(c *Config) {
			c.Credentials = Credentials{AuthType: authService, ServiceAccountInfo: `{"type":"authorized_user"}`}
		}, `has type "authorized_user"`},
		{"key without private key", func(c *Config) {
			c.Credentials = Credentials{AuthType: authService, ServiceAccountInfo: `{"type":"service_account","client_email":"a@p.iam.gserviceaccount.com"}`}
		}, "needs client_email and private_key"},
		{"no streams", func(c *Config) { c.Streams = nil }, "at least one stream"},
		{"duplicate", func(c *Config) { c.Streams = append(c.Streams, c.Streams[0]) }, `name "s" is used twice`},
		{"glob", func(c *Config) { c.Streams[0].Glob = "a/[" }, `glob "a/[" is not a valid pattern`},
		{"compression", func(c *Config) { c.Streams[0].Compression = "bzip2" }, `compression "bzip2" is not one of`},
		{"filetype", func(c *Config) { c.Streams[0].Format.Filetype = "xlsx" }, `filetype "xlsx" is not one of`},
		{"delimiter", func(c *Config) { c.Streams[0].Format.Delimiter = ptr("::") }, "must be one character"},
		{"same chars", func(c *Config) { c.Streams[0].Format.QuoteChar = ptr(",") }, "must differ"},
		{"no header", func(c *Config) { c.Streams[0].Format.Header = ptr(false) }, "header false needs column_names"},
		{"column names", func(c *Config) { c.Streams[0].Format.ColumnNames = []string{"a", "a"} }, "column_names must be unique"},
		{"schema", func(c *Config) { c.Streams[0].Schema = `{"type":"object"}` }, "schema: no properties"},
		{"schema type", func(c *Config) { c.Streams[0].Schema = `{"properties":{"a":{"type":"date"}}}` }, `unknown type "date"`},
		{"sample", func(c *Config) { c.Streams[0].SchemaSampleFiles = -1 }, "schema_sample_files -1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := good()
			cfg.applyDefaults()
			require.NoError(t, cfg.validate())
			c.mutate(&cfg)
			require.ErrorContains(t, cfg.validate(), c.want)
		})
	}
}

func TestGlobPrefix(t *testing.T) {
	for glob, want := range map[string]string{
		"exports/2026/*.csv": "exports/2026/",
		"a/**/b.csv":         "a/",
		"*.csv":              "",
		"logs/day-?/x":       "logs/day-",
		"data/{a,b}/x":       "data/",
		"exact/key.parquet":  "exact/key.parquet",
		"esc/\\*literal":     "esc/",
		"rng/[0-9]/x.jsonl":  "rng/",
	} {
		require.Equal(t, want, globPrefix(glob), glob)
	}
}
