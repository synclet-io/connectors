package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseConfig(t *testing.T, raw string) Config {
	t.Helper()
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(raw), &cfg))
	return cfg
}

func TestConfigDefaults(t *testing.T) {
	cfg := parseConfig(t, `{"bucket":"b","credentials":{"auth_type":"access_key","access_key_id":"k","secret_access_key":"s"}}`)
	require.NoError(t, cfg.validate())
	assert.Equal(t, defaultRegion, cfg.Region)
	assert.Equal(t, defaultPathTemplate, cfg.PathTemplate)
	assert.Equal(t, defaultFileSizeMB, cfg.FileSizeMB)
	assert.Equal(t, formatJSONL, cfg.Format.FormatType)
	assert.Equal(t, compressionNone, cfg.Format.Compression)
	assert.Equal(t, ".jsonl", cfg.Format.extension())

	pq := parseConfig(t, `{"bucket":"b","format":{"format_type":"parquet"}}`)
	assert.Equal(t, compressionSnappy, pq.Format.Compression)
	assert.Equal(t, numberTypeDecimal, pq.Format.ParquetNumberType)
	assert.Equal(t, defaultDecimalScale, *pq.Format.DecimalScale)
	assert.Equal(t, ".parquet", pq.Format.extension())

	zero := parseConfig(t, `{"bucket":"b","format":{"format_type":"parquet","decimal_scale":0}}`)
	assert.Equal(t, 0, *zero.Format.DecimalScale)

	gz := parseConfig(t, `{"bucket":"b","format":{"format_type":"csv","compression":"gzip"}}`)
	assert.Equal(t, ".csv.gz", gz.Format.extension())
}

func TestConfigValidate(t *testing.T) {
	const creds = `"credentials":{"auth_type":"access_key","access_key_id":"k","secret_access_key":"s"}`
	cases := map[string]string{
		`{` + creds + `}`: "bucket is required",
		`{"bucket":"b","credentials":{"auth_type":"access_key"}}`:                                   "access_key_id and secret_access_key are required",
		`{"bucket":"b","credentials":{"auth_type":"assume_role"}}`:                                  "role_arn is required",
		`{"bucket":"b","credentials":{"auth_type":"oauth"}}`:                                        `unsupported auth_type "oauth"`,
		`{"bucket":"b",` + creds + `,"format":{"format_type":"avro"}}`:                              `unsupported format_type "avro"`,
		`{"bucket":"b",` + creds + `,"format":{"format_type":"jsonl","compression":"zstd"}}`:        "compression of jsonl must be one of none, gzip",
		`{"bucket":"b",` + creds + `,"format":{"format_type":"parquet","decimal_scale":39}}`:        "decimal_scale must be between 0 and 38",
		`{"bucket":"b",` + creds + `,"format":{"format_type":"parquet","parquet_number_type":"x"}}`: "parquet_number_type",
		`{"bucket":"b",` + creds + `,"file_size_mb":-1}`:                                            "file_size_mb must be between",
		`{"bucket":"b",` + creds + `,"max_records_per_file":-1}`:                                    "max_records_per_file must not be negative",
		`{"bucket":"b",` + creds + `,"path_template":"{stream}/{bogus}/{part}"}`:                    "path_template: unknown token",
	}
	for raw, want := range cases {
		t.Run(want, func(t *testing.T) {
			cfg := parseConfig(t, raw)
			err := cfg.validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), want)
		})
	}
	for _, ok := range []string{
		`{"bucket":"b","credentials":{"auth_type":"default_chain"}}`,
		`{"bucket":"b","credentials":{"auth_type":"assume_role","role_arn":"arn:aws:iam::1:role/r"}}`,
	} {
		cfg := parseConfig(t, ok)
		assert.NoError(t, cfg.validate(), ok)
	}
}

func TestSpecRejectsAppendDedup(t *testing.T) {
	spec, err := NewS3Destination().Spec(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"append", "overwrite"},
		[]string{string(spec.SupportedDestinationSyncModes[0]), string(spec.SupportedDestinationSyncModes[1])})
	assert.Len(t, spec.SupportedDestinationSyncModes, 2)
}
