package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
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
	cfg := parseConfig(t, `{"bucket":"b","credentials":{"service_account_info":"{\"type\":\"service_account\"}"}}`)
	require.NoError(t, cfg.validate())
	assert.Equal(t, authServiceAccount, cfg.Credentials.AuthType)
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
	const creds = `"credentials":{"auth_type":"application_default"}`
	cases := map[string]string{
		`{` + creds + `}`: "bucket is required",
		`{"bucket":"b","credentials":{"auth_type":"service_account"}}`:                              "service_account_info is required",
		`{"bucket":"b","credentials":{"auth_type":"service_account","service_account_info":"{"}}`:   "service_account_info is not valid JSON",
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
	cfg := parseConfig(t, `{"bucket":"b","credentials":{"auth_type":"application_default"}}`)
	assert.NoError(t, cfg.validate())
}

func TestClientRejectsKeyThatIsNotAServiceAccount(t *testing.T) {
	cfg := parseConfig(t, `{"bucket":"b","credentials":{"service_account_info":"{\"type\":\"authorized_user\",\"client_id\":\"c\",\"client_secret\":\"s\",\"refresh_token\":\"r\"}"}}`)
	require.NoError(t, cfg.validate())
	_, err := newGCSClient(t.Context(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing service_account_info")
}

func TestSpecRejectsAppendDedup(t *testing.T) {
	spec, err := NewGCSDestination().Spec(t.Context())
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"append", "overwrite"},
		[]string{string(spec.SupportedDestinationSyncModes[0]), string(spec.SupportedDestinationSyncModes[1])})
	assert.Len(t, spec.SupportedDestinationSyncModes, 2)
}

func TestClientRejectsKeyThatCannotSignIn(t *testing.T) {
	tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`))
	}))
	t.Cleanup(tokenServer.Close)
	pk, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(pk)
	require.NoError(t, err)
	key, err := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     "synclet-test",
		"private_key_id": "0000",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "writer@synclet-test.iam.gserviceaccount.com",
		"token_uri":      tokenServer.URL,
	})
	require.NoError(t, err)
	cfg := Config{Credentials: Credentials{AuthType: authServiceAccount, ServiceAccountInfo: string(key)}}
	_, err = newGCSClient(t.Context(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid credentials")
	assert.Contains(t, err.Error(), "invalid_grant")
}
