package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSpreadsheetID(t *testing.T) {
	for in, want := range map[string]string{
		"https://docs.google.com/spreadsheets/d/abc123/edit": "abc123",
		"abc123":   "abc123",
		" abc123 ": "abc123",
		"https://docs.google.com/spreadsheets/d/abc-XYZ_123/edit#gid=0": "abc-XYZ_123",
		"https://docs.google.com/spreadsheets/d/abcdef012345678901234/": "abcdef012345678901234",
	} {
		assert.Equal(t, want, parseSpreadsheetID(in), in)
	}
}

func decodeConfig(t *testing.T, raw string) Config {
	t.Helper()
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(raw), &cfg))
	return cfg
}

func TestConfigUnmarshal(t *testing.T) {
	t.Run("OAuth auth_type Client", func(t *testing.T) {
		cfg := decodeConfig(t, `{"spreadsheet_id":"abc123","credentials":{"auth_type":"Client","client_id":"cid","client_secret":"csecret","refresh_token":"rtoken"}}`)
		assert.Equal(t, "abc123", cfg.SpreadsheetID)
		assert.Equal(t, CredentialsConfig{AuthType: "Client", ClientID: "cid", ClientSecret: "csecret", RefreshToken: "rtoken"}, cfg.Credentials)
	})
	t.Run("ServiceAccount auth_type Service", func(t *testing.T) {
		cfg := decodeConfig(t, `{"spreadsheet_id":"abc123","credentials":{"auth_type":"Service","service_account_info":"{\"type\":\"service_account\"}"}}`)
		assert.Equal(t, "Service", cfg.Credentials.AuthType)
		assert.Equal(t, `{"type":"service_account"}`, cfg.Credentials.ServiceAccountInfo)
	})
	t.Run("defaults keep the behaviour of existing configurations", func(t *testing.T) {
		cfg := decodeConfig(t, `{"spreadsheet_id":"abc123","credentials":{"auth_type":"ApplicationDefault"}}`)
		cfg.normalize()
		require.NoError(t, cfg.validate())
		assert.Equal(t, defaultBatchSize, cfg.BatchSize)
		assert.False(t, cfg.NamesConversion)
		assert.Equal(t, columnNamingLegacy, cfg.ColumnNaming)
		assert.Equal(t, valueRenderFormatted, cfg.ValueRender)
		assert.False(t, cfg.Incremental)
		assert.Empty(t, cfg.AdditionalSpreadsheetIDs)
		assert.Equal(t, 60, cfg.requestsPerMinute())
		assert.Equal(t, []spreadsheetRef{{id: "abc123"}}, cfg.spreadsheets())
	})
	for raw, want := range map[string]int{`"200"`: 200, `200`: 200, `"0"`: defaultBatchSize, `-5`: defaultBatchSize, `"lots"`: defaultBatchSize} {
		t.Run("batch_size "+raw, func(t *testing.T) {
			cfg := decodeConfig(t, `{"spreadsheet_id":"abc123","credentials":{"auth_type":"Client"},"batch_size":`+raw+`}`)
			assert.Equal(t, want, cfg.BatchSize)
		})
	}
	for raw, want := range map[string]bool{`true`: true, `false`: false, `"true"`: true, `"false"`: false, `"True"`: true, `""`: false, `null`: false} {
		t.Run("names_conversion "+raw, func(t *testing.T) {
			cfg := decodeConfig(t, `{"spreadsheet_id":"abc123","credentials":{"auth_type":"Client"},"names_conversion":`+raw+`}`)
			assert.Equal(t, want, cfg.NamesConversion)
		})
	}
	t.Run("names_conversion yes is an error", func(t *testing.T) {
		var cfg Config
		assert.EqualError(t, json.Unmarshal([]byte(`{"names_conversion":"yes"}`), &cfg), `names_conversion must be true or false, got "yes"`)
	})
}

func TestConfigValidate(t *testing.T) {
	n := func(i int) *int { return &i }
	tests := []struct {
		name string
		edit func(*Config)
		err  string
	}{
		{name: "additional spreadsheets", edit: func(c *Config) {
			c.AdditionalSpreadsheetIDs = []string{"https://docs.google.com/spreadsheets/d/other/edit"}
		}},
		{name: "same spreadsheet twice", edit: func(c *Config) {
			c.AdditionalSpreadsheetIDs = []string{"https://docs.google.com/spreadsheets/d/abc123/edit"}
		}, err: "spreadsheet abc123 is configured twice"},
		{name: "empty additional", edit: func(c *Config) { c.AdditionalSpreadsheetIDs = []string{" "} }, err: "additional_spreadsheet_ids has an empty entry"},
		{name: "value_render", edit: func(c *Config) { c.ValueRender = "raw" }, err: `value_render must be formatted or unformatted, got "raw"`},
		{name: "requests_per_minute", edit: func(c *Config) { c.RequestsPerMinute = n(0) }, err: "requests_per_minute must be positive, got 0"},
		{name: "empty spreadsheet_id", edit: func(c *Config) { c.SpreadsheetID = "" }, err: "spreadsheet_id is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{SpreadsheetID: "abc123", Credentials: CredentialsConfig{AuthType: authClient}}
			tt.edit(&cfg)
			cfg.normalize()
			err := cfg.validate()
			if tt.err == "" {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err, tt.err)
		})
	}
}

func TestConfigRoundTrip(t *testing.T) {
	in := Config{SpreadsheetID: "abc", BatchSize: 5, NamesConversion: true, Incremental: true, Credentials: CredentialsConfig{AuthType: authClient}}
	raw, err := json.Marshal(in)
	require.NoError(t, err)
	assert.Equal(t, in, decodeConfig(t, string(raw)))
}
