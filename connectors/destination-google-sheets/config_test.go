package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSpreadsheetID(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "full URL with /edit",
			input:    "https://docs.google.com/spreadsheets/d/abc123/edit",
			expected: "abc123",
		},
		{
			name:     "raw ID",
			input:    "abc123",
			expected: "abc123",
		},
		{
			name:     "full URL with gid fragment",
			input:    "https://docs.google.com/spreadsheets/d/abc-XYZ_123/edit#gid=0",
			expected: "abc-XYZ_123",
		},
		{
			name:     "URL with trailing slash",
			input:    "https://docs.google.com/spreadsheets/d/abcdef012345678901234/",
			expected: "abcdef012345678901234",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseSpreadsheetID(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestConfigUnmarshal(t *testing.T) {
	t.Run("spreadsheet_id URL extraction", func(t *testing.T) {
		data := `{
			"spreadsheet_id": "https://docs.google.com/spreadsheets/d/abc123/edit",
			"credentials": {"auth_type": "Client"}
		}`
		var cfg Config
		err := json.Unmarshal([]byte(data), &cfg)
		require.NoError(t, err)
		assert.Equal(t, "abc123", cfg.SpreadsheetID)
	})

	t.Run("OAuth auth_type Client", func(t *testing.T) {
		data := `{
			"spreadsheet_id": "abc123",
			"credentials": {
				"auth_type": "Client",
				"client_id": "cid",
				"client_secret": "csecret",
				"refresh_token": "rtoken"
			}
		}`
		var cfg Config
		err := json.Unmarshal([]byte(data), &cfg)
		require.NoError(t, err)
		assert.Equal(t, "abc123", cfg.SpreadsheetID)
		assert.Equal(t, "Client", cfg.Credentials.AuthType)
		assert.Equal(t, "cid", cfg.Credentials.ClientID)
		assert.Equal(t, "csecret", cfg.Credentials.ClientSecret)
		assert.Equal(t, "rtoken", cfg.Credentials.RefreshToken)
	})

	t.Run("ServiceAccount auth_type Service", func(t *testing.T) {
		data := `{
			"spreadsheet_id": "abc123",
			"credentials": {
				"auth_type": "Service",
				"service_account_info": "{\"type\":\"service_account\"}"
			}
		}`
		var cfg Config
		err := json.Unmarshal([]byte(data), &cfg)
		require.NoError(t, err)
		assert.Equal(t, "Service", cfg.Credentials.AuthType)
		assert.Equal(t, `{"type":"service_account"}`, cfg.Credentials.ServiceAccountInfo)
	})

	t.Run("ApplicationDefault auth_type", func(t *testing.T) {
		data := `{
			"spreadsheet_id": "abc123",
			"credentials": {
				"auth_type": "ApplicationDefault"
			}
		}`
		var cfg Config
		err := json.Unmarshal([]byte(data), &cfg)
		require.NoError(t, err)
		assert.Equal(t, "ApplicationDefault", cfg.Credentials.AuthType)
	})
}
