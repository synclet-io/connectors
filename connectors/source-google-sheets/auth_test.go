package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateClientOption(t *testing.T) {
	t.Run("OAuth Client returns non-nil option", func(t *testing.T) {
		creds := CredentialsConfig{
			AuthType:     "Client",
			ClientID:     "test-client-id",
			ClientSecret: "test-client-secret",
			RefreshToken: "test-refresh-token",
		}
		opt, err := createClientOption(creds, "https://www.googleapis.com/auth/spreadsheets.readonly")
		require.NoError(t, err)
		assert.NotNil(t, opt)
	})

	t.Run("Service with invalid JSON returns error", func(t *testing.T) {
		creds := CredentialsConfig{
			AuthType:           "Service",
			ServiceAccountInfo: "not-valid-json",
		}
		_, err := createClientOption(creds, "https://www.googleapis.com/auth/spreadsheets.readonly")
		assert.Error(t, err)
	})

	t.Run("unknown auth_type returns error", func(t *testing.T) {
		creds := CredentialsConfig{
			AuthType: "Unknown",
		}
		_, err := createClientOption(creds, "https://www.googleapis.com/auth/spreadsheets.readonly")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported auth_type")
	})
}
