package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateClientOptionRejectsForeignTokenURI(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	creds := CredentialsConfig{
		AuthType:           "Service",
		ServiceAccountInfo: newTestServiceAccountKey(t, map[string]string{"token_uri": srv.URL + "/token"}),
	}
	_, err := createClientOption(t.Context(), creds, bigqueryScope)
	require.EqualError(t, err, "service_account_info is not a valid service account key: token_uri must be https://oauth2.googleapis.com/token")
	assert.Zero(t, hits.Load())
}

func TestCreateClientOptionRejectsOtherCredentialTypes(t *testing.T) {
	creds := CredentialsConfig{
		AuthType:           "Service",
		ServiceAccountInfo: `{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"r"}`,
	}
	_, err := createClientOption(t.Context(), creds, bigqueryScope)
	require.EqualError(t, err, `service_account_info is not a valid service account key: type is "authorized_user", a service account key has type service_account`)
}

func TestCreateClientOptionAcceptsServiceAccountKey(t *testing.T) {
	creds := CredentialsConfig{AuthType: "Service", ServiceAccountInfo: newTestServiceAccountKey(t, map[string]string{"token_uri": ""})}
	_, err := createClientOption(t.Context(), creds, bigqueryScope)
	require.NoError(t, err)
}
