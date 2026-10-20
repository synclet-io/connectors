package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenSourceRejectsForeignTokenURI(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	creds := CredentialsConfig{
		AuthType:           authService,
		ServiceAccountInfo: newTestServiceAccountKey(t, map[string]string{"token_uri": srv.URL + "/token"}),
	}
	ts, err := newTokenSource(t.Context(), srv.Client(), "", creds)
	if err == nil {
		_, err = ts.get()
	}
	require.EqualError(t, err, "service_account_info is not a valid service account key: token_uri must be https://oauth2.googleapis.com/token")
	assert.Zero(t, hits.Load())
}

func TestTokenSourceRejectsAuthorizedUserWithForeignTokenURI(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	creds := CredentialsConfig{
		AuthType:           authService,
		ServiceAccountInfo: `{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"r","token_uri":"` + srv.URL + `"}`,
	}
	ts, err := newTokenSource(t.Context(), srv.Client(), "", creds)
	if err == nil {
		_, err = ts.get()
	}
	require.EqualError(t, err, "service_account_info is not a valid service account key: token_uri must be https://oauth2.googleapis.com/token")
	assert.Zero(t, hits.Load())
}

func TestTokenSourceAuthorizedUserUsesGoogleTokenEndpoint(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	creds := CredentialsConfig{
		AuthType:           authService,
		ServiceAccountInfo: `{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"r","installed":{"redirect_uris":["http://127.0.0.1:1/cb"]}}`,
	}
	ts, err := newTokenSource(t.Context(), srv.Client(), srv.URL+"/token", creds)
	require.NoError(t, err)
	tok, err := ts.get()
	require.NoError(t, err)
	assert.Equal(t, "stolen", tok)
	assert.Equal(t, int32(1), hits.Load())
	assert.Equal(t, "the Google user of service_account_info", ts.account)
}

func TestTokenSourceRejectsExternalAccount(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	creds := CredentialsConfig{
		AuthType:           authService,
		ServiceAccountInfo: `{"type":"external_account","token_url":"` + srv.URL + `","credential_source":{"url":"` + srv.URL + `"}}`,
	}
	_, err := newTokenSource(t.Context(), srv.Client(), "", creds)
	require.EqualError(t, err, `service_account_info is not a valid service account key: type is "external_account", want service_account or authorized_user`)
	assert.Zero(t, hits.Load())
}

func TestTokenSourceAcceptsLegacyTokenURI(t *testing.T) {
	creds := CredentialsConfig{AuthType: authService, ServiceAccountInfo: newTestServiceAccountKey(t, map[string]string{"token_uri": legacyGoogleTokenURL})}
	ts, err := newTokenSource(t.Context(), nil, "", creds)
	require.NoError(t, err)
	assert.Equal(t, "service account reader@synclet-test.iam.gserviceaccount.com", ts.account)
}
