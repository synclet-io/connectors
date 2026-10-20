package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenSourceRejectsForeignTokenURI(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	creds := Credentials{
		AuthType:          authServiceAccount,
		ServiceAccountKey: newTestServiceAccountKey(t, map[string]string{"token_uri": srv.URL + "/token"}),
	}
	ts, err := newTokenSource(t.Context(), srv.Client(), googleTokenURL, creds)
	if err == nil {
		_, err = ts.get()
	}
	require.EqualError(t, err, "service_account_key is not a valid service account key: token_uri must be https://oauth2.googleapis.com/token")
	assert.Zero(t, hits.Load())
}

func TestTokenSourceAcceptsLegacyTokenURI(t *testing.T) {
	creds := Credentials{
		AuthType:          authServiceAccount,
		ServiceAccountKey: newTestServiceAccountKey(t, map[string]string{"token_uri": legacyGoogleTokenURL}),
	}
	_, err := newTokenSource(t.Context(), nil, googleTokenURL, creds)
	require.NoError(t, err)
}
