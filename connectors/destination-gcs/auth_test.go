package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientRejectsForeignTokenURI(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	cfg := Config{Credentials: Credentials{
		AuthType:           authServiceAccount,
		ServiceAccountInfo: newTestServiceAccountKey(t, map[string]string{"token_uri": srv.URL + "/token"}),
	}}
	_, err := newGCSClient(t.Context(), cfg)
	require.EqualError(t, err, "credentials: parsing service_account_info: token_uri must be https://oauth2.googleapis.com/token")
	assert.Zero(t, hits.Load())
}
