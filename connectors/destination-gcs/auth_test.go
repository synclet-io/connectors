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

func TestClientSignsInWithKeyCarryingOAuthClientMembers(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	key := newTestServiceAccountKey(t, nil)
	key = key[:len(key)-1] + `,"installed":{"client_id":"c","redirect_uris":["http://127.0.0.1:1/cb"],"token_uri":"` + srv.URL + `/installed"}}`
	cfg := Config{Credentials: Credentials{AuthType: authServiceAccount, ServiceAccountInfo: key}}
	client, err := newGCSClientWithTokenURL(t.Context(), cfg, srv.URL+"/token")
	require.NoError(t, err)
	require.NoError(t, client.Close())
	assert.Equal(t, int32(1), hits.Load())
}
