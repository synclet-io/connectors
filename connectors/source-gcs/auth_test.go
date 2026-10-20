package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckRejectsForeignTokenURI(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	f := newFakeGCS(t)
	f.createBucket("check")
	f.put("check", "data/a.csv", []byte(datasetCSV), t0)
	cfg := f.config(t, "reader", "check", StreamConfig{Name: "s", Glob: "data/*.csv", Format: Format{Filetype: formatCSV}})
	cfg.Credentials.ServiceAccountInfo = newTestServiceAccountKey(t, map[string]string{"token_uri": srv.URL + "/token"})
	err := Source{clientOptions: f.source().clientOptions}.Check(t.Context(), cfg)
	require.ErrorContains(t, err, "service_account_info is not a valid service account key: token_uri must be https://oauth2.googleapis.com/token")
	assert.Zero(t, hits.Load())
}

func TestResolveCredentialsRejectsForeignTokenURI(t *testing.T) {
	srv, hits := foreignTokenServer(t)
	cr := Credentials{
		AuthType:           authService,
		ServiceAccountInfo: newTestServiceAccountKey(t, map[string]string{"token_uri": srv.URL + "/token"}),
	}
	creds, err := resolveCredentials(cr, googleTokenURL)
	if err == nil {
		_, err = creds.Token(t.Context())
	}
	require.EqualError(t, err, "service_account_info is not a valid service account key: token_uri must be https://oauth2.googleapis.com/token")
	assert.Zero(t, hits.Load())
}
