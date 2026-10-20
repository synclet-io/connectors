package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testKeyPEM = sync.OnceValue(func() string {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		panic(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
})

// newTestServiceAccountKey returns a service account JSON key with a real
// RSA private key, with fields added or replaced by set; an empty value
// removes the field.
func newTestServiceAccountKey(t *testing.T, set map[string]string) string {
	t.Helper()
	key := map[string]string{
		"type":            "service_account",
		"project_id":      "synclet-test",
		"private_key_id":  "0000",
		"private_key":     testKeyPEM(),
		"client_email":    "reader@synclet-test.iam.gserviceaccount.com",
		"token_uri":       googleTokenURL,
		"universe_domain": googleUniverseDomain,
	}
	for k, v := range set {
		if v == "" {
			delete(key, k)
			continue
		}
		key[k] = v
	}
	raw, err := json.Marshal(key)
	require.NoError(t, err)
	return string(raw)
}

// foreignTokenServer counts the requests that reach it.
func foreignTokenServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"stolen","token_type":"Bearer","expires_in":3600}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestCheckServiceAccountKeyAcceptsGoogleTokenEndpoint(t *testing.T) {
	for name, uri := range map[string]string{
		"default":  googleTokenURL,
		"legacy":   legacyGoogleTokenURL,
		"no field": "",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := checkServiceAccountKey([]byte(newTestServiceAccountKey(t, map[string]string{"token_uri": uri})), googleTokenURL)
			require.NoError(t, err)
			var key map[string]string
			require.NoError(t, json.Unmarshal(got, &key))
			assert.Equal(t, googleTokenURL, key["token_uri"])
			assert.Equal(t, "reader@synclet-test.iam.gserviceaccount.com", key["client_email"])
			assert.Equal(t, testKeyPEM(), key["private_key"])
		})
	}
}

func TestCheckServiceAccountKeySetsTokenURL(t *testing.T) {
	got, err := checkServiceAccountKey([]byte(newTestServiceAccountKey(t, nil)), "http://127.0.0.1:1/token")
	require.NoError(t, err)
	var key map[string]string
	require.NoError(t, json.Unmarshal(got, &key))
	assert.Equal(t, "http://127.0.0.1:1/token", key["token_uri"])
}

func TestCheckServiceAccountKeyRejects(t *testing.T) {
	srv, _ := foreignTokenServer(t)
	cases := map[string]struct {
		key, message string
	}{
		"token_uri of another host": {
			key:     newTestServiceAccountKey(t, map[string]string{"token_uri": srv.URL}),
			message: "token_uri must be https://oauth2.googleapis.com/token",
		},
		"token_uri on Google's host over http": {
			key:     newTestServiceAccountKey(t, map[string]string{"token_uri": "http://oauth2.googleapis.com/token"}),
			message: "token_uri must be https://oauth2.googleapis.com/token",
		},
		"token_uri not a string": {
			key:     `{"type":"service_account","token_uri":1}`,
			message: "token_uri is not a string",
		},
		"another universe": {
			key:     newTestServiceAccountKey(t, map[string]string{"universe_domain": "example.com"}),
			message: "universe_domain must be googleapis.com",
		},
		"another type": {
			key:     `{"type":"external_account","token_url":"` + srv.URL + `"}`,
			message: `type is "external_account", a service account key has type service_account`,
		},
		"no type": {
			key:     `{"token_uri":"` + googleTokenURL + `"}`,
			message: `type is "", a service account key has type service_account`,
		},
		"not an object": {
			key:     `"key"`,
			message: "the key is not a JSON object",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := checkServiceAccountKey([]byte(tc.key), googleTokenURL)
			require.EqualError(t, err, tc.message)
			assert.NotContains(t, err.Error(), "PRIVATE KEY")
		})
	}
}
