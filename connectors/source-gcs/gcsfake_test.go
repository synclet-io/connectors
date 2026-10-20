package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/stretchr/testify/require"
	apioption "google.golang.org/api/option"
)

// fakeGCS is an in-process Cloud Storage: fake-gcs-server behind a front
// that serves the OAuth token endpoint and plays IAM. A service account's
// access token is the local part of its client_email; the token of
// nolist@ cannot list objects, the one of noread@ cannot read them, and
// revoked@ gets no token at all.
type fakeGCS struct {
	gcs *fakestorage.Server
	srv *httptest.Server
}

func newFakeGCS(t *testing.T) *fakeGCS {
	t.Helper()
	gcs, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true, PublicHost: "127.0.0.1"})
	require.NoError(t, err)
	f := &fakeGCS{gcs: gcs}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() {
		f.srv.Close()
		gcs.Stop()
	})
	return f
}

func (f *fakeGCS) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/token" {
		f.token(w, r)
		return
	}
	tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	isList := strings.HasPrefix(r.URL.Path, "/storage/v1/b/") && strings.HasSuffix(strings.TrimSuffix(r.URL.Path, "/"), "/o")
	switch {
	case !ok:
		gcsError(w, http.StatusUnauthorized, "Anonymous caller does not have storage.objects.list access.")
	case tok == "nolist" && isList:
		gcsError(w, http.StatusForbidden, "nolist@synclet-test.iam.gserviceaccount.com does not have storage.objects.list access to the Google Cloud Storage bucket.")
	case tok == "noread" && !isList:
		gcsError(w, http.StatusForbidden, "noread@synclet-test.iam.gserviceaccount.com does not have storage.objects.get access to the Google Cloud Storage object.")
	default:
		f.gcs.HTTPHandler().ServeHTTP(w, r)
	}
}

func (f *fakeGCS) token(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	parts := strings.Split(r.Form.Get("assertion"), ".")
	var claims struct {
		Iss string `json:"iss"`
	}
	if len(parts) != 3 {
		http.Error(w, "no assertion", http.StatusBadRequest)
		return
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || json.Unmarshal(payload, &claims) != nil {
		http.Error(w, "bad assertion", http.StatusBadRequest)
		return
	}
	user, _, _ := strings.Cut(claims.Iss, "@")
	w.Header().Set("Content-Type", "application/json")
	if user == "revoked" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`))
		return
	}
	_, _ = w.Write([]byte(`{"access_token":"` + user + `","token_type":"Bearer","expires_in":3600}`))
}

func gcsError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(map[string]any{"error": map[string]any{
		"code": code, "message": msg, "errors": []map[string]string{{"message": msg, "domain": "global", "reason": "forbidden"}},
	}})
	_, _ = w.Write(b)
}

// source reaches the fake server instead of Cloud Storage.
func (f *fakeGCS) source() Source {
	return Source{clientOptions: []apioption.ClientOption{apioption.WithEndpoint(f.srv.URL + "/storage/v1/")}}
}

// config reaches bucket as the service account user@.
func (f *fakeGCS) config(t *testing.T, user, bucket string, streams ...StreamConfig) Config {
	return Config{
		Bucket:      bucket,
		Credentials: Credentials{AuthType: authService, ServiceAccountInfo: serviceAccountKey(t, user, f.srv.URL+"/token")},
		Streams:     streams,
	}
}

func (f *fakeGCS) createBucket(name string) {
	f.gcs.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: name})
}

func (f *fakeGCS) put(bucket, name string, data []byte, updated time.Time) {
	f.putObject(fakestorage.Object{ObjectAttrs: fakestorage.ObjectAttrs{BucketName: bucket, Name: name, Updated: updated}, Content: data})
}

func (f *fakeGCS) putObject(o fakestorage.Object) {
	if o.Created.IsZero() {
		o.Created = o.Updated
	}
	f.gcs.CreateObject(o)
}

var (
	keyOnce sync.Once
	keyPEM  string
)

// serviceAccountKey is a synthetic service account key of user@ whose
// tokens come from tokenURI.
func serviceAccountKey(t testing.TB, user, tokenURI string) string {
	t.Helper()
	keyOnce.Do(func() {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			panic(err)
		}
		keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	})
	email := user + "@synclet-test.iam.gserviceaccount.com"
	return string(mustJSON(t, map[string]string{
		"type":                        "service_account",
		"project_id":                  "synclet-test",
		"private_key_id":              "0000000000000000000000000000000000000000",
		"private_key":                 keyPEM,
		"client_email":                email,
		"client_id":                   "100000000000000000000",
		"auth_uri":                    "https://accounts.google.com/o/oauth2/auth",
		"token_uri":                   tokenURI,
		"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
		"client_x509_cert_url":        "https://www.googleapis.com/robot/v1/metadata/x509/" + url.PathEscape(email),
		"universe_domain":             "googleapis.com",
	}))
}
