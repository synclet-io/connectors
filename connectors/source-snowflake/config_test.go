package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/youmark/pkcs8"
)

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

var testKey = func() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
}()

func pkcs8PEM(t *testing.T, password string) string {
	t.Helper()
	var der []byte
	var err error
	typ := "PRIVATE KEY"
	if password == "" {
		der, err = x509.MarshalPKCS8PrivateKey(testKey)
	} else {
		der, err = pkcs8.MarshalPrivateKey(testKey, []byte(password), nil)
		typ = "ENCRYPTED PRIVATE KEY"
	}
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}))
}

func TestConfigValidate(t *testing.T) {
	ok := `"account":"acme-test","warehouse":"WH","database":"DB"`
	cases := []struct {
		name string
		json string
		want string
	}{
		{"missing account", `{"warehouse":"WH","database":"DB","credentials":{"auth_type":"username/password","username":"u","password":"p"}}`, "account is required"},
		{"missing warehouse", `{"account":"a","database":"DB","credentials":{"auth_type":"username/password","username":"u","password":"p"}}`, "warehouse is required"},
		{"missing database", `{"account":"a","warehouse":"WH","credentials":{"auth_type":"username/password","username":"u","password":"p"}}`, "database is required"},
		{"empty schema", `{` + ok + `,"schemas":[""],"credentials":{"auth_type":"username/password","username":"u","password":"p"}}`, "schemas may not contain an empty name"},
		{"missing username", `{` + ok + `,"credentials":{"auth_type":"username/password","password":"p"}}`, "credentials.username is required"},
		{"missing password", `{` + ok + `,"credentials":{"auth_type":"username/password","username":"u"}}`, "credentials.password is required"},
		{"missing key", `{` + ok + `,"credentials":{"auth_type":"Key Pair Authentication","username":"u"}}`, "credentials.private_key is required"},
		{"missing auth", `{` + ok + `}`, `auth_type "" is not one of`},
		{"unknown auth", `{` + ok + `,"credentials":{"auth_type":"OAuth","username":"u"}}`, `auth_type "OAuth" is not one of`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg Config
			require.NoError(t, json.Unmarshal([]byte(tc.json), &cfg))
			require.ErrorContains(t, cfg.validate(), tc.want)
		})
	}
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(`{`+ok+`,"credentials":{"auth_type":"username/password","username":"u","password":"p"}}`), &cfg))
	require.NoError(t, cfg.validate())
}

func TestParsePrivateKey(t *testing.T) {
	pkcs1 := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(testKey)}))
	for name, tc := range map[string]struct{ pem, password string }{
		"pkcs8":           {pkcs8PEM(t, ""), ""},
		"pkcs1":           {pkcs1, ""},
		"encrypted pkcs8": {pkcs8PEM(t, "s3cret"), "s3cret"},
	} {
		t.Run(name, func(t *testing.T) {
			k, err := parsePrivateKey(tc.pem, tc.password)
			require.NoError(t, err)
			require.True(t, testKey.Equal(k))
		})
	}

	bad := []struct {
		name, pem, password, want string
	}{
		{"not pem", "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC", "", "private_key is not a PEM block"},
		{"wrong password", pkcs8PEM(t, "s3cret"), "other", "decrypt private_key"},
		{"encrypted without password", pkcs8PEM(t, "s3cret"), "", "private_key is encrypted: set private_key_password"},
		{"garbage der", "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n", "", "parse private_key"},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parsePrivateKey(tc.pem, tc.password)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
