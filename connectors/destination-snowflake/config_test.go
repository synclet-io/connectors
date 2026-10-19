package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/youmark/pkcs8"
)

// testKey is the key pair of the fake's test user.
var testKey = func() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
}()

func pkcs8PEM(t *testing.T, key *rsa.PrivateKey, password string) string {
	t.Helper()
	if password != "" {
		der, err := pkcs8.MarshalPrivateKey(key, []byte(password), nil)
		require.NoError(t, err)
		return string(pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: der}))
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
}

func testConfig(t *testing.T) Config {
	return Config{
		Account: "synclet-test", Warehouse: "SYNCLET_WH", Database: "SYNCLET_DB", Schema: "RAW",
		Credentials: Credentials{AuthType: authKeyPair, Username: "SYNCLET_TEST", PrivateKey: pkcs8PEM(t, testKey, "")},
	}
}

func TestConfigValidate(t *testing.T) {
	require.NoError(t, testConfig(t).validate())

	cfg := testConfig(t)
	cfg.Schema = ""
	cfg.applyDefaults()
	assert.Equal(t, "PUBLIC", cfg.Schema)

	err := Config{Credentials: Credentials{AuthType: "username/password"}}.validate()
	require.Error(t, err)
	for _, want := range []string{
		"account is required", "warehouse is required", "database is required",
		"credentials.username is required", `credentials.auth_type "username/password" is not "Key Pair Authentication"`,
	} {
		assert.Contains(t, err.Error(), want)
	}

	cfg = testConfig(t)
	cfg.Credentials.PrivateKey = ""
	assert.ErrorContains(t, cfg.validate(), "credentials.private_key is required")
}

func TestParsePrivateKey(t *testing.T) {
	k, err := parsePrivateKey(pkcs8PEM(t, testKey, ""), "")
	require.NoError(t, err)
	assert.True(t, k.Equal(testKey))

	pkcs1 := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(testKey)}))
	k, err = parsePrivateKey(pkcs1, "")
	require.NoError(t, err)
	assert.True(t, k.Equal(testKey))

	enc := pkcs8PEM(t, testKey, "secret")
	k, err = parsePrivateKey(enc, "secret")
	require.NoError(t, err)
	assert.True(t, k.Equal(testKey))

	_, err = parsePrivateKey(enc, "")
	assert.ErrorContains(t, err, "private_key is encrypted: set private_key_password")
	_, err = parsePrivateKey(enc, "wrong")
	assert.ErrorContains(t, err, "decrypt private_key")
	_, err = parsePrivateKey("not a key", "")
	assert.ErrorContains(t, err, "private_key is not a PEM block")
}

func rsaKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	return k
}
