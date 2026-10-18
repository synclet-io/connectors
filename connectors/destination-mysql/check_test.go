package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheck(t *testing.T) {
	cfg := sharedMySQL(t, "check_ok")
	execSQL(t, cfg, "CREATE USER IF NOT EXISTS 'reader'@'%' IDENTIFIED BY 'reader-pw'")
	execSQL(t, cfg, "GRANT SELECT, INSERT ON `check_ok`.* TO 'reader'@'%'")
	execSQL(t, cfg, "CREATE USER IF NOT EXISTS 'norename'@'%' IDENTIFIED BY 'norename-pw'")
	execSQL(t, cfg, "GRANT SELECT, INSERT, UPDATE, CREATE ON `check_ok`.* TO 'norename'@'%'")
	execSQL(t, cfg, "CREATE USER IF NOT EXISTS 'outsider'@'%' IDENTIFIED BY 'outsider-pw'")
	reader, norename, outsider := cfg, cfg, cfg
	reader.Username, reader.Password = "reader", "reader-pw"
	norename.Username, norename.Password = "norename", "norename-pw"
	outsider.Username, outsider.Password = "outsider", "outsider-pw"

	t.Run("succeeds and leaves nothing behind", func(t *testing.T) {
		require.NoError(t, Destination{}.Check(t.Context(), cfg))
		assert.Empty(t, tables(t, cfg))
	})

	cases := []struct {
		name string
		edit func(c *Config)
		want string
	}{
		{"wrong password", func(c *Config) { c.Password = "wrong" }, `cannot log in as "root": wrong user name or password`},
		{"unreachable host", func(c *Config) { c.Host, c.Port = "127.0.0.1", closedPort(t) }, "cannot reach 127.0.0.1:"},
		{"missing database", func(c *Config) { c.Database = "nope" }, `database "nope" does not exist`},
		{"no access to the database", func(c *Config) { *c = outsider }, `user "outsider" has no access to database "check_ok"`},
		{"no CREATE privilege", func(c *Config) { *c = reader }, `user "reader" has no CREATE privilege on database "check_ok", which it needs to create a table`},
		{"no privilege to rename", func(c *Config) { *c = norename }, `which it needs to rename a table`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bad := cfg
			c.edit(&bad)
			err := Destination{}.Check(t.Context(), bad)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
		})
	}
}

// selfSignedCA returns a PEM certificate of a CA that signed nothing.
func selfSignedCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "unrelated CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestCheckTLS(t *testing.T) {
	cfg := sharedMySQL(t, "check_tls")
	for _, mode := range []string{sslPreferred, sslRequired} {
		t.Run(mode, func(t *testing.T) {
			c := cfg
			c.SSLMode = SSLMode{Mode: mode}
			require.NoError(t, Destination{}.Check(t.Context(), c))
		})
	}
	t.Run("required runs over TLS", func(t *testing.T) {
		c := cfg
		c.SSLMode = SSLMode{Mode: sslRequired}
		rows := queryStrings(t, c, "SHOW SESSION STATUS LIKE 'Ssl_cipher'")
		assert.NotEmpty(t, rows[0][1])
	})
	t.Run("verify_ca with another CA fails", func(t *testing.T) {
		c := cfg
		c.SSLMode = SSLMode{Mode: sslVerifyCA, CACertificate: selfSignedCA(t)}
		err := Destination{}.Check(t.Context(), c)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "server certificate of")
		assert.Contains(t, err.Error(), "is not trusted (ssl_mode verify_ca)")
	})
}
