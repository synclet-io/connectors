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
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// login creates a SQL Server login and returns cfg logged in as it.
func login(t *testing.T, cfg Config, name, password string) Config {
	t.Helper()
	master := cfg
	master.Database = "master"
	execSQL(t, master, "IF SUSER_ID(@p1) IS NULL EXEC(N'CREATE LOGIN '+@p2+' WITH PASSWORD = '''+@p3+''', CHECK_POLICY = OFF')",
		name, quote(name), password)
	c := cfg
	c.Username, c.Password = name, password
	return c
}

func TestCheck(t *testing.T) {
	cfg := sharedMSSQL(t, "check_ok")
	reader := login(t, cfg, "reader", "Reader-Test1!")
	execSQL(t, cfg, "CREATE USER reader FOR LOGIN reader; ALTER ROLE db_datareader ADD MEMBER reader; ALTER ROLE db_datawriter ADD MEMBER reader")
	creator := login(t, cfg, "creator", "Creator-Test1!")
	execSQL(t, cfg, "CREATE USER creator FOR LOGIN creator; GRANT CREATE TABLE TO creator")
	owner := login(t, cfg, "owner", "Owner-Test1!")
	execSQL(t, cfg, "CREATE USER owner FOR LOGIN owner; GRANT CREATE TABLE, CREATE SCHEMA TO owner; GRANT ALTER, INSERT, UPDATE, SELECT ON SCHEMA::dbo TO owner")
	outsider := login(t, cfg, "outsider", "Outsider-Test1!")

	t.Run("succeeds and leaves nothing behind", func(t *testing.T) {
		require.NoError(t, Destination{}.Check(t.Context(), cfg))
		assert.Empty(t, tables(t, cfg))
	})
	t.Run("a missing schema the user may create is not created", func(t *testing.T) {
		c := owner
		c.Schema = "fresh"
		require.NoError(t, Destination{}.Check(t.Context(), c))
		assert.Equal(t, [][]string{{"<NULL>"}}, queryStrings(t, cfg, "SELECT SCHEMA_ID('fresh')"))
	})
	t.Run("a user owning the schema's tables through grants succeeds", func(t *testing.T) {
		require.NoError(t, Destination{}.Check(t.Context(), owner))
	})
	t.Run("a user that passes check can sync every mode", func(t *testing.T) {
		streams := []synclet.ConfiguredStream{
			stream("o", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeOverwrite, nil, nil),
			stream("d", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, nil),
		}
		for _, schema := range []string{"dbo", "owned"} {
			c := owner
			c.Schema = schema
			for range 2 {
				out := runWrite(t, NewDestination(), c, streams, rec("o", `{"id":1}`), rec("d", `{"id":1}`))
				require.NoError(t, out.Err, schema)
			}
			assert.Equal(t, 1, countRows(t, c, "o"))
			assert.Equal(t, 1, countRows(t, c, "d"))
		}
		execSQL(t, cfg, "DROP TABLE dbo.o; DROP TABLE dbo.d")
	})

	cases := []struct {
		name string
		edit func(c *Config)
		want string
	}{
		{"wrong password", func(c *Config) { c.Password = "wrong" }, `cannot log in as "sa": wrong user name or password`},
		{"unknown user", func(c *Config) { c.Username = "nobody" }, `cannot log in as "nobody": wrong user name or password`},
		{"unreachable host", func(c *Config) { c.Host, c.Port = "127.0.0.1", closedPort(t) }, "cannot reach 127.0.0.1:"},
		{"unknown host", func(c *Config) { c.Host = "mssql.invalid" }, "cannot reach mssql.invalid:"},
		{"missing database", func(c *Config) { c.Database = "nope" }, `database "nope" does not exist`},
		{"login without a user in the database", func(c *Config) { *c = outsider }, `user "outsider" cannot open database "check_ok": the login has no user in it`},
		{"no CREATE TABLE permission", func(c *Config) { *c = reader }, `user "reader" may not create a table in schema "dbo" of database "check_ok": CREATE TABLE permission denied`},
		{"no ALTER on the schema", func(c *Config) { *c = creator }, `user "creator" may not create a table in schema "dbo" of database "check_ok"`},
		{"missing schema the user may not create", func(c *Config) { *c = creator; c.Schema = "nope" }, `user "creator" may not create schema "nope" in database "check_ok"`},
		{"invalid config", func(c *Config) { c.Host = "" }, "host is required"},
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
	cfg := sharedMSSQL(t, "check_tls")
	for _, method := range []string{sslUnencrypted, sslTrust} {
		t.Run(method, func(t *testing.T) {
			c := cfg
			c.SSLMethod = SSLMethod{SSLMethod: method}
			require.NoError(t, Destination{}.Check(t.Context(), c))
		})
	}
	t.Run("trust runs encrypted", func(t *testing.T) {
		c := cfg
		c.SSLMethod = SSLMethod{SSLMethod: sslTrust}
		rows := queryStrings(t, c, "SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID")
		assert.Equal(t, [][]string{{"TRUE"}}, rows)
	})
	t.Run("verify with another CA fails", func(t *testing.T) {
		c := cfg
		c.SSLMethod = SSLMethod{SSLMethod: sslVerify, Certificate: selfSignedCA(t)}
		err := Destination{}.Check(t.Context(), c)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "server certificate of")
		assert.Contains(t, err.Error(), "is not trusted (ssl_method encrypted_verify_certificate)")
	})
}
