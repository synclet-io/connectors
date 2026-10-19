package main

import (
	"errors"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	sessionSQL  = `SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_ROLE()`
	schemaSQL   = `SELECT COUNT(*) FROM "SYNCLET_DB".INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'RAW'`
	checkPrefix = `"SYNCLET_DB"."RAW"."_SYNCLET_CHECK_`
)

var (
	sessionOK = fakeQuery{sql: sessionSQL, columns: []string{"CURRENT_WAREHOUSE()", "CURRENT_DATABASE()", "CURRENT_ROLE()"},
		rows: [][]*string{row(str("SYNCLET_WH"), str("SYNCLET_DB"), str("SYNCLET_ROLE"))}}
	schemaExists  = fakeQuery{sql: schemaSQL, columns: []string{"COUNT(*)"}, rows: [][]*string{row(str("1"))}}
	schemaMissing = fakeQuery{sql: schemaSQL, columns: []string{"COUNT(*)"}, rows: [][]*string{row(str("0"))}}
)

func session(warehouse, database *string) fakeQuery {
	q := sessionOK
	q.rows = [][]*string{row(warehouse, database, str("SYNCLET_ROLE"))}
	return q
}

var configErrorType = reflect.TypeOf(synclet.ConfigError(errors.New("")))

// isConfigError reports whether err wraps a synclet.ConfigError.
func isConfigError(err error) bool {
	for ; err != nil; err = errors.Unwrap(err) {
		if reflect.TypeOf(err) == configErrorType {
			return true
		}
	}
	return false
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCheckPasses(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaExists)
	require.NoError(t, f.destination().Check(t.Context(), testConfig(t)))
	got := f.executed()
	require.Len(t, got, 5)
	assert.Equal(t, sessionSQL, got[0])
	assert.Equal(t, schemaSQL, got[1])
	assert.True(t, strings.HasPrefix(got[2], `CREATE TABLE `+checkPrefix), got[2])
	assert.True(t, strings.HasSuffix(got[2], `" ("ID" NUMBER(38,0))`), got[2])
	assert.True(t, strings.HasPrefix(got[3], `INSERT INTO `+checkPrefix), got[3])
	assert.True(t, strings.HasPrefix(got[4], `DROP TABLE IF EXISTS `+checkPrefix), got[4])
}

func TestCheckCreatesMissingSchema(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaMissing)
	require.NoError(t, f.destination().Check(t.Context(), testConfig(t)))
	assert.Equal(t, `CREATE SCHEMA IF NOT EXISTS "SYNCLET_DB"."RAW"`, f.executed()[2])
}

// TestCheckNamesTheCause runs one failing check per cause and asserts the
// message names it.
func TestCheckNamesTheCause(t *testing.T) {
	otherKey := testConfig(t)
	otherKey.Credentials.PrivateKey = pkcs8PEM(t, rsaKey(t), "")
	otherUser := testConfig(t)
	otherUser.Credentials.Username = "NOBODY"
	badPEM := testConfig(t)
	badPEM.Credentials.PrivateKey = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----"

	cases := []struct {
		name  string
		setup func(f *fakeSnowflake) *fakeSnowflake
		dst   func(d Destination) Destination
		cfg   Config
		want  []string
	}{
		{name: "key does not match the user", cfg: otherKey,
			want: []string{`login rejected for user "SYNCLET_TEST"`, "private_key does not match the user's RSA_PUBLIC_KEY, or the user does not exist"}},
		{name: "unknown user", cfg: otherUser,
			want: []string{`login rejected for user "NOBODY"`, "or the user does not exist"}},
		{name: "private key that does not parse", cfg: badPEM,
			want: []string{"credentials.private_key", "parse private_key"}},
		{name: "unknown account", setup: func(f *fakeSnowflake) *fakeSnowflake { f.loginStatus = http.StatusForbidden; return f },
			want: []string{`account "synclet-test" not found`}},
		{name: "account host not in DNS",
			dst: func(d Destination) Destination {
				d.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
					return nil, &net.DNSError{Err: "no such host", Name: r.URL.Host, IsNotFound: true}
				})
				return d
			},
			want: []string{`account "synclet-test" not found`}},
		{name: "role not granted", setup: func(f *fakeSnowflake) *fakeSnowflake {
			f.loginErr = [2]string{"390189", "Role 'ADMIN' specified in the connect string is not granted to this user."}
			return f
		}, want: []string{`role "ADMIN" is not granted to user "SYNCLET_TEST"`}},
		{name: "warehouse missing", setup: func(f *fakeSnowflake) *fakeSnowflake { return f.on(session(null, str("SYNCLET_DB"))) },
			want: []string{`warehouse "SYNCLET_WH" does not exist, or role "SYNCLET_ROLE" has no USAGE privilege on it`}},
		{name: "warehouse cannot run queries", setup: func(f *fakeSnowflake) *fakeSnowflake {
			return f.on(sessionOK, schemaExists).failOn("INSERT INTO", 0, "000606", "No active warehouse selected in the current session.")
		}, want: []string{`warehouse "SYNCLET_WH" cannot run queries for role "SYNCLET_ROLE"`}},
		{name: "database missing", setup: func(f *fakeSnowflake) *fakeSnowflake { return f.on(session(str("SYNCLET_WH"), null)) },
			want: []string{`database "SYNCLET_DB" does not exist, or role "SYNCLET_ROLE" has no USAGE privilege on it`}},
		{name: "database refused at login", setup: func(f *fakeSnowflake) *fakeSnowflake {
			f.loginErr = [2]string{"390201", "The requested database does not exist or not authorized."}
			return f
		}, want: []string{`database "SYNCLET_DB" or warehouse "SYNCLET_WH" does not exist or is not authorized for user "SYNCLET_TEST"`}},
		{name: "no CREATE TABLE on the schema", setup: func(f *fakeSnowflake) *fakeSnowflake {
			return f.on(sessionOK, schemaExists).failOn("CREATE TABLE", 0, "003001", "SQL access control error:\nInsufficient privileges to operate on schema 'RAW'")
		}, want: []string{`role "SYNCLET_ROLE" has no CREATE TABLE privilege on schema "SYNCLET_DB"."RAW"`}},
		{name: "no CREATE SCHEMA on the database", setup: func(f *fakeSnowflake) *fakeSnowflake {
			return f.on(sessionOK, schemaMissing).failOn("CREATE SCHEMA", 0, "003001", "SQL access control error:\nInsufficient privileges to operate on database 'SYNCLET_DB'")
		}, want: []string{`schema "RAW" does not exist and role "SYNCLET_ROLE" has no CREATE SCHEMA privilege on database "SYNCLET_DB"`}},
		{name: "no USAGE on the schema", setup: func(f *fakeSnowflake) *fakeSnowflake {
			return f.on(sessionOK, schemaExists).failOn("CREATE TABLE", 0, "002003", "Schema 'SYNCLET_DB.RAW' does not exist or not authorized.")
		}, want: []string{`role "SYNCLET_ROLE" has no USAGE privilege on schema "SYNCLET_DB"."RAW"`}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			if c.setup != nil {
				f = c.setup(f)
			}
			d := f.destination()
			if c.dst != nil {
				d = c.dst(d)
			}
			cfg := c.cfg
			if cfg.Account == "" {
				cfg = testConfig(t)
			}
			err := d.Check(t.Context(), cfg)
			require.Error(t, err)
			for _, w := range c.want {
				assert.Contains(t, err.Error(), w)
			}
			assert.True(t, isConfigError(err), "a cause the user fixes is a config error: %v", err)
		})
	}
}

func TestCheckInvalidConfig(t *testing.T) {
	err := NewDestination().Check(t.Context(), Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "account is required")
	assert.True(t, isConfigError(err))
}
