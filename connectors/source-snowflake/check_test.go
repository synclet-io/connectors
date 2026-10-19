package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	sessionSQL  = `SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_ROLE()`
	schemataSQL = `SELECT SCHEMA_NAME FROM "SYNCLET_DB".INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME <> 'INFORMATION_SCHEMA' ORDER BY SCHEMA_NAME`
	tablesSQL   = `SELECT TABLE_SCHEMA, TABLE_NAME FROM "SYNCLET_DB".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA' AND TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'MATERIALIZED VIEW') ORDER BY TABLE_SCHEMA, TABLE_NAME`
	probeSQL    = `SELECT 1 FROM "SYNCLET_DB"."PUBLIC"."EVENTS" LIMIT 1`
)

func passwordConfig() Config {
	return Config{
		Account: "synclet-test", Warehouse: "SYNCLET_WH", Database: "SYNCLET_DB", Schemas: []string{"PUBLIC"},
		Credentials: CredentialsConfig{AuthType: authPassword, Username: "SYNCLET_TEST", Password: "test-password"},
	}
}

func keyPairConfig(t *testing.T) Config {
	cfg := passwordConfig()
	cfg.Credentials = CredentialsConfig{AuthType: authKeyPair, Username: "SYNCLET_TEST", PrivateKey: pkcs8PEM(t, "")}
	return cfg
}

var (
	sessionOK = fakeQuery{sql: sessionSQL, columns: textColumns("CURRENT_WAREHOUSE()", "CURRENT_DATABASE()", "CURRENT_ROLE()"),
		rows: [][]*string{row(str("SYNCLET_WH"), str("SYNCLET_DB"), str("SYNCLET_ROLE"))}}
	schemataOK = fakeQuery{sql: schemataSQL, columns: textColumns("SCHEMA_NAME"), rows: [][]*string{row(str("OTHER")), row(str("PUBLIC"))}}
	tablesOK   = fakeQuery{sql: tablesSQL, columns: textColumns("TABLE_SCHEMA", "TABLE_NAME"),
		rows: [][]*string{row(str("OTHER"), str("A")), row(str("PUBLIC"), str("EVENTS")), row(str("PUBLIC"), str("TYPES"))}}
	probeOK = fakeQuery{sql: probeSQL, columns: []fakeColumn{{"1", "fixed"}}, rows: [][]*string{row(str("1"))}}
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

// TestLoginErrorDNS covers an account host that does not resolve, which the
// driver retries until its login timeout.
func TestLoginErrorDNS(t *testing.T) {
	err := loginError(passwordConfig(), fmt.Errorf("post login: %w", &net.DNSError{Err: "no such host", Name: "synclet-test.snowflakecomputing.com", IsNotFound: true}))
	require.ErrorContains(t, err, `account "synclet-test" not found: use the account identifier orgname-accountname or the account locator`)
	require.True(t, isConfigError(err))
}

func TestCheck(t *testing.T) {
	t.Run("password", func(t *testing.T) {
		src := newFake(t).on(sessionOK, schemataOK, tablesOK, probeOK).source()
		require.NoError(t, src.Check(t.Context(), passwordConfig()))
	})
	t.Run("key pair", func(t *testing.T) {
		src := newFake(t).on(sessionOK, schemataOK, tablesOK, probeOK).source()
		require.NoError(t, src.Check(t.Context(), keyPairConfig(t)))
	})
	t.Run("every schema", func(t *testing.T) {
		probeOther := fakeQuery{sql: `SELECT 1 FROM "SYNCLET_DB"."OTHER"."A" LIMIT 1`, columns: []fakeColumn{{"1", "fixed"}}}
		src := newFake(t).on(sessionOK, schemataOK, tablesOK, probeOK, probeOther).source()
		cfg := passwordConfig()
		cfg.Schemas = nil
		require.NoError(t, src.Check(t.Context(), cfg))
	})
}

func TestCheckFailures(t *testing.T) {
	otherKey := keyPairConfig(t)
	f := newFake(t)
	f.key = &rsaKey(t).PublicKey
	wrongKey := f.source()

	unknownUser := passwordConfig()
	unknownUser.Credentials.Username = "NOBODY"
	badPassword := passwordConfig()
	badPassword.Credentials.Password = "wrong"
	badPEM := passwordConfig()
	badPEM.Credentials = CredentialsConfig{AuthType: authKeyPair, Username: "SYNCLET_TEST", PrivateKey: "not a key"}
	lowerSchema := passwordConfig()
	lowerSchema.Schemas = []string{"public"}

	forbidden := newFake(t)
	forbidden.loginStatus = http.StatusForbidden
	roleNotGranted := newFake(t)
	roleNotGranted.loginErr = [2]string{"390189", "Role 'ANALYST' specified in the connect string is not granted to this user. Contact your local system administrator, or attempt to login with another role, e.g. PUBLIC."}
	analyst := passwordConfig()
	analyst.Role = "ANALYST"

	cases := []struct {
		name   string
		src    Source
		cfg    Config
		want   string
		config bool
	}{
		{"unknown user", newFake(t).source(), unknownUser, `login rejected for user "NOBODY": the username or password is wrong`, true},
		{"bad password", newFake(t).source(), badPassword, `login rejected for user "SYNCLET_TEST": the username or password is wrong`, true},
		{"key pair does not match", wrongKey, otherKey, `login rejected for user "SYNCLET_TEST": private_key does not match the user's RSA_PUBLIC_KEY, or the user does not exist`, true},
		{"key is not PEM", newFake(t).source(), badPEM, "private_key is not a PEM block", true},
		{"unknown account (HTTP 403)", forbidden.source(), passwordConfig(), `account "synclet-test" not found: use the account identifier orgname-accountname or the account locator`, true},
		{"role not granted", roleNotGranted.source(), analyst, `role "ANALYST" is not granted to user "SYNCLET_TEST": 390189`, true},
		{"warehouse missing", newFake(t).on(session(null, str("SYNCLET_DB"))).source(), passwordConfig(), `warehouse "SYNCLET_WH" does not exist, or role "SYNCLET_ROLE" has no USAGE privilege on it`, true},
		{"database missing", newFake(t).on(session(str("SYNCLET_WH"), null)).source(), passwordConfig(), `database "SYNCLET_DB" does not exist, or role "SYNCLET_ROLE" has no USAGE privilege on it`, true},
		{"schema missing", newFake(t).on(sessionOK, schemataOK).source(), lowerSchema, `schema "public" of database "SYNCLET_DB" does not exist, or role "SYNCLET_ROLE" has no USAGE privilege on it (unquoted names are stored in upper case: "PUBLIC")`, true},
		{"no SELECT", newFake(t).on(sessionOK, schemataOK, tablesOK, fakeQuery{sql: probeSQL, errCode: "002003", errMsg: "SQL compilation error:\nObject 'SYNCLET_DB.PUBLIC.EVENTS' does not exist or not authorized."}).source(), passwordConfig(), `role "SYNCLET_ROLE" has no SELECT privilege on "SYNCLET_DB"."PUBLIC"."EVENTS"`, true},
		{"no table visible", newFake(t).on(sessionOK, schemataOK, fakeQuery{sql: tablesSQL, columns: textColumns("TABLE_SCHEMA", "TABLE_NAME"), rows: [][]*string{row(str("OTHER"), str("A"))}}).source(), passwordConfig(), `schema "PUBLIC" has no table or view role "SYNCLET_ROLE" can read: grant it SELECT on the tables to sync`, true},
		{"warehouse suspended", newFake(t).on(sessionOK, schemataOK, tablesOK, fakeQuery{sql: probeSQL, errCode: "000606", errMsg: "No active warehouse selected in the current session.  Select an active warehouse with the 'use warehouse' command."}).source(), passwordConfig(), `warehouse "SYNCLET_WH" cannot run queries for role "SYNCLET_ROLE": it is missing, or the role has no USAGE privilege on it`, true},
		{"server error", newFake(t).on(sessionOK, schemataOK, tablesOK, fakeQuery{sql: probeSQL, errCode: "000603", errMsg: "SQL execution internal error"}).source(), passwordConfig(), `read "SYNCLET_DB"."PUBLIC"."EVENTS": 000603`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.src.Check(t.Context(), tc.cfg)
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, tc.config, isConfigError(err), "config error: %v", err)
		})
	}
}
