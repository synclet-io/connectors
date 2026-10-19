package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// liveConfig is the configuration of a real Snowflake account from
// SNOWFLAKE_TEST_* variables. The test skips when SNOWFLAKE_TEST_ACCOUNT is
// unset.
func liveConfig(t *testing.T) Config {
	t.Helper()
	account := os.Getenv("SNOWFLAKE_TEST_ACCOUNT")
	if account == "" {
		t.Skip("set SNOWFLAKE_TEST_ACCOUNT, _USER, _PRIVATE_KEY or _PASSWORD, _WAREHOUSE, _DATABASE and optionally _ROLE to run against a real account")
	}
	cfg := Config{
		Account:   account,
		Warehouse: os.Getenv("SNOWFLAKE_TEST_WAREHOUSE"),
		Database:  os.Getenv("SNOWFLAKE_TEST_DATABASE"),
		Role:      os.Getenv("SNOWFLAKE_TEST_ROLE"),
		Credentials: CredentialsConfig{
			AuthType:           authKeyPair,
			Username:           os.Getenv("SNOWFLAKE_TEST_USER"),
			PrivateKey:         os.Getenv("SNOWFLAKE_TEST_PRIVATE_KEY"),
			PrivateKeyPassword: os.Getenv("SNOWFLAKE_TEST_PRIVATE_KEY_PASSWORD"),
		},
	}
	if cfg.Credentials.PrivateKey == "" {
		cfg.Credentials = CredentialsConfig{AuthType: authPassword, Username: cfg.Credentials.Username, Password: os.Getenv("SNOWFLAKE_TEST_PASSWORD")}
	}
	require.NoError(t, cfg.validate())
	return cfg
}

// liveFixture creates the tables of the fake's fixture in a new schema
// and drops the schema when the test ends.
var liveFixture = []string{
	`CREATE TABLE TYPES (C_NUM38 NUMBER(38,0), C_DEC NUMBER(10,2), C_FLOAT FLOAT, C_TEXT TEXT, C_BOOL BOOLEAN, C_DATE DATE, C_TIME TIME(9),
		C_NTZ TIMESTAMP_NTZ(9), C_LTZ TIMESTAMP_LTZ(9), C_TZ TIMESTAMP_TZ(9), C_VARIANT VARIANT, C_OBJECT OBJECT, C_ARRAY ARRAY, C_BINARY BINARY, C_GEO GEOGRAPHY)`,
	`INSERT INTO TYPES SELECT 12345678901234567890123456789012345678, -123.45, 1.5, 'héllo', TRUE, '2024-02-29', '23:59:59.123456',
		'2024-02-29 12:34:56.789', '2024-02-29 10:34:56.789 +00:00'::TIMESTAMP_TZ::TIMESTAMP_LTZ, '2024-02-29 12:34:56.789 +02:00',
		PARSE_JSON('{"a":[1,2],"n":123456789012345678901234567890}'), OBJECT_CONSTRUCT('k', 'x'), PARSE_JSON('[1,"two",null]')::ARRAY,
		TO_BINARY('00FF', 'HEX'), TO_GEOGRAPHY('POINT(30.5 50.45)')`,
	`INSERT INTO TYPES (C_NUM38) VALUES (NULL)`,
	`CREATE TABLE EVENTS (ID NUMBER(38,0) PRIMARY KEY, NAME TEXT, UPDATED_AT TIMESTAMP_TZ(9))`,
	`INSERT INTO EVENTS VALUES (1, 'one', '2024-01-01 00:00:00 +00:00'), (2, 'two', '2024-01-02 00:00:00 +00:00'),
		(3, 'three', '2024-01-03 02:00:00 +02:00'), (4, 'four', NULL)`,
}

func liveSchema(t *testing.T, cfg Config) (Source, Config) {
	t.Helper()
	ctx := context.Background()
	src := Source{}
	c, err := src.connect(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(c.close)
	suffix := make([]byte, 4)
	_, err = rand.Read(suffix)
	require.NoError(t, err)
	schema := "SYNCLET_TEST_" + hex.EncodeToString(suffix)
	_, err = c.db.ExecContext(ctx, `CREATE SCHEMA `+ident(c.database)+`.`+ident(schema))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := c.db.ExecContext(ctx, `DROP SCHEMA IF EXISTS `+ident(c.database)+`.`+ident(schema))
		require.NoError(t, err)
	})
	_, err = c.db.ExecContext(ctx, `USE SCHEMA `+ident(c.database)+`.`+ident(schema))
	require.NoError(t, err)
	for _, stmt := range liveFixture {
		_, err := c.db.ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}
	cfg.Schemas = []string{schema}
	return src, cfg
}

func TestLive(t *testing.T) {
	cfg := liveConfig(t)
	src, cfg := liveSchema(t, cfg)
	schema := cfg.Schemas[0]
	require.NoError(t, src.Check(t.Context(), cfg))

	cat, err := src.Discover(t.Context(), cfg)
	require.NoError(t, err)
	require.Len(t, cat.Streams, 2)
	events, types := cat.Streams[0], cat.Streams[1]
	require.JSONEq(t, eventsSchema, string(events.JSONSchema))
	require.Equal(t, [][]string{{"ID"}}, events.SourceDefinedPrimaryKey)
	require.JSONEq(t, typesSchema, string(types.JSONSchema))

	t.Run("types", func(t *testing.T) {
		out := runRead(t, src, cfg, catalog(fullRefresh(types)), nil)
		require.NoError(t, out.Err)
		got := rawRecords(t, out, types.Descriptor())
		require.Len(t, got, 2)
		require.JSONEq(t, typesRecords[0], got[0])
		require.JSONEq(t, typesRecords[1], got[1])
	})

	t.Run("incremental resume", func(t *testing.T) {
		desc := events.Descriptor()
		cat := catalog(incremental(events, "UPDATED_AT"))
		first := runRead(t, src, cfg, cat, nil)
		require.NoError(t, first.Err)
		require.Equal(t, []string{"1", "2", "3", "4"}, ids(t, first, desc))
		t3 := "2024-01-03T02:00:00.000000000+02:00"
		require.Equal(t, []streamState{{CursorField: []string{"UPDATED_AT"}, Cursor: &t3}}, streamStates(t, first))

		t2 := "2024-01-02T00:00:00.000000000+00:00"
		again := runRead(t, src, cfg, cat, stateFor(t, desc, streamState{CursorField: []string{"UPDATED_AT"}, Cursor: &t2}))
		require.NoError(t, again.Err)
		require.Equal(t, []string{"2", "3"}, ids(t, again, desc))
	})

	broken := cfg
	broken.Schemas = []string{schema + "_MISSING"}
	t.Run("conformance full refresh", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: mustJSON(t, cfg), BrokenConfig: mustJSON(t, broken),
			Catalog: catalog(fullRefresh(events), fullRefresh(types)),
		})
	})
	t.Run("conformance incremental", func(t *testing.T) {
		testkit.RunSourceSuite(t, src, testkit.SourceSuite{
			Config: mustJSON(t, cfg), BrokenConfig: mustJSON(t, broken),
			Catalog: &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{incremental(events, "UPDATED_AT")}},
		})
	})
}
