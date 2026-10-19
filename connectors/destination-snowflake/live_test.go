package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	sf "github.com/snowflakedb/gosnowflake/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// liveConfig is the configuration of a real account from SNOWFLAKE_TEST_*,
// with a scratch schema; the test is skipped without SNOWFLAKE_TEST_ACCOUNT.
func liveConfig(t *testing.T) Config {
	t.Helper()
	account := os.Getenv("SNOWFLAKE_TEST_ACCOUNT")
	if account == "" {
		t.Skip("SNOWFLAKE_TEST_ACCOUNT is not set")
	}
	return Config{
		Account:   account,
		Warehouse: os.Getenv("SNOWFLAKE_TEST_WAREHOUSE"),
		Database:  os.Getenv("SNOWFLAKE_TEST_DATABASE"),
		Role:      os.Getenv("SNOWFLAKE_TEST_ROLE"),
		Schema:    fmt.Sprintf("SYNCLET_TEST_%d", time.Now().UnixNano()),
		Credentials: Credentials{
			AuthType:           authKeyPair,
			Username:           os.Getenv("SNOWFLAKE_TEST_USER"),
			PrivateKey:         os.Getenv("SNOWFLAKE_TEST_PRIVATE_KEY"),
			PrivateKeyPassword: os.Getenv("SNOWFLAKE_TEST_PRIVATE_KEY_PASSWORD"),
		},
	}
}

// liveDB opens a session of cfg for the test's own queries and drops the
// scratch schema at the end.
func liveDB(t *testing.T, cfg Config) *sql.DB {
	t.Helper()
	dc, err := NewDestination().driverConfig(cfg)
	require.NoError(t, err)
	db := sql.OpenDB(sf.NewConnector(sf.SnowflakeDriver{}, dc))
	t.Cleanup(func() {
		_, err := db.Exec(`DROP SCHEMA IF EXISTS ` + ident(cfg.Database, cfg.Schema))
		assert.NoError(t, err)
		_ = db.Close()
	})
	return db
}

func liveRows(t *testing.T, db *sql.DB, q string) [][]string {
	t.Helper()
	rows, err := db.Query(q)
	require.NoError(t, err)
	out, err := resultRows(rows)
	require.NoError(t, err)
	var got [][]string
	for _, r := range out {
		var cells []string
		for _, k := range strings.Split(r["__order"], ",") {
			cells = append(cells, r[k])
		}
		got = append(got, cells)
	}
	return got
}

func liveRun(t *testing.T, cfg Config, catalog, messages string) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	cat, err := os.ReadFile("testdata/" + catalog)
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), NewDestination(), synclet.Request{
		Command: synclet.CommandWrite, Config: raw, Catalog: cat, Input: strings.NewReader(messages),
	})
	require.NoError(t, err)
	return out
}

// TestLive checks, writes the protocol fixture twice and the type fixture
// once against a real account and reads the tables back.
func TestLive(t *testing.T) {
	cfg := liveConfig(t)
	db := liveDB(t, cfg)
	require.NoError(t, NewDestination().Check(t.Context(), cfg))

	messages := readTestdata(t, "protocol_messages.jsonl")
	for i := range 2 {
		out := liveRun(t, cfg, "protocol_catalog.json", messages)
		require.NoError(t, out.Err, "sync %d", i+1)
		assert.Equal(t, []string{`events {"id":2}`, `users {"updated_at":"2024-02-01T00:00:00Z"}`, `rates {}`}, stateNames(out))
	}
	s := ident(cfg.Database, cfg.Schema)
	assert.Equal(t, [][]string{{"4"}}, liveRows(t, db, `SELECT COUNT(*) AS c, 'c' AS "__order" FROM `+s+`."EVENTS"`), "append adds rows")
	assert.Equal(t, [][]string{{"1", "Ann B", "2024-02-01 00:00:00.000 +0000"}, {"2", "", "2024-01-15 10:00:00.000 +0200"}},
		liveRows(t, db, `SELECT TO_VARCHAR(id) AS id, name, TO_VARCHAR(updated_at, 'YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM') AS u, 'id,name,u' AS "__order" FROM `+s+`.users ORDER BY id`),
		"dedup keeps the highest cursor; unquoted names resolve")
	assert.Equal(t, [][]string{{"EUR", "0.912345678"}, {"UAH", "41.250000000"}, {"XXX", ""}},
		liveRows(t, db, `SELECT code, TO_VARCHAR(rate) AS rate, 'code,rate' AS "__order" FROM `+s+`.rates ORDER BY code`),
		"overwrite replaces the rows")

	out := liveRun(t, cfg, "types_catalog.json", readTestdata(t, "types_messages.jsonl"))
	require.NoError(t, out.Err)
	assert.Equal(t, [][]string{{
		"12345678901234567890123456789012345678", "true", "12345678901234567890.123456789", "2024-02-29",
		"2024-01-02 03:04:05.123456789 +0530", "2024-01-02 03:04:05.123456789", "23:59:59.999999999",
		`{"a":12345678901234567890123456789012345678}`, `["x","y"]`, "TIMESTAMP_TZ,TIMESTAMP_NTZ,VARIANT",
	}}, liveRows(t, db, `SELECT TO_VARCHAR(big) AS big, TO_VARCHAR(flag) AS flag, TO_VARCHAR(amount) AS amount, TO_VARCHAR(day) AS day,
		TO_VARCHAR(at_tz, 'YYYY-MM-DD HH24:MI:SS.FF9 TZHTZM') AS at_tz, TO_VARCHAR(at_ntz, 'YYYY-MM-DD HH24:MI:SS.FF9') AS at_ntz,
		TO_VARCHAR(clock, 'HH24:MI:SS.FF9') AS clock, TO_JSON(payload) AS payload, TO_JSON(tags) AS tags,
		(SELECT LISTAGG(DATA_TYPE, ',') WITHIN GROUP (ORDER BY COLUMN_NAME) FROM `+ident(cfg.Database)+`.INFORMATION_SCHEMA.COLUMNS
		 WHERE TABLE_SCHEMA = `+sqlString(cfg.Schema)+` AND TABLE_NAME = 'ALL_TYPES' AND COLUMN_NAME IN ('AT_TZ', 'AT_NTZ', 'PAYLOAD')) AS types,
		'big,flag,amount,day,at_tz,at_ntz,clock,payload,tags,types' AS "__order" FROM `+s+`.all_types`))
}
