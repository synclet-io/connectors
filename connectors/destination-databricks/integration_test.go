package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

// liveConfig is the account the integration test runs against, from
// DATABRICKS_TEST_HOST, DATABRICKS_TEST_HTTP_PATH, DATABRICKS_TEST_CATALOG
// and DATABRICKS_TEST_TOKEN, or DATABRICKS_TEST_CLIENT_ID and
// DATABRICKS_TEST_CLIENT_SECRET. Without DATABRICKS_TEST_HOST the test skips.
func liveConfig(t *testing.T) Config {
	t.Helper()
	host := os.Getenv("DATABRICKS_TEST_HOST")
	if host == "" {
		t.Skip("DATABRICKS_TEST_HOST is not set")
	}
	cfg := Config{
		Hostname: host,
		HTTPPath: os.Getenv("DATABRICKS_TEST_HTTP_PATH"),
		Database: os.Getenv("DATABRICKS_TEST_CATALOG"),
		Schema:   "synclet_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12],
		Volume:   "staging",
		Authentication: Auth{
			AuthType:            authPAT,
			PersonalAccessToken: os.Getenv("DATABRICKS_TEST_TOKEN"),
		},
	}
	if id := os.Getenv("DATABRICKS_TEST_CLIENT_ID"); id != "" {
		cfg.Authentication = Auth{AuthType: authOAuth, ClientID: id, Secret: os.Getenv("DATABRICKS_TEST_CLIENT_SECRET")}
	}
	cfg.applyDefaults()
	require.NoError(t, cfg.validate())
	return cfg
}

// TestLiveWrite creates a schema and a volume of its own, runs check and
// the protocol fixture twice, and reads the tables back.
func TestLiveWrite(t *testing.T) {
	cfg := liveConfig(t)
	ctx := t.Context()
	d := NewDestination()
	run, err := d.open(ctx, cfg, d.client(), newBearer(ctx, cfg, d.client()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = run.close() })

	schema := ident(cfg.Database) + "." + ident(cfg.Schema)
	require.NoError(t, run.exec(ctx, "CREATE SCHEMA "+schema))
	t.Cleanup(func() {
		_ = run.exec(context.WithoutCancel(ctx), "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})
	require.NoError(t, run.exec(ctx, "CREATE VOLUME "+schema+"."+ident(cfg.Volume)))

	require.NoError(t, d.Check(ctx, cfg))

	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	catalog, err := os.ReadFile("testdata/protocol_catalog.json")
	require.NoError(t, err)
	loc := location{catalog: cfg.Database, schema: cfg.Schema}
	for i := range 2 {
		messages, err := os.Open("testdata/protocol_messages.jsonl")
		require.NoError(t, err)
		out, err := testkit.RunDestination(ctx, d, synclet.Request{Command: synclet.CommandWrite, Config: raw, Catalog: catalog, Input: messages})
		_ = messages.Close()
		require.NoError(t, err)
		require.NoError(t, out.Err, "sync %d", i+1)
		assert.Len(t, out.States(), 4, "sync %d", i+1)

		events, err := run.query(ctx, "SELECT count(*) AS n FROM "+loc.table("events"))
		require.NoError(t, err)
		assert.Equal(t, []map[string]string{{"n": []string{"2", "4"}[i]}}, events, "append adds rows")

		users, err := run.query(ctx, "SELECT id, name, CAST(updated_at AS STRING) AS updated_at FROM "+loc.table("users")+" ORDER BY id")
		require.NoError(t, err)
		assert.Equal(t, []map[string]string{
			{"id": "1", "name": "Ann B", "updated_at": "2024-02-01 00:00:00"},
			{"id": "2", "name": "Bob", "updated_at": "2024-01-15 00:00:00"},
		}, users, "append_dedup keeps the highest cursor per key")

		rates, err := run.query(ctx, "SELECT code, CAST(rate AS STRING) AS rate, CAST(day AS STRING) AS day FROM "+loc.table("rates")+" ORDER BY code")
		require.NoError(t, err)
		assert.Equal(t, []map[string]string{
			{"code": "EUR", "rate": "0.912345678", "day": "2024-01-02"},
			{"code": "UAH", "rate": "41.250000000", "day": "2024-01-02"},
		}, rates, "overwrite replaces")

		payload, err := run.query(ctx, "SELECT to_json(payload) AS p FROM "+loc.table("events")+" WHERE id = 1 LIMIT 1")
		require.NoError(t, err)
		assert.Equal(t, []map[string]string{{"p": `{"x":1}`}}, payload)
	}
	leftovers, err := run.query(ctx, "SHOW TABLES IN "+schema+" LIKE '_synclet_*'")
	require.NoError(t, err)
	assert.Empty(t, leftovers, "no shadow or stage table is left")
}
