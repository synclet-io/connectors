package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// liveConfig reads a real Redshift account from the environment; the test
// is skipped without REDSHIFT_TEST_HOST. The staging bucket is read with
// REDSHIFT_TEST_IAM_ROLE when set, with the AWS default credential chain of
// the test process otherwise.
func liveConfig(t *testing.T) Config {
	t.Helper()
	host := os.Getenv("REDSHIFT_TEST_HOST")
	if host == "" {
		t.Skip("REDSHIFT_TEST_HOST is not set; the live Redshift test runs only with a real account")
	}
	port := 5439
	if p := os.Getenv("REDSHIFT_TEST_PORT"); p != "" {
		n, err := strconv.Atoi(p)
		require.NoError(t, err)
		port = n
	}
	return Config{
		Host:     host,
		Port:     port,
		Database: os.Getenv("REDSHIFT_TEST_DATABASE"),
		Schema:   "synclet_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12],
		Username: os.Getenv("REDSHIFT_TEST_USER"),
		Password: os.Getenv("REDSHIFT_TEST_PASSWORD"),
		UploadingMethod: UploadingMethod{
			BucketName:   os.Getenv("REDSHIFT_TEST_S3_BUCKET"),
			BucketPath:   "synclet-it",
			BucketRegion: os.Getenv("REDSHIFT_TEST_S3_REGION"),
			IAMRoleARN:   os.Getenv("REDSHIFT_TEST_IAM_ROLE"),
		},
	}
}

func liveQuery(t *testing.T, cfg Config, sql string) []string {
	t.Helper()
	w, err := connect(t.Context(), cfg)
	require.NoError(t, err)
	defer w.close()
	rows, err := w.query(t.Context(), sql)
	require.NoError(t, err)
	out := make([]string, len(rows))
	for i, r := range rows {
		parts := make([]string, len(r))
		for j, v := range r {
			parts[j] = fmt.Sprint(v)
		}
		out[i] = strings.Join(parts, "|")
	}
	return out
}

// TestLiveRedshift checks, writes the protocol fixture twice and reads the
// tables back from a real cluster.
func TestLiveRedshift(t *testing.T) {
	cfg := liveConfig(t)
	t.Cleanup(func() {
		w, err := connect(context.Background(), cfg)
		if err == nil {
			_ = w.exec(context.Background(), "DROP SCHEMA IF EXISTS "+quote(cfg.Schema)+" CASCADE")
			w.close()
		}
	})
	require.NoError(t, NewDestination().Check(t.Context(), cfg))

	for i := range 2 {
		out := runWriteRaw(t, NewDestination(), cfg, readTestdata(t, "protocol_catalog.json"), readTestdata(t, "protocol_messages.jsonl"))
		require.NoError(t, out.Err, "sync %d", i+1)
		assert.Len(t, out.States(), 3)
		for _, st := range out.States() {
			assert.Equal(t, synclet.StateTypeStream, st.Type)
		}
		s := quote(cfg.Schema)
		assert.Equal(t, []string{strconv.Itoa(2 * (i + 1))}, liveQuery(t, cfg, "SELECT count(*) FROM "+s+".events"), "append adds rows")
		assert.Equal(t, []string{"1|Ann B", "2|Bob"}, liveQuery(t, cfg, "SELECT id, name FROM "+s+".users ORDER BY id"), "dedup keeps the highest cursor")
		assert.Equal(t, []string{"EUR|0.912345", "UAH|41.25"}, liveQuery(t, cfg, "SELECT code, rate FROM "+s+".rates ORDER BY code"), "overwrite replaces the rows")
	}
}
