package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestProtocolSpec(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), NewDestination(), synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	assert.Equal(t, "object", spec.ConnectionSpecification.Type)
	assert.ElementsMatch(t, []synclet.DestinationSyncMode{
		synclet.DestinationSyncModeOverwrite, synclet.DestinationSyncModeAppend, synclet.DestinationSyncModeAppendDedup,
	}, spec.SupportedDestinationSyncModes)
}

func TestProtocolCheckFailure(t *testing.T) {
	cfg, err := json.Marshal(Config{Host: "127.0.0.1", Port: closedPort(t), Database: "d", Username: "u", SSLMode: SSLMode{Mode: sslDisable},
		UploadingMethod: UploadingMethod{BucketName: "staging", BucketRegion: "eu-west-1"}})
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), NewDestination(), synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, "cannot reach 127.0.0.1:")
}

// TestProtocolWrite writes the fixture, one stream per supported sync mode,
// through the SDK's transport and checks the echoed states, the files
// staged for each stream and the statements that load them.
func TestProtocolWrite(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	staged := &stagedRows{}
	r := &recorder{onExec: staged.hook(t, m)}
	out := runWriteRaw(t, recordingDestination(m, r), m.destConfig(bucket),
		readTestdata(t, "protocol_catalog.json"), readTestdata(t, "protocol_messages.jsonl"))
	require.NoError(t, out.Err)
	assert.Empty(t, out.ErrorTraces())
	var got []string
	for _, st := range out.States() {
		require.Equal(t, synclet.StateTypeStream, st.Type)
		got = append(got, st.Stream.StreamDescriptor.Name+" "+string(st.Stream.StreamState))
	}
	assert.Equal(t, []string{
		`events {"id":2}`,
		`users {"updated_at":"2024-02-01T00:00:00Z"}`,
		`rates {}`,
	}, got)

	assert.Len(t, staged.rows["stage/sync1/events/1.csv.gz"], 2)
	assert.Len(t, staged.rows["stage/sync1/users/1.csv.gz"], 3, "dedup happens in Redshift, after the load")
	rates := staged.rows["stage/sync1/rates/1.csv.gz"]
	require.Len(t, rates, 2)
	assert.Equal(t, []string{"EUR", "0.912345"}, rates[0][4:])

	log := strings.Join(r.log(), "\n")
	assert.Contains(t, log, `INSERT INTO "s"."events" (`)
	assert.Contains(t, log, `MERGE INTO "s"."users" USING "_synclet_dedup_1" AS src ON "s"."users"."id" = src."id"`)
	assert.Contains(t, log, `ALTER TABLE "s"."_synclet_tmp_rates" RENAME TO "rates"`)
}

// TestProtocolWriteFailedSwap fails the overwrite swap: the overwrite
// stream's state stays unechoed while the others' are echoed.
func TestProtocolWriteFailedSwap(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	r := &recorder{fail: func(sql string) error {
		if strings.HasPrefix(sql, "ALTER TABLE") {
			return errors.New("swap failed")
		}
		return nil
	}}
	out := runWriteRaw(t, recordingDestination(m, r), m.destConfig(bucket),
		readTestdata(t, "protocol_catalog.json"), readTestdata(t, "protocol_messages.jsonl"))
	require.Error(t, out.Err)
	var got []string
	for _, st := range out.States() {
		got = append(got, st.Stream.StreamDescriptor.Name)
	}
	assert.Equal(t, []string{"events", "users"}, got)
}
