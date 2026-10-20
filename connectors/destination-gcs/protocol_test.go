package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const brokenConfig = `{"bucket":"b","credentials":{"auth_type":"oauth"}}`

func TestProtocolSpec(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), NewGCSDestination(), synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	assert.Equal(t, "object", spec.ConnectionSpecification.Type)
	assert.ElementsMatch(t, []synclet.DestinationSyncMode{synclet.DestinationSyncModeAppend, synclet.DestinationSyncModeOverwrite},
		spec.SupportedDestinationSyncModes)
}

func TestProtocolCheckFailure(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), NewGCSDestination(), synclet.Request{
		Command: synclet.CommandCheck,
		Config:  json.RawMessage(brokenConfig),
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
	assert.Contains(t, status.Message, `unsupported auth_type "oauth"`)
}

func TestProtocolWriteFailure(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), NewGCSDestination(), synclet.Request{
		Command: synclet.CommandWrite,
		Config:  json.RawMessage(brokenConfig),
		Catalog: json.RawMessage(`{"streams":[]}`),
		Input:   strings.NewReader(""),
	})
	require.NoError(t, err)
	require.Error(t, out.Err)
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Contains(t, traces[0].Message, `unsupported auth_type "oauth"`)
	assert.Empty(t, out.States())
}

// TestProtocolWrite drives Write through the SDK transport with one append
// and one overwrite stream, interleaved records and STATE messages.
func TestProtocolWrite(t *testing.T) {
	m := startFakeGCS(t)
	bucket := m.newBucket(t)
	m.putObject(bucket, "sync/shop/users/2026_01_01/old.parquet")
	m.putObject(bucket, "sync/shop/orders/2026_01_01/old.parquet")

	cfgJSON, err := json.Marshal(map[string]any{
		"bucket":      bucket,
		"bucket_path": "sync",
		"credentials": map[string]any{"auth_type": "application_default"},
		"format":      map[string]any{"format_type": "parquet"},
	})
	require.NoError(t, err)
	catalog, err := json.Marshal(fixtureCatalog(t,
		fixtureStream("orders", "shop", synclet.DestinationSyncModeAppend),
		fixtureStream("users", "shop", synclet.DestinationSyncModeOverwrite)))
	require.NoError(t, err)

	input := []string{
		recordMessage("orders", "shop", fixtureRecords[0]),
		recordMessage("users", "shop", fixtureRecords[1]),
		stateMessage("orders", "shop", 1),
		recordMessage("orders", "shop", fixtureRecords[2]),
		stateMessage("users", "shop", 2),
		recordMessage("users", "shop", fixtureRecords[2]),
		stateMessage("orders", "shop", 3),
	}
	dst := m.destination(nil)
	dst.now = func() time.Time { return testStart }
	out, err := testkit.RunDestination(t.Context(), dst, synclet.Request{
		Command: synclet.CommandWrite,
		Config:  cfgJSON,
		Catalog: catalog,
		Input:   strings.NewReader(strings.Join(input, "\n") + "\n"),
	})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	assert.Empty(t, out.ErrorTraces())

	var cursors []string
	for _, st := range out.States() {
		require.Equal(t, synclet.StateTypeStream, st.Type)
		cursors = append(cursors, st.Stream.StreamDescriptor.Name+":"+string(st.Stream.StreamState))
	}
	assert.Equal(t, []string{`orders:{"cursor":1}`, `orders:{"cursor":3}`, `users:{"cursor":2}`}, cursors,
		"append states in input order, the overwrite stream's state after the cleanup")

	keys, content := m.objects(t, bucket)
	var orders, users []map[string]any
	for _, k := range keys {
		switch {
		case strings.HasPrefix(k, "sync/shop/orders/2026_10_18/"):
			got, _ := readParquet(t, fixtureColumns(t), defaultDecimalScale, content[k])
			orders = append(orders, got...)
		case strings.HasPrefix(k, "sync/shop/users/2026_10_18/"):
			got, _ := readParquet(t, fixtureColumns(t), defaultDecimalScale, content[k])
			users = append(users, got...)
		}
	}
	assert.Contains(t, keys, "sync/shop/orders/2026_01_01/old.parquet", "append keeps previous objects")
	assert.NotContains(t, keys, "sync/shop/users/2026_01_01/old.parquet", "overwrite removes previous objects")
	want := expectedRecords(t)
	assert.Equal(t, []map[string]any{want[0], want[2]}, orders)
	assert.Equal(t, []map[string]any{want[1], want[2]}, users)
}
