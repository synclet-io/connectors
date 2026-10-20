package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestProtocolSpec(t *testing.T) {
	out, err := testkit.RunDestination(t.Context(), Destination{}, synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	spec := out.Spec()
	require.NotNil(t, spec)
	assert.Equal(t, "object", spec.ConnectionSpecification.Type)
	assert.Equal(t, []synclet.DestinationSyncMode{synclet.DestinationSyncModeAppend}, spec.SupportedDestinationSyncModes)

	got, err := json.Marshal(spec)
	require.NoError(t, err)
	want, err := os.ReadFile("spec.json")
	require.NoError(t, err)
	assert.JSONEq(t, string(want), string(got), "spec.json is out of date; run: task spec CONNECTOR=destination-kafka")
}

func TestProtocolSpecDefaultsAreValid(t *testing.T) {
	spec, err := Destination{}.Spec(t.Context())
	require.NoError(t, err)
	cfg := map[string]any{"bootstrap_servers": "k:9092", "topic_pattern": "{stream}"}
	for name, prop := range spec.ConnectionSpecification.Properties.Properties {
		if prop.Default != nil {
			cfg[string(name)] = prop.Default
		}
	}
	b, err := json.Marshal(cfg)
	require.NoError(t, err)
	var c Config
	require.NoError(t, json.Unmarshal(b, &c))
	s, err := c.settings()
	require.NoError(t, err)
	want, err := parseSettings(t, `{"bootstrap_servers":"k:9092","topic_pattern":"{stream}"}`)
	require.NoError(t, err)
	assert.Equal(t, want, s, "the form's defaults equal an empty configuration")
}

func TestProtocolCheckAndWrite(t *testing.T) {
	c := startCluster(t)
	status := runCheck(t, Destination{}, plainConfig(c, `"test_topic":"check"`))
	require.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)

	out := runWrite(t, Destination{}, plainConfig(c, ""), catalog(t, "public.users:id"),
		rec("public", "users", `{"id":1}`), state("users", 1),
		rec("public", "users", `{"id":2}`), state("users", 2))
	require.NoError(t, out.Err)
	states := out.States()
	require.Len(t, states, 2)
	assert.JSONEq(t, `{"cursor":1}`, string(states[0].Stream.StreamState))
	assert.JSONEq(t, `{"cursor":2}`, string(states[1].Stream.StreamState))
	consume(t, bootstrap(c), "public_users", 2)
}
