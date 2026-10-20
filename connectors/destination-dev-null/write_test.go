package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func rec(stream string, id int) string {
	return fmt.Sprintf(`{"type":"RECORD","record":{"stream":%q,"data":{"id":%d},"emitted_at":1700000000000}}`, stream, id)
}

func state(stream string, cursor int) string {
	return fmt.Sprintf(`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":%q},"stream_state":{"cursor":%d}}}}`, stream, cursor)
}

func records(stream string, n int) []string {
	lines := make([]string, n)
	for i := range n {
		lines[i] = rec(stream, i)
	}
	return lines
}

func runWrite(t *testing.T, config string, lines ...string) *testkit.Output {
	t.Helper()
	out, err := testkit.RunDestination(t.Context(), Destination{}, synclet.Request{
		Command: synclet.CommandWrite,
		Config:  json.RawMessage(config),
		Catalog: json.RawMessage(`{"streams":[]}`),
		Input:   strings.NewReader(strings.Join(lines, "\n") + "\n"),
	})
	require.NoError(t, err)
	return out
}

func stateCursors(out *testkit.Output) []string {
	var got []string
	for _, st := range out.States() {
		got = append(got, st.Stream.StreamDescriptor.Name+" "+string(st.Stream.StreamState))
	}
	return got
}

func logMessages(out *testkit.Output) []string {
	var got []string
	for _, l := range out.Logs() {
		if strings.HasPrefix(l.Message, "Logging Destination(") {
			got = append(got, l.Message)
		}
	}
	return got
}

func loggingConfig(inner string) string {
	return `{"test_destination":{"test_destination_type":"LOGGING","logging_config":` + inner + `}}`
}

func TestWriteSilentEchoesStatesInOrder(t *testing.T) {
	out := runWrite(t, `{"test_destination":{"test_destination_type":"SILENT"}}`,
		rec("users", 1), rec("users", 2), state("users", 2),
		rec("orders", 1), state("orders", 1),
		rec("users", 3), state("users", 3),
	)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`users {"cursor":2}`, `orders {"cursor":1}`, `users {"cursor":3}`}, stateCursors(out))
	assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: "users"}))
	assert.Empty(t, logMessages(out))
	for _, m := range out.Messages {
		assert.NotEqual(t, synclet.MessageTypeRecord, m.Type)
	}
}

func TestWriteEchoesGlobalState(t *testing.T) {
	global := `{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"lsn":42},"stream_states":[{"stream_descriptor":{"name":"users"},"stream_state":{"c":1}}]}}}`
	out := runWrite(t, `{}`, rec("users", 1), global)
	require.NoError(t, out.Err)
	states := out.States()
	require.Len(t, states, 1)
	assert.Equal(t, synclet.StateTypeGlobal, states[0].Type)
	assert.JSONEq(t, `{"lsn":42}`, string(states[0].Global.SharedState))
}

func TestWriteLoggingFirstN(t *testing.T) {
	lines := append(records("users", 10), records("orders", 2)...)
	out := runWrite(t, loggingConfig(`{"logging_type":"FirstN","max_entry_count":3}`), append(lines, state("users", 9))...)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		`Logging Destination(stream=users, recordIndex=0, logEntry=1/3): {"id":0}`,
		`Logging Destination(stream=users, recordIndex=1, logEntry=2/3): {"id":1}`,
		`Logging Destination(stream=users, recordIndex=2, logEntry=3/3): {"id":2}`,
		`Logging Destination(stream=orders, recordIndex=0, logEntry=1/3): {"id":0}`,
		`Logging Destination(stream=orders, recordIndex=1, logEntry=2/3): {"id":1}`,
	}, logMessages(out))
	for _, l := range out.Logs() {
		assert.Equal(t, synclet.LogLevelInfo, l.Level)
	}
	assert.Equal(t, []string{`users {"cursor":9}`}, stateCursors(out))
}

func TestWriteLoggingEveryNth(t *testing.T) {
	out := runWrite(t, loggingConfig(`{"logging_type":"EveryNth","nth_entry_to_log":3,"max_entry_count":3}`), records("users", 20)...)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		`Logging Destination(stream=users, recordIndex=0, logEntry=1/3): {"id":0}`,
		`Logging Destination(stream=users, recordIndex=3, logEntry=2/3): {"id":3}`,
		`Logging Destination(stream=users, recordIndex=6, logEntry=3/3): {"id":6}`,
	}, logMessages(out))
}

func TestWriteLoggingRandomSampling(t *testing.T) {
	cfg := loggingConfig(`{"logging_type":"RandomSampling","sampling_ratio":0.1,"seed":1900,"max_entry_count":1000}`)
	out := runWrite(t, cfg, records("users", 5000)...)
	require.NoError(t, out.Err)
	logged := logMessages(out)
	assert.InDelta(t, 500, len(logged), 100)

	again := runWrite(t, cfg, records("users", 5000)...)
	assert.Equal(t, logged, logMessages(again), "a seed makes the sample repeatable")

	capped := runWrite(t, loggingConfig(`{"logging_type":"RandomSampling","sampling_ratio":1,"max_entry_count":4}`), records("users", 50)...)
	assert.Len(t, logMessages(capped), 4)

	none := runWrite(t, loggingConfig(`{"logging_type":"RandomSampling","sampling_ratio":0,"max_entry_count":4}`), records("users", 50)...)
	assert.Empty(t, logMessages(none))
}

func TestWriteThrottled(t *testing.T) {
	start := time.Now()
	out := runWrite(t, `{"test_destination":{"test_destination_type":"THROTTLED","millis_per_record":20}}`,
		append(records("users", 5), state("users", 4))...)
	elapsed := time.Since(start)
	require.NoError(t, out.Err)
	assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond)
	assert.Less(t, elapsed, 2*time.Second)
	assert.Equal(t, []string{`users {"cursor":4}`}, stateCursors(out))
}

func TestWriteFailing(t *testing.T) {
	cfg := `{"test_destination":{"test_destination_type":"FAILING","num_messages":3}}`
	out := runWrite(t, cfg,
		rec("users", 0), rec("users", 1), state("users", 1),
		rec("orders", 0), rec("orders", 1), rec("orders", 2), state("orders", 2),
		rec("users", 2), state("users", 2),
		rec("users", 3), state("users", 3),
	)
	require.Error(t, out.Err)
	assert.Equal(t, []string{`users {"cursor":1}`, `orders {"cursor":2}`, `users {"cursor":2}`}, stateCursors(out))
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, "failing destination: stream users failed at record 4 after 3 records", traces[0].Message)
	assert.Equal(t, synclet.MessageTypeTrace, out.Messages[len(out.Messages)-1].Type)

	zero := runWrite(t, `{"test_destination":{"test_destination_type":"FAILING","num_messages":0}}`, state("users", 0), rec("users", 0))
	require.Error(t, zero.Err)
	assert.Equal(t, []string{`users {"cursor":0}`}, stateCursors(zero))
}

func TestWriteInvalidConfig(t *testing.T) {
	out := runWrite(t, `{"test_destination":{"test_destination_type":"THROTTLED"}}`, rec("users", 1), state("users", 1))
	require.Error(t, out.Err)
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
	assert.Empty(t, out.States())
}

func TestWriteBadInput(t *testing.T) {
	out := runWrite(t, `{}`, state("users", 1), "not json")
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "read input")
	assert.Equal(t, []string{`users {"cursor":1}`}, stateCursors(out))
}
