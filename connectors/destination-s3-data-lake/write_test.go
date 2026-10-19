package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return b
}

func runTestdata(t *testing.T, cfg Config, catalogFile, messagesFile string) *testkit.Output {
	t.Helper()
	return runRaw(t, NewDestination(), cfg, readTestdata(t, catalogFile), bytes.NewReader(readTestdata(t, messagesFile)))
}

func TestWriteAllTypes(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	out := runTestdata(t, cfg, "catalog.json", "messages.jsonl")
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"cursor":3}`}, stateValues(out))
	assert.Equal(t, fixtureTypes, tableTypes(t, cfg, "all_types"))

	rows := tableRows(t, cfg, "all_types", "id", "bool_col", "int_col", "airbyte_int_col", "num_col", "big_int_col",
		"big_int_num_col", "str_col", "date_col", "ts_col", "ts_ntz_col", "time_col", "time_plain_col", "timetz_col",
		"obj_col", "arr_col", "union_col", "any_of_col", "untyped_col", "_airbyte_extracted_at")
	require.Len(t, rows, 3)
	assert.Equal(t, []string{
		"1", "true", "-9223372036854775808", "42", "12345678901234567890.123456789", "12345678901234567890123456789012345678",
		"-99999999999999999999999", "tab\there, \"quoted\"\nnewline \\ backslash", "2024-02-29", "2024-02-29T11:14:15.123456Z",
		"2024-02-29T13:14:15", "13:14:15.500000", "23:59:59.999999", "13:14:15+02:00",
		`{"a":1,"nested":{"n":99999999999999999999.5}}`, `[1.10,2]`, `"x"`, `3`, `{"k":"v"}`, "2023-11-14T22:13:20Z",
	}, rows[0])
	assert.Equal(t, []string{
		"2", "<NULL>", "0", "0", "0.000000001", "-1", "0", "", "0001-01-01", "1970-01-01T00:00:00Z",
		"1970-01-01T00:00:00", "00:00:00.000000", "00:00:00.000000", "00:00:00Z", `{}`, `[]`, `7`, `"s"`, `"s"`, "2023-11-14T22:13:20Z",
	}, rows[1])
	assert.Equal(t, "3", rows[2][0])
	for _, v := range rows[2][1:19] {
		assert.Equal(t, "<NULL>", v, "a missing field is null")
	}

	raw := tableRows(t, cfg, "all_types", "_airbyte_raw_id")
	require.Len(t, raw, 3)
	assert.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-`, raw[0][0])
}

func TestWriteValueErrorNamesRecord(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	streams := []synclet.ConfiguredStream{stream("rates", `{"properties":{"code":{"type":"string"},"rate":{"type":"number"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	out := runWrite(t, NewDestination(), cfg, streams,
		rec("rates", `{"code":"EUR","rate":0.5}`),
		rec("rates", `{"code":"UAH","rate":0.1234567891}`),
		state("rates", `{}`))
	require.Error(t, out.Err)
	assert.Equal(t, `stream "rates": record 2: column "rate": value 0.1234567891 has more than 9 decimal places`, out.Err.Error())
	assert.Empty(t, out.States())
	assert.Empty(t, tableRows(t, cfg, "rates", "code"), "nothing of the failed batch is committed")
	assert.Empty(t, dataObjects(t, e, cfg, "rates"))
}

func TestEvolutionAddsColumn(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	v1 := []synclet.ConfiguredStream{stream("users", `{"properties":{"id":{"type":"integer"},"gone":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	out := runWrite(t, NewDestination(), cfg, v1, rec("users", `{"id":1,"gone":"a"}`), state("users", `{"n":1}`))
	require.NoError(t, out.Err)

	v2 := []synclet.ConfiguredStream{stream("users", `{"properties":{"id":{"type":"integer"},"email":{"type":"string"},"born":{"type":"string","format":"date"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	out = runWrite(t, NewDestination(), cfg, v2, rec("users", `{"id":2,"email":"b@x","born":"2000-01-02","gone":"ignored"}`), state("users", `{"n":2}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"n":2}`}, stateValues(out))

	types := tableTypes(t, cfg, "users")
	assert.Equal(t, "string", types["email"])
	assert.Equal(t, "date", types["born"])
	assert.Equal(t, "string", types["gone"], "a dropped field keeps its column")
	assert.Equal(t, [][]string{
		{"1", "<NULL>", "<NULL>", "a"},
		{"2", "b@x", "2000-01-02", "<NULL>"},
	}, tableRows(t, cfg, "users", "id", "email", "born", "gone"))
	assert.Equal(t, []string{"append", "append"}, snapshots(t, cfg, "users"))
}

func TestEvolutionTypeChangeFails(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	v1 := []synclet.ConfiguredStream{stream("users", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	require.NoError(t, runWrite(t, NewDestination(), cfg, v1, rec("users", `{"id":1}`)).Err)
	before := loadTestTable(t, cfg, "users").MetadataLocation()

	v2 := []synclet.ConfiguredStream{stream("users", `{"properties":{"id":{"type":"string"},"extra":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	out := runWrite(t, NewDestination(), cfg, v2, rec("users", `{"id":"x"}`), state("users", `{}`))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `column "id" is long in the table but string in the catalog`)
	assert.Empty(t, out.States())
	assert.Equal(t, before, loadTestTable(t, cfg, "users").MetadataLocation(), "the table is unchanged")
	assert.NotContains(t, tableTypes(t, cfg, "users"), "extra")
}

func TestAppendSnapshots(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	d := NewDestination()
	d.maxRows = 2
	streams := []synclet.ConfiguredStream{stream("events", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	lines := []string{}
	for i := range 5 {
		lines = append(lines, rec("events", `{"id":`+string(rune('1'+i))+`}`))
	}
	out := runWrite(t, d, cfg, streams, append(lines, state("events", `{"id":5}`))...)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"id":5}`}, stateValues(out))
	assert.Equal(t, []string{"append", "append", "append"}, snapshots(t, cfg, "events"), "one snapshot per flush: 2+2+1 records")
	assert.Equal(t, [][]string{{"1"}, {"2"}, {"3"}, {"4"}, {"5"}}, tableRows(t, cfg, "events", "id"))

	out = runWrite(t, d, cfg, streams, rec("events", `{"id":6}`))
	require.NoError(t, out.Err)
	assert.Len(t, tableRows(t, cfg, "events", "id"), 6, "append keeps earlier rows")
}

func TestOverwriteReplacesOnSuccess(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	d := NewDestination()
	d.maxRows = 1
	streams := []synclet.ConfiguredStream{stream("rates", `{"properties":{"code":{"type":"string"}}}`, synclet.DestinationSyncModeOverwrite, nil, nil)}
	require.NoError(t, runWrite(t, d, cfg, streams, rec("rates", `{"code":"OLD1"}`), rec("rates", `{"code":"OLD2"}`)).Err)
	require.Len(t, snapshots(t, cfg, "rates"), 1)

	out := runWrite(t, d, cfg, streams,
		rec("rates", `{"code":"EUR"}`), state("rates", `{"s":1}`),
		rec("rates", `{"code":"UAH"}`), state("rates", `{"s":2}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"s":1}`, `{"s":2}`}, stateValues(out), "held until the overwrite commits, then echoed in order")
	ops := snapshots(t, cfg, "rates")
	assert.Equal(t, []string{"append", "overwrite"}, ops, "one replacing snapshot per sync, whatever the flushes")
	assert.Equal(t, [][]string{{"EUR"}, {"UAH"}}, tableRows(t, cfg, "rates", "code"))

	out = runWrite(t, d, cfg, streams, state("rates", `{"s":3}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"s":3}`}, stateValues(out))
	assert.Empty(t, tableRows(t, cfg, "rates", "code"), "an empty overwrite empties the table")
	assert.Equal(t, "delete", snapshots(t, cfg, "rates")[2])
}

func TestOverwriteFailureKeepsSnapshot(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	d := NewDestination()
	d.maxRows = 1
	streams := []synclet.ConfiguredStream{stream("rates", `{"properties":{"code":{"type":"string"},"rate":{"type":"integer"}}}`, synclet.DestinationSyncModeOverwrite, nil, nil)}
	require.NoError(t, runWrite(t, d, cfg, streams, rec("rates", `{"code":"OLD","rate":1}`)).Err)
	before := loadTestTable(t, cfg, "rates").CurrentSnapshot().SnapshotID

	out := runWrite(t, d, cfg, streams,
		rec("rates", `{"code":"EUR","rate":2}`), state("rates", `{"s":1}`),
		rec("rates", `{"code":"UAH","rate":3}`),
		rec("rates", `{"code":"BAD","rate":"x"}`))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream "rates": record 3: column "rate"`)
	assert.Empty(t, out.States(), "no state of a failed overwrite")
	assert.Equal(t, before, loadTestTable(t, cfg, "rates").CurrentSnapshot().SnapshotID)
	assert.Equal(t, [][]string{{"OLD"}}, tableRows(t, cfg, "rates", "code"))
	assert.Equal(t, liveFiles(t, cfg, "rates"), dataObjects(t, e, cfg, "rates"), "the files written before the failure are removed")
}

func TestStateEchoAfterCommit(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	d := NewDestination()
	d.maxAge = 0
	streams := []synclet.ConfiguredStream{stream("events", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	out := runWrite(t, d, cfg, streams,
		rec("events", `{"id":1}`), state("events", `{"id":1}`),
		rec("events", `{"id":2}`), state("events", `{"id":2}`),
		rec("events", `{"id":3}`),
		rec("events", `{"id":"bad"}`), state("events", `{"id":4}`))
	require.Error(t, out.Err)
	assert.Equal(t, []string{`{"id":1}`, `{"id":2}`}, stateValues(out))
	assert.Equal(t, [][]string{{"1"}, {"2"}}, tableRows(t, cfg, "events", "id"), "the rows of every echoed state are committed")
	assert.Len(t, snapshots(t, cfg, "events"), 2)
}

func TestStateEchoWaitsForBatch(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	d := NewDestination()
	d.maxAge = time.Hour
	streams := []synclet.ConfiguredStream{stream("events", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	out := runWrite(t, d, cfg, streams,
		state("events", `{"id":0}`),
		rec("events", `{"id":1}`), state("events", `{"id":1}`),
		rec("events", `{"id":2}`), state("events", `{"id":2}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"id":0}`, `{"id":1}`, `{"id":2}`}, stateValues(out))
	assert.Len(t, snapshots(t, cfg, "events"), 1, "states inside the age limit share one commit")
}

func TestCommitConflictRetries(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	proxy := newCatalogProxy(t, e.uri)
	cfg.Catalog.URI = proxy.URL
	streams := []synclet.ConfiguredStream{stream("events", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	proxy.conflicts.Store(1)
	out := runWrite(t, NewDestination(), cfg, streams, rec("events", `{"id":1}`), state("events", `{"id":1}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"id":1}`}, stateValues(out))
	assert.Equal(t, int64(2), proxy.commits.Load(), "one conflict, one retry")
	assert.Equal(t, [][]string{{"1"}}, tableRows(t, cfg, "events", "id"))
}

func TestCommitConflictFailsLoudly(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	proxy := newCatalogProxy(t, e.uri)
	cfg.Catalog.URI = proxy.URL
	streams := []synclet.ConfiguredStream{stream("events", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	proxy.conflicts.Store(-1)
	out := runWrite(t, NewDestination(), cfg, streams, rec("events", `{"id":1}`), state("events", `{"id":1}`))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream "events": commit to `+cfg.Namespace+`.events failed after 5 conflicts`)
	assert.Empty(t, out.States())
	assert.Equal(t, int64(maxCommitAttempts), proxy.commits.Load())
	assert.Empty(t, tableRows(t, cfg, "events", "id"))
	assert.Empty(t, dataObjects(t, e, cfg, "events"), "the files of a failed commit are removed")
}

func TestAppendDedupTwoSyncs(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	streams := []synclet.ConfiguredStream{stream("users",
		`{"properties":{"id":{"type":"integer"},"name":{"type":"string"},"v":{"type":"integer"}}}`,
		synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"v"})}
	out := runWrite(t, NewDestination(), cfg, streams,
		rec("users", `{"id":1,"name":"Ann","v":1}`),
		rec("users", `{"id":2,"name":"Bob","v":1}`),
		rec("users", `{"id":1,"name":"Ann B","v":3}`),
		rec("users", `{"id":1,"name":"stale","v":2}`),
		state("users", `{"v":3}`))
	require.NoError(t, out.Err)
	assert.Equal(t, [][]string{{"1", "Ann B"}, {"2", "Bob"}}, tableRows(t, cfg, "users", "id", "name"), "highest cursor wins in a batch")

	out = runWrite(t, NewDestination(), cfg, streams,
		rec("users", `{"id":2,"name":"Bob C","v":4}`),
		rec("users", `{"id":3,"name":"Cy","v":4}`),
		state("users", `{"v":4}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"v":4}`}, stateValues(out))
	assert.Equal(t, [][]string{{"1", "Ann B"}, {"2", "Bob C"}, {"3", "Cy"}}, tableRows(t, cfg, "users", "id", "name"))
	assert.Equal(t, []string{"overwrite", "overwrite"}, snapshots(t, cfg, "users"), "one row delta per flush")

	out = runWrite(t, NewDestination(), cfg, streams, rec("users", `{"id":null,"name":"x"}`))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `primary key column "id" is null`)
}

func TestNamespaceMissingFailsWrite(t *testing.T) {
	e := startLake(t)
	cfg := e.config(t)
	cfg.Namespace = "missing_ns"
	streams := []synclet.ConfiguredStream{stream("events", `{"properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)}
	out := runWrite(t, NewDestination(), cfg, streams, rec("events", `{"id":1}`))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `namespace "missing_ns" does not exist`)
}
