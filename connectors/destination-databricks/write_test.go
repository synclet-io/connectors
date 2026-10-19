package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const eventsSchema = `{"type":"object","properties":{"id":{"type":"integer"},"payload":{"type":"object"},"amount":{"type":"number"}}}`

var stagedFile = regexp.MustCompile(`FROM '(/Volumes/main/raw/stg/_synclet/[0-9a-f-]{36}/\d+-\d+\.json)'\)`)

// stagedLines returns the rows of the file a COPY INTO statement read.
func stagedLines(t *testing.T, body []byte) []map[string]*string {
	t.Helper()
	var out []map[string]*string
	sc := bufio.NewScanner(bytes.NewReader(body))
	for sc.Scan() {
		var row map[string]*string
		require.NoError(t, json.Unmarshal(sc.Bytes(), &row), sc.Text())
		out = append(out, row)
	}
	return out
}

// capture keeps the staged files of every COPY INTO before the connector
// deletes them.
func capture(ws *fakeWorkspace, sql *fakeSQL) map[string][]byte {
	files := map[string][]byte{}
	sql.fail = func(stmt string) error {
		if m := stagedFile.FindStringSubmatch(stmt); m != nil {
			files[m[1]] = ws.file(m[1])
		}
		return nil
	}
	return files
}

func str(s string) *string { return &s }

// TestWriteAppendAddsRows stages each batch as one file, copies it into the
// stage table and appends that to the table, then cleans up.
func TestWriteAppendAddsRows(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	files := capture(ws, sql)
	d := ws.destination(sql)
	d.maxAge = 0
	states, err := runWrite(t, d, ws.config(t), sql,
		catalogJSON(streamJSON("events", "append", eventsSchema, "", "")),
		record("events", `{"id":1,"payload":{"a":[1,"x"]},"amount":0.5}`)+
			record("events", `{"id":2,"amount":null,"extra":true}`)+
			state("events", `{"id":2}`)+
			record("events", `{"id":3,"amount":"12345678901234567890123456789.123456789"}`)+
			state("events", `{"id":3}`))
	require.NoError(t, err)
	assert.Equal(t, []string{`events {"id":2}`, `events {"id":3}`}, states)

	assert.Equal(t, []string{"CREATE TABLE IF NOT EXISTS `main`.`raw`.`events` (`_airbyte_raw_id` STRING, `_airbyte_extracted_at` TIMESTAMP, `amount` DECIMAL(38,9), `id` BIGINT, `payload` VARIANT)" + tableProps}, sql.matching("CREATE TABLE"))
	inserts := sql.matching("INSERT INTO `main`.`raw`.`events`")
	assert.Len(t, inserts, 2, "one append per batch")
	assert.Len(t, sql.matching("COPY INTO `main`.`raw`.`_synclet_stage_events`"), 2)
	assert.Equal(t, []string{"DROP TABLE IF EXISTS `main`.`raw`.`_synclet_stage_events`"}, sql.matching("DROP TABLE"))

	require.Len(t, files, 2)
	var rows []map[string]*string
	for _, stmt := range sql.matching("COPY INTO") {
		rows = append(rows, stagedLines(t, files[stagedFile.FindStringSubmatch(stmt)[1]])...)
	}
	require.Len(t, rows, 3)
	for i, r := range rows {
		assert.Regexp(t, `^[0-9a-f-]{36}$`, *r["c0"])
		assert.Equal(t, "2024-01-02T03:04:05.123000Z", *r["c1"])
		assert.Equal(t, []string{"1", "2", "3"}[i], *r["s"])
	}
	assert.Equal(t, map[string]*string{"s": rows[0]["s"], "c0": rows[0]["c0"], "c1": rows[0]["c1"], "c2": str("0.500000000"), "c3": str("1"), "c4": str(`{"a":[1,"x"]}`)}, rows[0])
	assert.Nil(t, rows[1]["c2"])
	assert.Nil(t, rows[1]["c4"])
	assert.Equal(t, "12345678901234567890123456789.123456789", *rows[2]["c2"])

	assert.Zero(t, ws.fileCount(), "staged files are deleted")
	assert.Contains(t, ws.opsList(), "DELETE "+strings.TrimSuffix(strings.Split(stagedFile.FindStringSubmatch(sql.matching("COPY INTO")[0])[1], "/0-")[0], "/"))
	assert.True(t, sql.closed)
}

// TestWriteAppendDedupMerges merges each batch on the primary key.
func TestWriteAppendDedupMerges(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	states, err := runWrite(t, ws.destination(sql), ws.config(t), sql,
		catalogJSON(streamJSON("users", "append_dedup", usersSchema, "id", "updated_at")),
		record("users", `{"id":1,"name":"Ann","updated_at":"2024-01-01T00:00:00Z"}`)+
			record("users", `{"id":1,"name":"Ann B","updated_at":"2024-02-01T00:00:00Z"}`)+
			state("users", `{"updated_at":"2024-02-01T00:00:00Z"}`))
	require.NoError(t, err)
	assert.Equal(t, []string{`users {"updated_at":"2024-02-01T00:00:00Z"}`}, states)
	p := planOf(t, "append_dedup", usersSchema, [][]string{{"id"}}, []string{"updated_at"})
	assert.Equal(t, []string{mergeSQL(testLoc, p)}, sql.matching("MERGE INTO"))
	assert.Empty(t, sql.matching("INSERT INTO"))
}

func TestWriteRejectsNullPrimaryKey(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	states, err := runWrite(t, ws.destination(sql), ws.config(t), sql,
		catalogJSON(streamJSON("users", "append_dedup", usersSchema, "id", "updated_at")),
		record("users", `{"name":"no key"}`)+state("users", `{}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `stream "users": record 1: primary key column "id" is null`)
	assert.Empty(t, states)
}

const ratesSchema = `{"type":"object","properties":{"code":{"type":"string"},"rate":{"type":"number"}}}`

// TestWriteOverwriteReplacesOnSuccess writes into the shadow table and
// replaces the table only after the last batch, then echoes the held states.
func TestWriteOverwriteReplacesOnSuccess(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	d := ws.destination(sql)
	d.maxAge = 0
	states, err := runWrite(t, d, ws.config(t), sql,
		catalogJSON(streamJSON("rates", "overwrite", ratesSchema, "", "")),
		record("rates", `{"code":"EUR","rate":0.912345678}`)+state("rates", `{"a":1}`)+
			record("rates", `{"code":"UAH","rate":41.25}`)+state("rates", `{"a":2}`))
	require.NoError(t, err)
	assert.Equal(t, []string{`rates {"a":1}`, `rates {"a":2}`}, states)

	p := planOf(t, "overwrite", ratesSchema, nil, nil)
	p.table, p.shadow = "rates", "_synclet_tmp_rates"
	ev := sql.events()
	swap := indexOf(ev, swapSQL(testLoc, p))
	require.GreaterOrEqual(t, swap, 0, "the table is replaced")
	assert.Len(t, sql.matching("COPY INTO `main`.`raw`.`_synclet_tmp_rates`"), 2)
	assert.Equal(t, "CREATE OR REPLACE TABLE `main`.`raw`.`_synclet_tmp_rates` (`_airbyte_raw_id` STRING, `_airbyte_extracted_at` TIMESTAMP, `code` STRING, `rate` DECIMAL(38,9))"+tableProps, ev[0])
	assert.Equal(t, []string{"DROP TABLE IF EXISTS `main`.`raw`.`_synclet_tmp_rates`"}, sql.matching("DROP TABLE"))
	assert.Greater(t, indexOf(ev, `STATE rates {"a":1}`), swap, "states wait for the swap")
	assert.Empty(t, sql.matching("INSERT INTO"))
}

// TestWriteOverwriteKeepsTableOnFailure never replaces the table of a sync
// that fails, drops the shadow table and echoes no state.
func TestWriteOverwriteKeepsTableOnFailure(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	d := ws.destination(sql)
	d.maxAge = 0
	states, err := runWrite(t, d, ws.config(t), sql,
		catalogJSON(streamJSON("rates", "overwrite", ratesSchema, "", "")),
		record("rates", `{"code":"EUR","rate":0.9}`)+state("rates", `{"a":1}`)+
			record("rates", `{"code":"UAH","rate":"not a number"}`)+state("rates", `{"a":2}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `stream "rates": record 2: column "rate": value "not a number" is not a number`)
	assert.Empty(t, states)
	assert.Empty(t, sql.matching("CREATE OR REPLACE TABLE `main`.`raw`.`rates`"), "the table is not replaced")
	assert.Equal(t, []string{"DROP TABLE IF EXISTS `main`.`raw`.`_synclet_tmp_rates`"}, sql.matching("DROP TABLE"))
	assert.Zero(t, ws.fileCount())
}

// TestWriteEchoesStateAfterCommit echoes a STATE only once the batch before
// it is in the table; a failure mid-batch echoes nothing for that batch.
func TestWriteEchoesStateAfterCommit(t *testing.T) {
	failures := map[string]func(stmt string) bool{
		"copy fails": func(stmt string) bool {
			return strings.HasPrefix(stmt, "COPY INTO `main`.`raw`.`_synclet_stage_b`")
		},
		"insert into the second table fails after the first commits": func(stmt string) bool {
			return strings.HasPrefix(stmt, "INSERT INTO `main`.`raw`.`b`")
		},
	}
	for name, failing := range failures {
		t.Run(name, func(t *testing.T) {
			ws := newWorkspace(t)
			sql := newFakeSQL(ws)
			batch := 0
			sql.fail = func(stmt string) error {
				if strings.HasPrefix(stmt, "COPY INTO `main`.`raw`.`_synclet_stage_a`") {
					batch++
				}
				if batch == 2 && failing(stmt) {
					return errors.New("warehouse failure")
				}
				return nil
			}
			d := ws.destination(sql)
			d.maxAge = 0
			states, err := runWrite(t, d, ws.config(t), sql,
				catalogJSON(streamJSON("a", "append", eventsSchema, "", ""), streamJSON("b", "append", eventsSchema, "", "")),
				record("a", `{"id":1}`)+record("b", `{"id":1}`)+state("a", `{"n":1}`)+
					record("a", `{"id":2}`)+record("b", `{"id":2}`)+state("a", `{"n":2}`))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "warehouse failure")
			assert.Equal(t, []string{`a {"n":1}`}, states)

			ev := sql.events()
			first := indexOf(ev, `STATE a {"n":1}`)
			assert.Greater(t, first, indexOf(ev, "INSERT INTO `main`.`raw`.`b` (`_airbyte_raw_id`, `_airbyte_extracted_at`, `amount`, `id`, `payload`) SELECT `_airbyte_raw_id`, `_airbyte_extracted_at`, `amount`, `id`, `payload` FROM `main`.`raw`.`_synclet_stage_b`"))
			assert.ElementsMatch(t, []string{
				"DROP TABLE IF EXISTS `main`.`raw`.`_synclet_stage_a`",
				"DROP TABLE IF EXISTS `main`.`raw`.`_synclet_stage_b`",
			}, sql.matching("DROP TABLE"))
			assert.Zero(t, ws.fileCount(), "staged files are deleted")
		})
	}
}

// TestWriteFailsOnRowCountMismatch fails a batch COPY INTO did not load in
// full instead of dropping rows.
func TestWriteFailsOnRowCountMismatch(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	sql.copied = func(_ string, lines int) int { return lines - 1 }
	states, err := runWrite(t, ws.destination(sql), ws.config(t), sql,
		catalogJSON(streamJSON("events", "append", eventsSchema, "", "")),
		record("events", `{"id":1}`)+record("events", `{"id":2}`)+state("events", `{"id":2}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `stream "events": COPY INTO loaded 1 of 2 rows`)
	assert.Empty(t, states)
	assert.Empty(t, sql.matching("INSERT INTO"))
}

func TestWriteStateWithoutPendingRecordsIsEchoedAtOnce(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	states, err := runWrite(t, ws.destination(sql), ws.config(t), sql,
		catalogJSON(streamJSON("events", "append", eventsSchema, "", "")),
		state("events", `{"id":0}`))
	require.NoError(t, err)
	assert.Equal(t, []string{`events {"id":0}`}, states)
	assert.Empty(t, sql.matching("COPY INTO"))
}

// TestWriteEvolvesExistingTable adds the columns a table lacks and fails on
// one whose type differs from the catalog's.
func TestWriteEvolvesExistingTable(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	sql.columns["events"] = []map[string]string{
		{"column_name": "_airbyte_raw_id", "full_data_type": "string"},
		{"column_name": "_airbyte_extracted_at", "full_data_type": "timestamp"},
		{"column_name": "ID", "full_data_type": "bigint"},
	}
	_, err := runWrite(t, ws.destination(sql), ws.config(t), sql,
		catalogJSON(streamJSON("events", "append", eventsSchema, "", "")), record("events", `{"id":1}`))
	require.NoError(t, err)
	assert.Empty(t, sql.matching("CREATE TABLE IF NOT EXISTS"))
	assert.Equal(t, []string{"ALTER TABLE `main`.`raw`.`events` ADD COLUMNS (`amount` DECIMAL(38,9), `payload` VARIANT)"}, sql.matching("ALTER TABLE"))

	sql = newFakeSQL(ws)
	sql.columns["events"] = []map[string]string{{"column_name": "amount", "full_data_type": "double"}}
	_, err = runWrite(t, ws.destination(sql), ws.config(t), sql,
		catalogJSON(streamJSON("events", "append", eventsSchema, "", "")), record("events", `{"id":1}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "column \"amount\" of table `main`.`raw`.`events` is DOUBLE, the catalog needs DECIMAL(38,9)")
	assert.Empty(t, sql.matching("COPY INTO"))
}

func TestWriteRejectsRecordOfUnknownStream(t *testing.T) {
	ws := newWorkspace(t)
	sql := newFakeSQL(ws)
	_, err := runWrite(t, ws.destination(sql), ws.config(t), sql,
		catalogJSON(streamJSON("events", "append", eventsSchema, "", "")), record("nope", `{"id":1}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `record of stream "nope"`)
}

func indexOf(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
}
