package main

import (
	"encoding/json"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const (
	db       = `"SYNCLET_DB"`
	sch      = `"SYNCLET_DB"."RAW"`
	csvOpts  = `FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = TRUE NULL_IF = () ESCAPE_UNENCLOSED_FIELD = NONE COMPRESSION = GZIP ENCODING = 'UTF8') ON_ERROR = ABORT_STATEMENT PURGE = TRUE`
	putOpts  = `SOURCE_COMPRESSION = GZIP AUTO_COMPRESS = FALSE`
	tsTZ     = `'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'`
	eventsDD = `("_AIRBYTE_RAW_ID" VARCHAR, "_AIRBYTE_EXTRACTED_AT" TIMESTAMP_TZ, "ID" NUMBER(38,0), "KIND" VARCHAR)`
	usersDD  = `("_AIRBYTE_RAW_ID" VARCHAR, "_AIRBYTE_EXTRACTED_AT" TIMESTAMP_TZ, "ID" NUMBER(38,0), "NAME" VARCHAR, "UPDATED_AT" TIMESTAMP_TZ, PRIMARY KEY ("ID"))`
	ratesDD  = `("_AIRBYTE_RAW_ID" VARCHAR, "_AIRBYTE_EXTRACTED_AT" TIMESTAMP_TZ, "CODE" VARCHAR, "RATE" FLOAT)`
	meta     = `"_AIRBYTE_RAW_ID", "_AIRBYTE_EXTRACTED_AT"`
	metaCast = `"_AIRBYTE_RAW_ID", TO_TIMESTAMP_TZ("_AIRBYTE_EXTRACTED_AT", ` + tsTZ + `)`
)

func columnsSQL(table string) string {
	return `SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM "SYNCLET_DB".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = '` + table + `' ORDER BY ORDINAL_POSITION`
}

// tableColumns answers the columns query of table with rows of name,
// data type, precision and scale.
func tableColumns(table string, cols ...[4]string) fakeQuery {
	q := fakeQuery{sql: columnsSQL(table), columns: []string{"COLUMN_NAME", "DATA_TYPE", "NUMERIC_PRECISION", "NUMERIC_SCALE"}}
	for _, c := range cols {
		r := make([]*string, 4)
		for i, v := range c {
			if v != "" {
				r[i] = str(v)
			}
		}
		q.rows = append(q.rows, r)
	}
	return q
}

var (
	tempFile = regexp.MustCompile(`'file://[^']+'`)
	fileName = regexp.MustCompile(`synclet_[0-9a-f]{32}\.csv\.gz`)
	runStage = regexp.MustCompile(`@~/synclet/[0-9a-f]{32}/`)
	rawID    = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// normalized replaces the random parts of the statements: temp file paths
// and staged file names.
func normalized(sqls []string) []string {
	out := make([]string, len(sqls))
	for i, s := range sqls {
		s = tempFile.ReplaceAllString(s, `'file://<tmp>'`)
		s = runStage.ReplaceAllString(s, `@~/synclet/<run>/`)
		out[i] = fileName.ReplaceAllString(s, `<file>`)
	}
	return out
}

func runWrite(t *testing.T, d Destination, catalog, messages string) *testkit.Output {
	t.Helper()
	cfg, err := json.Marshal(testConfig(t))
	require.NoError(t, err)
	cat, err := os.ReadFile("testdata/" + catalog)
	require.NoError(t, err)
	out, err := testkit.RunDestination(t.Context(), d, synclet.Request{
		Command: synclet.CommandWrite, Config: cfg, Catalog: cat, Input: strings.NewReader(messages),
	})
	require.NoError(t, err)
	return out
}

func readTestdata(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	require.NoError(t, err)
	return string(b)
}

func stateNames(out *testkit.Output) []string {
	var got []string
	for _, st := range out.States() {
		got = append(got, st.Stream.StreamDescriptor.Name+" "+string(st.Stream.StreamState))
	}
	return got
}

func protocolFake(t *testing.T) *fakeSnowflake {
	return newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS"), tableColumns("USERS"), tableColumns("RATES"))
}

// TestWriteSQL asserts every statement of a sync of one stream per mode:
// overwrite loads into a shadow table swapped in at the end, append
// inserts, append_dedup merges keeping the highest cursor.
func TestWriteSQL(t *testing.T) {
	f := protocolFake(t)
	out := runWrite(t, f.destination(), "protocol_catalog.json", readTestdata(t, "protocol_messages.jsonl"))
	require.NoError(t, out.Err)

	stage := func(n string) string { return sch + `."_SYNCLET_STAGE_` + n + `"` }
	want := []string{
		sessionSQL,
		schemaSQL,
		columnsSQL("EVENTS"),
		`CREATE TABLE IF NOT EXISTS ` + sch + `."EVENTS" ` + eventsDD,
		`CREATE OR REPLACE TEMPORARY TABLE ` + stage("0") + ` ("_SYNCLET_SEQ" NUMBER(38,0), "_AIRBYTE_RAW_ID" VARCHAR, "_AIRBYTE_EXTRACTED_AT" VARCHAR, "ID" VARCHAR, "KIND" VARCHAR)`,
		columnsSQL("USERS"),
		`CREATE TABLE IF NOT EXISTS ` + sch + `."USERS" ` + usersDD,
		`CREATE OR REPLACE TEMPORARY TABLE ` + stage("1") + ` ("_SYNCLET_SEQ" NUMBER(38,0), "_AIRBYTE_RAW_ID" VARCHAR, "_AIRBYTE_EXTRACTED_AT" VARCHAR, "ID" VARCHAR, "NAME" VARCHAR, "UPDATED_AT" VARCHAR)`,
		columnsSQL("RATES"),
		`CREATE OR REPLACE TABLE ` + sch + `."_SYNCLET_TMP_RATES" ` + ratesDD,
		`CREATE OR REPLACE TEMPORARY TABLE ` + stage("2") + ` ("_SYNCLET_SEQ" NUMBER(38,0), "_AIRBYTE_RAW_ID" VARCHAR, "_AIRBYTE_EXTRACTED_AT" VARCHAR, "CODE" VARCHAR, "RATE" VARCHAR)`,

		`PUT 'file://<tmp>' @~/synclet/<run>/ ` + putOpts,
		`COPY INTO ` + stage("0") + ` FROM @~/synclet/<run>/ FILES = ('<file>') ` + csvOpts,
		`PUT 'file://<tmp>' @~/synclet/<run>/ ` + putOpts,
		`COPY INTO ` + stage("1") + ` FROM @~/synclet/<run>/ FILES = ('<file>') ` + csvOpts,
		`PUT 'file://<tmp>' @~/synclet/<run>/ ` + putOpts,
		`COPY INTO ` + stage("2") + ` FROM @~/synclet/<run>/ FILES = ('<file>') ` + csvOpts,
		`BEGIN`,
		`INSERT INTO ` + sch + `."EVENTS" (` + meta + `, "ID", "KIND") SELECT ` + metaCast + `, TO_NUMBER("ID", 38, 0), "KIND" FROM ` + stage("0") + ` ORDER BY "_SYNCLET_SEQ"`,
		`DELETE FROM ` + stage("0"),
		`MERGE INTO ` + sch + `."USERS" AS t USING (SELECT * FROM (SELECT "_SYNCLET_SEQ", ` +
			metaCast + ` AS "_AIRBYTE_EXTRACTED_AT", TO_NUMBER("ID", 38, 0) AS "ID", "NAME", TO_TIMESTAMP_TZ("UPDATED_AT", ` + tsTZ + `) AS "UPDATED_AT" FROM ` + stage("1") + `)` +
			` QUALIFY ROW_NUMBER() OVER (PARTITION BY "ID" ORDER BY "UPDATED_AT" DESC NULLS LAST, "_SYNCLET_SEQ" DESC) = 1) AS s` +
			` ON t."ID" = s."ID"` +
			` WHEN MATCHED AND (t."UPDATED_AT" IS NULL OR s."UPDATED_AT" >= t."UPDATED_AT") THEN UPDATE SET t."_AIRBYTE_RAW_ID" = s."_AIRBYTE_RAW_ID", t."_AIRBYTE_EXTRACTED_AT" = s."_AIRBYTE_EXTRACTED_AT", t."NAME" = s."NAME", t."UPDATED_AT" = s."UPDATED_AT"` +
			` WHEN NOT MATCHED THEN INSERT (` + meta + `, "ID", "NAME", "UPDATED_AT") VALUES (s."_AIRBYTE_RAW_ID", s."_AIRBYTE_EXTRACTED_AT", s."ID", s."NAME", s."UPDATED_AT")`,
		`DELETE FROM ` + stage("1"),
		`INSERT INTO ` + sch + `."_SYNCLET_TMP_RATES" (` + meta + `, "CODE", "RATE") SELECT ` + metaCast + `, "CODE", TO_DOUBLE("RATE") FROM ` + stage("2") + ` ORDER BY "_SYNCLET_SEQ"`,
		`DELETE FROM ` + stage("2"),
		`COMMIT`,
		`CREATE TABLE IF NOT EXISTS ` + sch + `."RATES" ` + ratesDD,
		`ALTER TABLE ` + sch + `."_SYNCLET_TMP_RATES" SWAP WITH ` + sch + `."RATES"`,
		`DROP TABLE IF EXISTS ` + sch + `."_SYNCLET_TMP_RATES"`,
		`REMOVE @~/synclet/<run>/`,
	}
	assert.Equal(t, want, normalized(f.executed()))

	assert.Equal(t, []string{`events {"id":2}`, `users {"updated_at":"2024-02-01T00:00:00Z"}`, `rates {}`}, stateNames(out))
	assert.Len(t, out.Logs(), 1)
	assert.Contains(t, out.Logs()[0].Message, `stream "rates": fields not in the catalog schema are not written: extra`)
}

// TestWriteStagedValues checks the CSV rows COPY INTO loads: sequence, a
// fresh raw id, the extraction time and each value in its canonical text,
// NULL as an empty unquoted field and "" as the empty string.
func TestWriteStagedValues(t *testing.T) {
	f := protocolFake(t)
	out := runWrite(t, f.destination(), "protocol_catalog.json", readTestdata(t, "protocol_messages.jsonl"))
	require.NoError(t, out.Err)

	stage := func(n string) [][]string {
		rows := f.rowsOf(sch + `."_SYNCLET_STAGE_` + n + `"`)
		for _, r := range rows {
			require.Regexp(t, rawID, r[1])
			r[1] = "<uuid>"
		}
		return rows
	}
	at := "2023-11-14 22:13:20.000000000 +00:00"
	assert.Equal(t, [][]string{
		{"1", "<uuid>", at, "1", "click"},
		{"2", "<uuid>", at, "2", "view, \"quoted\"\nline"},
	}, stage("0"))
	assert.Equal(t, [][]string{
		{"3", "<uuid>", at, "1", "Ann", "2024-01-01 00:00:00.000000000 +00:00"},
		{"4", "<uuid>", at, "1", "Ann B", "2024-02-01 00:00:00.000000000 +00:00"},
		{"5", "<uuid>", at, "2", "", "2024-01-15 10:00:00.000000000 +02:00"},
	}, stage("1"))
	assert.Equal(t, [][]string{
		{"6", "<uuid>", at, "EUR", "0.912345678"},
		{"7", "<uuid>", at, "UAH", "41.25"},
		{"8", "<uuid>", at, "XXX", nullCell},
	}, stage("2"))
}

// TestWriteStateAfterCommit forces a batch per two records and fails the
// second batch's insert: the state after the first batch is echoed, the
// state whose records were in the failed batch is not.
func TestWriteStateAfterCommit(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS")).
		failOn(`INSERT INTO "SYNCLET_DB"."RAW"."EVENTS"`, 2, "100038", "Numeric value 'x' is not recognized")
	d := f.destination()
	d.maxRows = 2
	msgs := `{"type":"RECORD","record":{"stream":"events","data":{"id":1}}}
{"type":"RECORD","record":{"stream":"events","data":{"id":2}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"events"},"stream_state":{"id":2}}}}
{"type":"RECORD","record":{"stream":"events","data":{"id":3}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"events"},"stream_state":{"id":3}}}}
{"type":"RECORD","record":{"stream":"events","data":{"id":4}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"events"},"stream_state":{"id":4}}}}
`
	out := runWrite(t, d, "events_catalog.json", msgs)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream "events": insert batch into "SYNCLET_DB"."RAW"."EVENTS"`)
	assert.Equal(t, []string{`events {"id":2}`}, stateNames(out))

	sqls := f.executed()
	assert.Equal(t, 2, count(sqls, "BEGIN"))
	assert.Equal(t, 1, count(sqls, "COMMIT"))
	assert.Equal(t, 1, count(sqls, "ROLLBACK"))
	assert.Equal(t, "ROLLBACK", sqls[len(sqls)-2])
	assert.Equal(t, `REMOVE @~/synclet/<run>/`, normalized(sqls)[len(sqls)-1])
}

// TestWriteStateWithoutPendingRecords echoes a STATE at once when every
// record before it is committed.
func TestWriteStateWithoutPendingRecords(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS"))
	d := f.destination()
	d.maxRows = 1
	msgs := `{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"events"},"stream_state":{"id":0}}}}
{"type":"RECORD","record":{"stream":"events","data":{"id":1}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"events"},"stream_state":{"id":1}}}}
`
	out := runWrite(t, d, "events_catalog.json", msgs)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`events {"id":0}`, `events {"id":1}`}, stateNames(out))
	assert.Equal(t, 1, count(f.executed(), "COMMIT"))
}

// TestWriteOverwriteFailureKeepsTheOldTable fails the load of an overwrite
// stream: no swap, the shadow table is dropped and its state is not echoed.
func TestWriteOverwriteFailureKeepsTheOldTable(t *testing.T) {
	f := protocolFake(t).failOn(`INSERT INTO "SYNCLET_DB"."RAW"."_SYNCLET_TMP_RATES"`, 0, "100038", "Numeric value is not recognized")
	out := runWrite(t, f.destination(), "protocol_catalog.json", readTestdata(t, "protocol_messages.jsonl"))
	require.Error(t, out.Err)
	sqls := f.executed()
	for _, s := range sqls {
		assert.NotContains(t, s, "SWAP")
	}
	assert.Equal(t, `DROP TABLE IF EXISTS `+sch+`."_SYNCLET_TMP_RATES"`, sqls[len(sqls)-2])
	assert.Equal(t, `REMOVE @~/synclet/<run>/`, normalized(sqls)[len(sqls)-1], "the run's staged files are removed on failure too")
	assert.Empty(t, out.States(), "the batch failed, so neither its states nor the overwrite state are echoed")
}

// TestWriteHeldOverwriteState holds an overwrite stream's state over
// committed batches until the swap.
func TestWriteHeldOverwriteState(t *testing.T) {
	f := protocolFake(t).failOn("SWAP WITH", 0, "002003", "Object does not exist or not authorized.")
	d := f.destination()
	d.maxRows = 1
	out := runWrite(t, d, "protocol_catalog.json", readTestdata(t, "protocol_messages.jsonl"))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `swap "SYNCLET_DB"."RAW"."_SYNCLET_TMP_RATES" with "SYNCLET_DB"."RAW"."RATES"`)
	assert.Equal(t, []string{`events {"id":2}`, `users {"updated_at":"2024-02-01T00:00:00Z"}`}, stateNames(out))
}

// TestWriteFailsOnBadValues fails the sync on a value that would be
// changed, before anything of its batch is loaded.
func TestWriteFailsOnBadValues(t *testing.T) {
	cases := []struct{ data, want string }{
		{`{"id":1.5}`, `stream "events": record 2: column "id": value "1.5" is not an integer`},
		{`{"id":123456789012345678901234567890123456789}`, `stream "events": record 2: column "id": value "123456789012345678901234567890123456789" has more than 38 digits`},
		{`[1]`, `stream "events": record 2: not a JSON object`},
	}
	for _, c := range cases {
		f := newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS"))
		msgs := `{"type":"RECORD","record":{"stream":"events","data":{"id":1}}}
{"type":"RECORD","record":{"stream":"events","data":` + c.data + `}}
`
		out := runWrite(t, f.destination(), "events_catalog.json", msgs)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), c.want)
		for _, s := range f.executed() {
			assert.NotContains(t, s, "PUT ", "nothing of the batch is loaded")
		}
	}
}

func TestWriteFailsOnNullPrimaryKey(t *testing.T) {
	f := protocolFake(t)
	msgs := `{"type":"RECORD","record":{"stream":"users","data":{"name":"x"}}}
`
	out := runWrite(t, f.destination(), "protocol_catalog.json", msgs)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream "users": record 1: primary key column "id" is null`)
}

func TestWriteFailsOnUnknownStream(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS"))
	out := runWrite(t, f.destination(), "events_catalog.json", `{"type":"RECORD","record":{"stream":"nope","data":{}}}
`)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `record of stream "nope" (namespace ""), which is not in the catalog`)
}

// TestWriteCopyCountMismatch fails when COPY INTO loads fewer rows than
// the batch has, rather than drop the rest.
func TestWriteCopyCountMismatch(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS"))
	one := 1
	f.copyLoaded = &one
	msgs := `{"type":"RECORD","record":{"stream":"events","data":{"id":1}}}
{"type":"RECORD","record":{"stream":"events","data":{"id":2}}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"events"},"stream_state":{"id":2}}}}
`
	out := runWrite(t, f.destination(), "events_catalog.json", msgs)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream "events": COPY INTO loaded 1 of 2 rows`)
	assert.Empty(t, out.States())
	assert.Zero(t, count(f.executed(), "BEGIN"))
}

// TestWriteExistingTable adds a missing column and fails on a changed type.
func TestWriteExistingTable(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS",
		[4]string{"_AIRBYTE_RAW_ID", "TEXT", "", ""},
		[4]string{"_AIRBYTE_EXTRACTED_AT", "TIMESTAMP_TZ", "", ""},
		[4]string{"ID", "NUMBER", "38", "0"},
		[4]string{"OLD", "TEXT", "", ""},
	))
	out := runWrite(t, f.destination(), "events_catalog.json", "")
	require.NoError(t, out.Err)
	sqls := f.executed()
	assert.Contains(t, sqls, `ALTER TABLE `+sch+`."EVENTS" ADD COLUMN "KIND" VARCHAR`)
	for _, s := range sqls {
		assert.NotContains(t, s, `CREATE TABLE IF NOT EXISTS `+sch+`."EVENTS"`)
	}

	f = newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS",
		[4]string{"_AIRBYTE_RAW_ID", "TEXT", "", ""},
		[4]string{"_AIRBYTE_EXTRACTED_AT", "TIMESTAMP_TZ", "", ""},
		[4]string{"ID", "NUMBER", "38", "9"},
	))
	out = runWrite(t, f.destination(), "events_catalog.json", "")
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream "events": column "ID" of "SYNCLET_DB"."RAW"."EVENTS" is NUMBER(38,9), the catalog maps it to NUMBER(38,0)`)
}

// TestWriteStageCleanupFailureWarns removes the run's staged files after a
// sync; a failed REMOVE is a warning, not a failed sync.
func TestWriteStageCleanupFailureWarns(t *testing.T) {
	f := protocolFake(t).failOn("REMOVE @~/synclet/", 0, "003001", "Insufficient privileges")
	out := runWrite(t, f.destination(), "protocol_catalog.json", readTestdata(t, "protocol_messages.jsonl"))
	require.NoError(t, out.Err)
	var warned bool
	for _, l := range out.Logs() {
		if strings.Contains(l.Message, "remove staged files @~/synclet/") && strings.Contains(l.Message, "Insufficient privileges") {
			warned = true
		}
	}
	assert.True(t, warned, "logs: %v", out.Logs())
	assert.Len(t, out.States(), 3)
}

// TestWriteNoStagedFilesNoRemove issues no REMOVE when nothing was staged.
func TestWriteNoStagedFilesNoRemove(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS"))
	out := runWrite(t, f.destination(), "events_catalog.json", "")
	require.NoError(t, out.Err)
	for _, s := range f.executed() {
		assert.NotContains(t, s, "REMOVE")
	}
}

// TestWriteOverwriteCopiesGrants creates the shadow of an existing table
// LIKE it with COPY GRANTS, so the swap keeps readers' grants, and alters
// its columns to the catalog's when they differ.
func TestWriteOverwriteCopiesGrants(t *testing.T) {
	shadow := sch + `."_SYNCLET_TMP_RATES"`
	like := `CREATE OR REPLACE TABLE ` + shadow + ` LIKE ` + sch + `."RATES" COPY GRANTS`
	meta := [][4]string{{"_AIRBYTE_RAW_ID", "TEXT", "", ""}, {"_AIRBYTE_EXTRACTED_AT", "TIMESTAMP_TZ", "", ""}}

	same := append(meta, [4]string{"CODE", "TEXT", "", ""}, [4]string{"RATE", "FLOAT", "", ""})
	f := newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS"), tableColumns("USERS"), tableColumns("RATES", same...))
	out := runWrite(t, f.destination(), "protocol_catalog.json", readTestdata(t, "protocol_messages.jsonl"))
	require.NoError(t, out.Err)
	sqls := f.executed()
	assert.Contains(t, sqls, like)
	for _, s := range sqls {
		assert.NotContains(t, s, "ALTER TABLE "+shadow+" ADD", "columns match")
		assert.NotContains(t, s, "ALTER TABLE "+shadow+" DROP", "columns match")
		assert.NotContains(t, s, "CREATE OR REPLACE TABLE "+shadow+" (")
	}

	changed := append(meta, [4]string{"RATE", "NUMBER", "38", "9"}, [4]string{"OLD", "TEXT", "", ""})
	f = newFake(t).on(sessionOK, schemaExists, tableColumns("EVENTS"), tableColumns("USERS"), tableColumns("RATES", changed...))
	out = runWrite(t, f.destination(), "protocol_catalog.json", readTestdata(t, "protocol_messages.jsonl"))
	require.NoError(t, out.Err)
	sqls = f.executed()
	i := slices.Index(sqls, like)
	require.GreaterOrEqual(t, i, 0, "statements: %v", sqls)
	assert.Equal(t, []string{
		like,
		`ALTER TABLE ` + shadow + ` ADD COLUMN "CODE" VARCHAR`,
		`ALTER TABLE ` + shadow + ` DROP COLUMN "RATE"`,
		`ALTER TABLE ` + shadow + ` DROP COLUMN "OLD"`,
		`ALTER TABLE ` + shadow + ` ADD COLUMN "RATE" FLOAT`,
	}, sqls[i:i+5])
}

// TestWriteFailsOnInexactNumber names the stream, the record and the
// column of a number FLOAT cannot hold exactly.
func TestWriteFailsOnInexactNumber(t *testing.T) {
	f := protocolFake(t)
	msgs := `{"type":"RECORD","record":{"stream":"rates","data":{"code":"A","rate":0.30000000000000004}}}
{"type":"RECORD","record":{"stream":"rates","data":{"code":"B","rate":9007199254740993}}}
`
	out := runWrite(t, f.destination(), "protocol_catalog.json", msgs)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream "rates": record 2: column "rate": value "9007199254740993" has more precision than FLOAT`)
}

func count(sqls []string, s string) int {
	n := 0
	for _, q := range sqls {
		if q == s {
			n++
		}
	}
	return n
}

// TestWriteTypesFixture writes the type fixture and checks the column types
// of the table and the text staged for each value.
func TestWriteTypesFixture(t *testing.T) {
	f := newFake(t).on(sessionOK, schemaExists, tableColumns("ALL_TYPES"))
	out := runWrite(t, f.destination(), "types_catalog.json", readTestdata(t, "types_messages.jsonl"))
	require.NoError(t, out.Err)
	assert.Contains(t, f.executed(), `CREATE TABLE IF NOT EXISTS `+sch+`."ALL_TYPES" ("_AIRBYTE_RAW_ID" VARCHAR, "_AIRBYTE_EXTRACTED_AT" TIMESTAMP_TZ, `+
		`"AMOUNT" FLOAT, "AT_NTZ" TIMESTAMP_NTZ, "AT_TZ" TIMESTAMP_TZ, "AT_TZ2" TIMESTAMP_TZ, "BIG" NUMBER(38,0), "CLOCK" TIME, "CLOCK_NTZ" TIME, "CLOCK_TZ" VARCHAR, `+
		`"COUNT" NUMBER(38,0), "DAY" DATE, "FLAG" BOOLEAN, "ID" NUMBER(38,0), "MIXED" VARIANT, "NAME" VARCHAR, "PAYLOAD" VARIANT, "REF_NUM" FLOAT, "REF_TS" TIMESTAMP_TZ, `+
		`"TAGS" VARIANT, "UNION" VARIANT, "UNTYPED" VARIANT)`)
	rows := f.rowsOf(sch + `."_SYNCLET_STAGE_0"`)
	require.Len(t, rows, 1)
	assert.Equal(t, []string{
		"37.77492912345",
		"2024-01-02 03:04:05.123456789",
		"2024-01-02 03:04:05.123456789 +05:30",
		"2024-01-02 03:04:05.000000000 +00:00",
		"12345678901234567890123456789012345678",
		"23:59:59.999999999",
		"00:00:00.000000000",
		"10:00:00+02:00",
		"3",
		"2024-02-29",
		"true",
		"1",
		`"s"`,
		"n",
		`{"a":12345678901234567890123456789012345678}`,
		"1.5",
		"2024-01-02 03:04:05.000000000 +00:00",
		`["x","y"]`,
		"7",
		`{"k":null}`,
	}, rows[0][3:])
}
