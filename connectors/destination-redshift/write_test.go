package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const metaDefs = `"_airbyte_raw_id" character varying(36), "_airbyte_extracted_at" timestamp with time zone, "_airbyte_meta" super`
const metaCols = `"_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta"`

// copySQL is the COPY a batch of table's stage index loads with: MinIO's
// root keys, the configured region and the CSV options.
func copySQL(m *minioServer, bucket string, index int, cols, table string, part int) string {
	return fmt.Sprintf(`COPY "_synclet_stage_%d" ("_synclet_seq", %s, %s) FROM 's3://%s/stage/sync1/%s/%d.csv.gz' `+
		`ACCESS_KEY_ID '%s' SECRET_ACCESS_KEY '%s' REGION 'eu-west-1' `+
		`FORMAT AS CSV GZIP NULL AS '__synclet_null__' DATEFORMAT 'YYYY-MM-DD' TIMEFORMAT 'auto'`,
		index, metaCols, cols, bucket, table, part, m.user, m.password)
}

// stagedRows records, at each COPY, the rows of the file it loads; the
// writer deletes the file once the batch is committed.
type stagedRows struct {
	mu   sync.Mutex
	rows map[string][][]string
}

var copyFrom = regexp.MustCompile(`FROM 's3://([^/]+)/([^']+)'`)

func (s *stagedRows) hook(t *testing.T, m *minioServer) func(sql string) {
	s.rows = map[string][][]string{}
	return func(sql string) {
		if !strings.HasPrefix(sql, "COPY ") {
			return
		}
		mm := copyFrom.FindStringSubmatch(sql)
		require.NotNil(t, mm, sql)
		rows := m.csvRows(t, mm[1], mm[2])
		s.mu.Lock()
		s.rows[mm[2]] = rows
		s.mu.Unlock()
	}
}

func TestWriteAllTypes(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	staged := &stagedRows{}
	r := &recorder{onExec: staged.hook(t, m)}
	s := stream("all_types", `{"type":"object","properties":{
		"id":{"type":"integer"},
		"bool_col":{"type":"boolean"},
		"int_col":{"type":"integer"},
		"airbyte_int_col":{"type":"number","airbyte_type":"integer"},
		"num_col":{"type":"number"},
		"big_int_col":{"type":"string","airbyte_type":"big_integer"},
		"str_col":{"type":"string"},
		"long_col":{"type":["null","string"]},
		"date_col":{"type":"string","format":"date"},
		"ts_col":{"type":"string","format":"date-time"},
		"ts_ntz_col":{"type":"string","format":"date-time","airbyte_type":"timestamp_without_timezone"},
		"time_col":{"type":"string","format":"time"},
		"timetz_col":{"type":"string","airbyte_type":"time_with_timezone"},
		"obj_col":{"type":"object"},
		"arr_col":{"type":"array"},
		"union_col":{"type":["string","integer"]},
		"untyped_col":{}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	long := strings.Repeat("x", 70000)
	out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s},
		rec("all_types", strings.ReplaceAll(`{"id":1,"bool_col":true,"int_col":-9223372036854775808,"airbyte_int_col":7,"num_col":0.30000000000000004,
			"big_int_col":"123456789012345678901234567890","str_col":"tab\there, \"quoted\"\nnewline \\ backslash","long_col":"`+long+`",
			"date_col":"2024-02-29","ts_col":"2024-02-29T13:14:15.123456+02:00","ts_ntz_col":"2024-02-29T13:14:15","time_col":"13:14:15",
			"timetz_col":"13:14:15+02:00","obj_col":{"a":1,"nested":{"n":99999999999999999999.5}},"arr_col":[1.10,2],"union_col":"x","untyped_col":{"k":"v"}}`, "\n\t\t\t", "")),
		rec("all_types", `{"id":2,"num_col":1e-30,"str_col":"","union_col":7}`),
		state("all_types", `{"cursor":2}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{`{"cursor":2}`}, stateValues(out))

	cols := `"airbyte_int_col" bigint, "arr_col" super, "big_int_col" numeric(38,0), "bool_col" boolean, "date_col" date, ` +
		`"id" bigint, "int_col" bigint, "long_col" character varying(65535), "num_col" double precision, "obj_col" super, ` +
		`"str_col" character varying(65535), "time_col" time without time zone, "timetz_col" time with time zone, ` +
		`"ts_col" timestamp with time zone, "ts_ntz_col" timestamp without time zone, "union_col" super, "untyped_col" super`
	assert.Contains(t, r.log(), `CREATE TABLE IF NOT EXISTS "s"."all_types" (`+metaDefs+`, `+cols+`)`)

	rows := staged.rows["stage/sync1/all_types/1.csv.gz"]
	require.Len(t, rows, 2)
	uuidRE := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	for i, row := range rows {
		assert.Equal(t, fmt.Sprint(i+1), row[0], "seq")
		assert.Regexp(t, uuidRE, row[1], "raw id")
		assert.Equal(t, "2023-11-14 22:13:20+00", row[2], "extracted at")
	}
	assert.Equal(t, []string{
		`{"changes":[{"field":"long_col","change":"TRUNCATED","reason":"DESTINATION_FIELD_SIZE_LIMITATION"}]}`,
		"7", "[1.10,2]", "123456789012345678901234567890", "true", "2024-02-29", "1", "-9223372036854775808",
		strings.Repeat("x", 65535), "0.30000000000000004", `{"a":1,"nested":{"n":99999999999999999999.5}}`,
		"tab\there, \"quoted\"\nnewline \\ backslash", "13:14:15", "13:14:15+02:00",
		"2024-02-29 11:14:15.123456+00", "2024-02-29 13:14:15", `"x"`, `{"k":"v"}`,
	}, rows[0][3:])
	assert.Equal(t, []string{
		`{"changes":[]}`, "__synclet_null__", "__synclet_null__", "__synclet_null__", "__synclet_null__", "__synclet_null__", "2", "__synclet_null__",
		"__synclet_null__", "1e-30", "__synclet_null__", "", "__synclet_null__", "__synclet_null__",
		"__synclet_null__", "__synclet_null__", "7", "__synclet_null__",
	}, rows[1][3:])

	var warns []string
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn {
			warns = append(warns, l.Message)
		}
	}
	assert.Contains(t, warns, `stream "all_types": 1 value longer than 65535 bytes truncated (recorded in _airbyte_meta): long_col`)
	assert.Empty(t, m.keys(t, bucket), "staged files are purged after the commit")
}

func TestWriteKeepsStagedFilesWhenAsked(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	cfg := m.destConfig(bucket)
	keep := false
	cfg.UploadingMethod.PurgeStagingData = &keep
	s := stream("events", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	out := runWrite(t, recordingDestination(m, &recorder{}), cfg, []synclet.ConfiguredStream{s}, rec("events", `{"id":1}`))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"stage/sync1/events/1.csv.gz"}, m.keys(t, bucket))
}

func TestWriteValueThatDoesNotFit(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	s := stream("nums", `{"type":"object","properties":{"n":{"type":"integer"},"x":{"type":"number"},"s":{"type":"string"},"o":{"type":"object"}}}`,
		synclet.DestinationSyncModeAppend, nil, nil)
	for _, c := range []struct{ data, want string }{
		{`{"x": 0.912345678901234567890}`, `stream "nums": record 1: column "x": value 0.912345678901234567890 has more precision than DOUBLE PRECISION`},
		{`{"n": 99999999999999999999}`, `stream "nums": record 1: column "n": value 99999999999999999999 is outside the BIGINT range`},
		{`{"s": "__synclet_null__"}`, `stream "nums": record 1: column "s": value equals the staging NULL marker`},
		{`{"o": {"k": ["` + strings.Repeat(strings.Repeat("a", 60000)+`","`, 80) + `"]}}`, `stream "nums": record 1: row is larger than 4 MiB`},
	} {
		r := &recorder{}
		out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s}, rec("nums", c.data), state("nums", `1`))
		require.Error(t, out.Err, c.want)
		assert.Contains(t, out.Err.Error(), c.want)
		assert.Empty(t, out.States())
		for _, sql := range r.log() {
			assert.NotContains(t, sql, "COPY", "nothing is loaded")
		}
		assert.Empty(t, m.keys(t, bucket))
	}
}

func TestWriteOverwrite(t *testing.T) {
	m := startMinio(t)
	s := stream("rates", `{"type":"object","properties":{"code":{"type":"string"},"rate":{"type":"number"}}}`, synclet.DestinationSyncModeOverwrite, nil, nil)
	lines := []string{rec("rates", `{"code":"EUR","rate":0.91}`), rec("rates", `{"code":"UAH","rate":41.25}`), state("rates", `{}`)}
	cols := `"code", "rate"`
	defs := metaDefs + `, "code" character varying(65535), "rate" double precision`

	t.Run("swaps the shadow table after the last commit", func(t *testing.T) {
		bucket := m.newBucket(t)
		r := &recorder{}
		out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s}, lines...)
		require.NoError(t, out.Err)
		assert.Equal(t, []string{`{}`}, stateValues(out))
		assert.Equal(t, []string{
			`CREATE SCHEMA IF NOT EXISTS "s"`,
			`DROP TABLE IF EXISTS "s"."_synclet_tmp_rates"`,
			`CREATE TABLE "s"."_synclet_tmp_rates" (` + defs + `)`,
			`BEGIN`,
			`DROP TABLE IF EXISTS "_synclet_stage_0"`,
			`CREATE TEMP TABLE "_synclet_stage_0" ("_synclet_seq" bigint, ` + defs + `)`,
			copySQL(m, bucket, 0, cols, "rates", 1),
			`INSERT INTO "s"."_synclet_tmp_rates" (` + metaCols + `, ` + cols + `) SELECT ` + metaCols + `, ` + cols + ` FROM "_synclet_stage_0"`,
			`COMMIT`,
			`BEGIN`,
			`DROP TABLE IF EXISTS "s"."rates"`,
			`ALTER TABLE "s"."_synclet_tmp_rates" RENAME TO "rates"`,
			`COMMIT`,
		}, r.log())
	})

	t.Run("a failed sync keeps the old table and drops the shadow", func(t *testing.T) {
		bucket := m.newBucket(t)
		r := &recorder{fail: func(sql string) error {
			if strings.HasPrefix(sql, "COPY ") {
				return &pgconn.PgError{Severity: "ERROR", Code: "XX000", Message: "Load into table '_synclet_stage_0' failed."}
			}
			return nil
		}}
		out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s}, lines...)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `stream "rates": load batch: Load into table '_synclet_stage_0' failed.`)
		assert.Empty(t, out.States())
		log := r.log()
		assert.Equal(t, []string{`ROLLBACK`, `DROP TABLE IF EXISTS "s"."_synclet_tmp_rates"`}, log[len(log)-2:])
		for _, sql := range log {
			assert.NotContains(t, sql, "RENAME", "the old table stays")
		}
		assert.Empty(t, m.keys(t, bucket), "the staged file is deleted")
	})
}

func TestWriteAppend(t *testing.T) {
	m := startMinio(t)
	s := stream("events", `{"type":"object","properties":{"id":{"type":"integer"},"kind":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	existing := func(cols map[string]string) func(string) ([][]any, error) {
		return func(sql string) ([][]any, error) {
			if !strings.Contains(sql, "format_type") {
				return nil, nil
			}
			var rows [][]any
			for n, typ := range cols {
				rows = append(rows, []any{n, typ})
			}
			return rows, nil
		}
	}
	meta := map[string]string{"_airbyte_raw_id": "character varying(36)", "_airbyte_extracted_at": "timestamp with time zone", "_airbyte_meta": "super"}
	with := func(extra map[string]string) map[string]string {
		out := map[string]string{}
		for k, v := range meta {
			out[k] = v
		}
		for k, v := range extra {
			out[k] = v
		}
		return out
	}

	t.Run("inserts into the existing table and adds new columns", func(t *testing.T) {
		bucket := m.newBucket(t)
		r := &recorder{rows: existing(with(map[string]string{"id": "bigint"}))}
		out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s}, rec("events", `{"id":1,"kind":"click"}`))
		require.NoError(t, out.Err)
		cols := `"id", "kind"`
		assert.Equal(t, []string{
			`CREATE SCHEMA IF NOT EXISTS "s"`,
			`ALTER TABLE "s"."events" ADD COLUMN "kind" character varying(65535)`,
			`BEGIN`,
			`DROP TABLE IF EXISTS "_synclet_stage_0"`,
			`CREATE TEMP TABLE "_synclet_stage_0" ("_synclet_seq" bigint, ` + metaDefs + `, "id" bigint, "kind" character varying(65535))`,
			copySQL(m, bucket, 0, cols, "events", 1),
			`INSERT INTO "s"."events" (` + metaCols + `, ` + cols + `) SELECT ` + metaCols + `, ` + cols + ` FROM "_synclet_stage_0"`,
			`COMMIT`,
		}, r.log())
	})

	t.Run("a changed column type fails before anything is loaded", func(t *testing.T) {
		bucket := m.newBucket(t)
		r := &recorder{rows: existing(with(map[string]string{"id": "bigint", "kind": "bigint"}))}
		out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s}, rec("events", `{"id":1,"kind":"click"}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `column "kind" of table "s"."events" is bigint, the catalog needs character varying(65535)`)
		assert.Equal(t, []string{`CREATE SCHEMA IF NOT EXISTS "s"`}, r.log())
	})
}

func TestMergeSQL(t *testing.T) {
	cs := stream("Users", `{"type":"object","properties":{"id":{"type":"integer"},"org":{"type":"string"},"name":{"type":"string"},"updated_at":{"type":"string","format":"date-time"}}}`,
		synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}, {"org"}}, []string{"updated_at"})
	plans, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{cs}})
	require.NoError(t, err)
	cols := metaCols + `, "id", "name", "org", "updated_at"`
	assert.Equal(t, []string{
		`DROP TABLE IF EXISTS "_synclet_dedup_0"`,
		`CREATE TEMP TABLE "_synclet_dedup_0" AS SELECT ` + cols + ` FROM (SELECT "_synclet_seq", ` + cols +
			`, ROW_NUMBER() OVER (PARTITION BY "id", "org" ORDER BY "updated_at" DESC NULLS LAST, "_synclet_seq" DESC) AS "_synclet_rn" FROM "_synclet_stage_0") AS src` +
			` WHERE src."_synclet_rn" = 1 AND NOT EXISTS (SELECT 1 FROM "s"."users" AS tgt WHERE tgt."id" = src."id" AND tgt."org" = src."org"` +
			` AND tgt."updated_at" IS NOT NULL AND (src."updated_at" IS NULL OR src."updated_at" < tgt."updated_at"))`,
		`MERGE INTO "s"."users" USING "_synclet_dedup_0" AS src ON "s"."users"."id" = src."id" AND "s"."users"."org" = src."org"` +
			` WHEN MATCHED THEN UPDATE SET "_airbyte_raw_id" = src."_airbyte_raw_id", "_airbyte_extracted_at" = src."_airbyte_extracted_at",` +
			` "_airbyte_meta" = src."_airbyte_meta", "name" = src."name", "updated_at" = src."updated_at"` +
			` WHEN NOT MATCHED THEN INSERT (` + cols + `) VALUES (src."_airbyte_raw_id", src."_airbyte_extracted_at", src."_airbyte_meta",` +
			` src."id", src."name", src."org", src."updated_at")`,
	}, dedupSQL("s", plans[0], `"_synclet_stage_0"`))

	noCursor := cs
	noCursor.CursorField = nil
	plans, err = planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{noCursor}})
	require.NoError(t, err)
	sql := dedupSQL("s", plans[0], `"_synclet_stage_0"`)
	assert.Contains(t, sql[1], `ORDER BY "_synclet_seq" DESC) AS "_synclet_rn" FROM "_synclet_stage_0") AS src WHERE src."_synclet_rn" = 1`)
	assert.NotContains(t, sql[1], "NOT EXISTS", "without a cursor the latest record wins")
}

func TestWriteDedup(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	r := &recorder{}
	s := stream("users", `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"},"updated_at":{"type":"string","format":"date-time"}}}`,
		synclet.DestinationSyncModeAppendDedup, [][]string{{"id"}}, []string{"updated_at"})
	out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s},
		rec("users", `{"id":1,"name":"Ann","updated_at":"2024-01-01T00:00:00Z"}`), state("users", `1`))
	require.NoError(t, out.Err)
	log := r.log()
	assert.Equal(t, `CREATE TABLE IF NOT EXISTS "s"."users" (`+metaDefs+`, "id" bigint, "name" character varying(65535), "updated_at" timestamp with time zone, PRIMARY KEY ("id"))`, log[1])
	plans, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{s}})
	require.NoError(t, err)
	assert.Equal(t, append(dedupSQL("s", plans[0], `"_synclet_stage_0"`), "COMMIT"), log[len(log)-4:])

	for _, c := range []struct {
		pk     [][]string
		cursor []string
		want   string
	}{
		{nil, nil, "append_dedup needs a primary key"},
		{[][]string{{"id"}}, []string{"tags"}, `cursor field "tags" lands as SUPER`},
		{[][]string{{"tags"}}, nil, `primary key field "tags" lands as SUPER`},
		{[][]string{{"id"}, {"a", "b"}}, nil, "nested"},
	} {
		bad := stream("users", `{"type":"object","properties":{"id":{"type":"integer"},"tags":{"type":"array"}}}`, synclet.DestinationSyncModeAppendDedup, c.pk, c.cursor)
		out := runWrite(t, recordingDestination(m, &recorder{}), m.destConfig(bucket), []synclet.ConfiguredStream{bad}, rec("users", `{"id":1}`))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), c.want)
	}
	nullKey := runWrite(t, recordingDestination(m, &recorder{}), m.destConfig(bucket), []synclet.ConfiguredStream{s}, rec("users", `{"name":"x"}`))
	require.Error(t, nullKey.Err)
	assert.Contains(t, nullKey.Err.Error(), `stream "users": record 1: primary key column "id" is null`)
}

// commitsAtState is the emitter's output: at each STATE it notes how many
// COMMIT statements the recorder has seen.
type commitsAtState struct {
	r   *recorder
	got []string
}

func (c *commitsAtState) Write(p []byte) (int, error) {
	line := string(p)
	if strings.Contains(line, `"type":"STATE"`) {
		mm := regexp.MustCompile(`"stream_state":("[^"]*"|[0-9]+)`).FindStringSubmatch(line)
		commits := 0
		for _, sql := range c.r.log() {
			if sql == "COMMIT" {
				commits++
			}
		}
		c.got = append(c.got, fmt.Sprintf("%s after %d commits", mm[1], commits))
	}
	return len(p), nil
}

// TestStateAfterCommit checks that every echoed STATE follows the COMMIT of
// the records before it, by counting the commits when each state is written.
func TestStateAfterCommit(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	r := &recorder{}
	d := recordingDestination(m, r)
	d.maxRows = 2
	s := stream("events", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	lines := []string{
		state("events", `0`),
		rec("events", `{"id":1}`), state("events", `1`),
		rec("events", `{"id":2}`), state("events", `2`),
		rec("events", `{"id":3}`), state("events", `3`),
	}
	em := &commitsAtState{r: r}
	cfg := m.destConfig(bucket)
	err := d.Write(t.Context(), cfg, &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{s}},
		synclet.NewMessageReader(strings.NewReader(strings.Join(lines, "\n")+"\n")), synclet.NewEmitter(em))
	require.NoError(t, err)
	assert.Equal(t, []string{"0 after 0 commits", "1 after 1 commits", "2 after 1 commits", "3 after 2 commits"}, em.got)
}

func TestStateMidBatchFailure(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	copies := 0
	r := &recorder{
		fail: func(sql string) error {
			if strings.HasPrefix(sql, "COPY ") {
				copies++
				if copies == 2 {
					return &pgconn.PgError{Severity: "ERROR", Code: "XX000", Message: "Load into table '_synclet_stage_0' failed. Check 'sys_load_error_detail' system table for details."}
				}
			}
			return nil
		},
		rows: func(sql string) ([][]any, error) {
			if strings.Contains(sql, "sys_load_error_detail") {
				return [][]any{{int64(2), "kind", "Invalid digit, Value 'x', Pos 0, Type: Integer"}}, nil
			}
			return nil, nil
		},
	}
	d := recordingDestination(m, r)
	d.maxRows = 2
	s := stream("events", `{"type":"object","properties":{"id":{"type":"integer"},"kind":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	out := runWrite(t, d, m.destConfig(bucket), []synclet.ConfiguredStream{s},
		rec("events", `{"id":1}`), rec("events", `{"id":2}`), state("events", `"s1"`),
		rec("events", `{"id":3}`), rec("events", `{"id":4,"kind":"x"}`), state("events", `"s2"`))
	require.Error(t, out.Err)
	assert.Equal(t, []string{`"s1"`}, stateValues(out), "the state after the failed batch is not echoed")
	assert.Contains(t, out.Err.Error(), `stream "events": record 4: column "kind": Invalid digit, Value 'x', Pos 0, Type: Integer`)
	log := r.log()
	assert.Equal(t, "ROLLBACK", log[len(log)-1])
	assert.Equal(t, 1, strings.Count(strings.Join(log, "\n"), "COMMIT"))
	assert.Empty(t, m.keys(t, bucket))
}

func TestStateHeldUntilSwap(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	r := &recorder{fail: func(sql string) error {
		if strings.HasPrefix(sql, "ALTER TABLE") {
			return errors.New("swap failed")
		}
		return nil
	}}
	d := recordingDestination(m, r)
	d.maxRows = 1
	ow := stream("rates", `{"type":"object","properties":{"code":{"type":"string"}}}`, synclet.DestinationSyncModeOverwrite, nil, nil)
	ap := stream("events", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	out := runWrite(t, d, m.destConfig(bucket), []synclet.ConfiguredStream{ow, ap},
		rec("rates", `{"code":"EUR"}`), state("rates", `"r"`),
		rec("events", `{"id":1}`), state("events", `"e"`),
		`{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"g":1},"stream_states":[]}}}`)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "swap failed")
	assert.Equal(t, []string{`"e"`}, stateValues(out), "states of the overwrite stream and GLOBAL states wait for the swap")
	assert.Len(t, out.States(), 1)
}

func TestWriteTruncatesLongStringsInSuper(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	staged := &stagedRows{}
	r := &recorder{onExec: staged.hook(t, m)}
	s := stream("docs", `{"type":"object","properties":{"o":{"type":"object"},"u":{}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	long := strings.Repeat("x", maxVarcharBytes+10)
	out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s},
		rec("docs", `{"o":{"items":[{"body":"`+long+`"}]},"u":"`+long+`"}`))
	require.NoError(t, out.Err)
	rows := staged.rows["stage/sync1/docs/1.csv.gz"]
	require.Len(t, rows, 1)
	assert.JSONEq(t, `{"changes":[
		{"field":"o.items[0].body","change":"TRUNCATED","reason":"DESTINATION_FIELD_SIZE_LIMITATION"},
		{"field":"u","change":"TRUNCATED","reason":"DESTINATION_FIELD_SIZE_LIMITATION"}]}`, rows[0][3])
	cut := strings.Repeat("x", maxVarcharBytes)
	assert.Equal(t, `{"items":[{"body":"`+cut+`"}]}`, rows[0][4])
	assert.Equal(t, `"`+cut+`"`, rows[0][5])
}

// TestCopyErrorNamesRecordAcrossEmbeddedNewlines maps the physical line
// sys_load_error_detail reports to the record whose CSV row spans it.
func TestCopyErrorNamesRecordAcrossEmbeddedNewlines(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	r := &recorder{
		fail: func(sql string) error {
			if strings.HasPrefix(sql, "COPY ") {
				return &pgconn.PgError{Severity: "ERROR", Code: "XX000", Message: "Load failed."}
			}
			return nil
		},
		rows: func(sql string) ([][]any, error) {
			if strings.Contains(sql, "sys_load_error_detail") {
				return [][]any{{int64(4), "kind", "bad value"}}, nil
			}
			return nil, nil
		},
	}
	s := stream("my_events", `{"type":"object","properties":{"id":{"type":"integer"},"kind":{"type":"string"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	out := runWrite(t, recordingDestination(m, r), m.destConfig(bucket), []synclet.ConfiguredStream{s},
		rec("my_events", `{"id":1,"kind":"a\nb\nc"}`), rec("my_events", `{"id":2,"kind":"x"}`), rec("my_events", `{"id":3}`))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream "my_events": record 2: column "kind": bad value`)
	var detailQuery string
	for _, q := range r.queries {
		if strings.Contains(q, "sys_load_error_detail") {
			detailQuery = q
		}
	}
	assert.Contains(t, detailQuery, `file_name LIKE '%stage/sync1/my\\_events/1.csv.gz'`)
}

// swapsAtState records, for each echoed STREAM state, how many tables were
// renamed into place before it.
type swapsAtState struct {
	r   *recorder
	got []string
}

func (c *swapsAtState) Write(p []byte) (int, error) {
	line := string(p)
	if strings.Contains(line, `"type":"STATE"`) {
		what := "global"
		if mm := regexp.MustCompile(`"stream_state":("[^"]*")`).FindStringSubmatch(line); mm != nil {
			what = mm[1]
		}
		renames := 0
		for _, sql := range c.r.log() {
			if strings.HasPrefix(sql, "ALTER TABLE") {
				renames++
			}
		}
		c.got = append(c.got, fmt.Sprintf("%s after %d renames", what, renames))
	}
	return len(p), nil
}

func TestStateEchoedAfterSwap(t *testing.T) {
	m := startMinio(t)
	bucket := m.newBucket(t)
	r := &recorder{}
	d := recordingDestination(m, r)
	d.maxRows = 1
	ow := stream("rates", `{"type":"object","properties":{"code":{"type":"string"}}}`, synclet.DestinationSyncModeOverwrite, nil, nil)
	ap := stream("events", `{"type":"object","properties":{"id":{"type":"integer"}}}`, synclet.DestinationSyncModeAppend, nil, nil)
	lines := []string{
		rec("rates", `{"code":"EUR"}`), state("rates", `"r"`),
		rec("events", `{"id":1}`), state("events", `"e"`),
		`{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"g":1},"stream_states":[]}}}`,
	}
	em := &swapsAtState{r: r}
	err := d.Write(t.Context(), m.destConfig(bucket), &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{ow, ap}},
		synclet.NewMessageReader(strings.NewReader(strings.Join(lines, "\n")+"\n")), synclet.NewEmitter(em))
	require.NoError(t, err)
	assert.Equal(t, []string{`"e" after 0 renames`, `"r" after 1 renames`, `global after 1 renames`}, em.got)
}

func TestIsHeld(t *testing.T) {
	plans, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{
		stream("rates", `{"type":"object"}`, synclet.DestinationSyncModeOverwrite, nil, nil),
		stream("events", `{"type":"object"}`, synclet.DestinationSyncModeAppend, nil, nil),
	}})
	require.NoError(t, err)
	w := newWriter(nil, nil, Config{}, nil, Destination{}, "s", plans)
	streamState := func(name, ns string) synclet.StateMessage {
		return synclet.StateMessage{Type: synclet.StateTypeStream, Stream: &synclet.StreamState{
			StreamDescriptor: synclet.StreamDescriptor{Name: name, Namespace: ns}}}
	}
	assert.True(t, w.isHeld(streamState("rates", "")))
	assert.True(t, w.isHeld(streamState("rates", "public")), "a namespace the catalog lacks matches by name")
	assert.False(t, w.isHeld(streamState("events", "")))
	assert.False(t, w.isHeld(streamState("events", "public")))
	assert.True(t, w.isHeld(streamState("other", "")), "a state of no catalog stream may cover the overwrite stream")
	assert.True(t, w.isHeld(synclet.StateMessage{Type: synclet.StateTypeGlobal}))
	assert.True(t, w.isHeld(synclet.StateMessage{Type: synclet.StateTypeLegacy}))

	appendOnly := newWriter(nil, nil, Config{}, nil, Destination{}, "s", plans[1:])
	assert.False(t, appendOnly.isHeld(synclet.StateMessage{Type: synclet.StateTypeGlobal}))
}
