package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

var testLoc = location{catalog: "main", schema: "raw"}

const deltaProps = " USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"

func planOf(t *testing.T, mode synclet.DestinationSyncMode, schema string, pk [][]string, cursor []string) *streamPlan {
	t.Helper()
	plans, err := planStreams(&synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream:              synclet.Stream{Name: "Users", JSONSchema: json.RawMessage(schema)},
		DestinationSyncMode: mode,
		PrimaryKey:          pk,
		CursorField:         cursor,
	}}})
	require.NoError(t, err)
	return plans[0]
}

const usersSchema = `{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"},"updated_at":{"type":"string","format":"date-time"},"amount":{"type":"number"},"tags":{"type":"array"}}}`

const (
	usersCols   = "`_airbyte_raw_id`, `_airbyte_extracted_at`, `amount`, `id`, `name`, `tags`, `updated_at`"
	usersDefs   = "`_airbyte_raw_id` STRING, `_airbyte_extracted_at` TIMESTAMP, `amount` DECIMAL(38,9), `id` BIGINT, `name` STRING, `tags` VARIANT, `updated_at` TIMESTAMP"
	usersSelect = "c0 AS `_airbyte_raw_id`, CAST(c1 AS TIMESTAMP) AS `_airbyte_extracted_at`, CAST(c2 AS DECIMAL(38,9)) AS `amount`, CAST(c3 AS BIGINT) AS `id`, c4 AS `name`, parse_json(c5) AS `tags`, CAST(c6 AS TIMESTAMP) AS `updated_at`"
	wantFormat  = " FILEFORMAT = JSON FORMAT_OPTIONS ('mode' = 'FAILFAST', 'primitivesAsString' = 'true')"
)

func TestAppendSQL(t *testing.T) {
	p := planOf(t, synclet.DestinationSyncModeAppend, usersSchema, nil, nil)
	assert.Equal(t, "CREATE TABLE IF NOT EXISTS `main`.`raw`.`users` ("+usersDefs+")"+deltaProps, createTableSQL(testLoc, p))
	assert.Equal(t, "CREATE OR REPLACE TABLE `main`.`raw`.`_synclet_stage_users` (`_synclet_seq` BIGINT, "+usersDefs+")"+deltaProps, createStageSQL(testLoc, p))
	assert.Equal(t,
		"COPY INTO `main`.`raw`.`_synclet_stage_users` FROM (SELECT CAST(s AS BIGINT) AS `_synclet_seq`, "+usersSelect+
			" FROM '/Volumes/main/raw/stg/_synclet/r1/0-1.json')"+wantFormat,
		copySQL(testLoc, p, "/Volumes/main/raw/stg/_synclet/r1/0-1.json"))
	assert.Equal(t, "INSERT INTO `main`.`raw`.`users` ("+usersCols+") SELECT "+usersCols+" FROM `main`.`raw`.`_synclet_stage_users`", insertSQL(testLoc, p))
	assert.Equal(t, "ALTER TABLE `main`.`raw`.`users` ADD COLUMNS (`amount` DECIMAL(38,9), `tags` VARIANT)",
		addColumnsSQL(testLoc, p, []column{p.columns[2], p.columns[5]}))
}

// TestAppendDedupSQL merges a batch on the primary key: within the batch the
// row with the highest cursor wins, the latest one among equal cursors, and
// a stored row is replaced only by a cursor not lower than its own.
func TestAppendDedupSQL(t *testing.T) {
	p := planOf(t, synclet.DestinationSyncModeAppendDedup, usersSchema, [][]string{{"id"}}, []string{"updated_at"})
	assert.Equal(t,
		"MERGE INTO `main`.`raw`.`users` AS t USING (SELECT "+usersCols+" FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY `id` ORDER BY `updated_at` DESC NULLS LAST, `_synclet_seq` DESC) AS `_synclet_rn` FROM `main`.`raw`.`_synclet_stage_users`) WHERE `_synclet_rn` = 1) AS s"+
			" ON t.`id` = s.`id`"+
			" WHEN MATCHED AND (t.`updated_at` IS NULL OR s.`updated_at` >= t.`updated_at`) THEN UPDATE SET t.`_airbyte_raw_id` = s.`_airbyte_raw_id`, t.`_airbyte_extracted_at` = s.`_airbyte_extracted_at`, t.`amount` = s.`amount`, t.`name` = s.`name`, t.`tags` = s.`tags`, t.`updated_at` = s.`updated_at`"+
			" WHEN NOT MATCHED THEN INSERT ("+usersCols+") VALUES (s.`_airbyte_raw_id`, s.`_airbyte_extracted_at`, s.`amount`, s.`id`, s.`name`, s.`tags`, s.`updated_at`)",
		mergeSQL(testLoc, p))
}

func TestAppendDedupSQLCompositeKeyWithoutCursor(t *testing.T) {
	schema := `{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"integer"},"v":{"type":"string"}}}`
	p := planOf(t, synclet.DestinationSyncModeAppendDedup, schema, [][]string{{"a"}, {"b"}}, nil)
	cols := "`_airbyte_raw_id`, `_airbyte_extracted_at`, `a`, `b`, `v`"
	assert.Equal(t,
		"MERGE INTO `main`.`raw`.`users` AS t USING (SELECT "+cols+" FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY `a`, `b` ORDER BY `_synclet_seq` DESC) AS `_synclet_rn` FROM `main`.`raw`.`_synclet_stage_users`) WHERE `_synclet_rn` = 1) AS s"+
			" ON t.`a` = s.`a` AND t.`b` = s.`b`"+
			" WHEN MATCHED THEN UPDATE SET t.`_airbyte_raw_id` = s.`_airbyte_raw_id`, t.`_airbyte_extracted_at` = s.`_airbyte_extracted_at`, t.`v` = s.`v`"+
			" WHEN NOT MATCHED THEN INSERT ("+cols+") VALUES (s.`_airbyte_raw_id`, s.`_airbyte_extracted_at`, s.`a`, s.`b`, s.`v`)",
		mergeSQL(testLoc, p))
}

// TestOverwriteSQL writes an overwrite stream into a shadow table that
// replaces the table in one statement at the end of a successful sync.
func TestOverwriteSQL(t *testing.T) {
	p := planOf(t, synclet.DestinationSyncModeOverwrite, usersSchema, nil, nil)
	assert.Equal(t, "CREATE OR REPLACE TABLE `main`.`raw`.`_synclet_tmp_users` ("+usersDefs+")"+deltaProps, createShadowSQL(testLoc, p))
	assert.Equal(t,
		"COPY INTO `main`.`raw`.`_synclet_tmp_users` FROM (SELECT "+usersSelect+" FROM '/Volumes/main/raw/stg/_synclet/r1/0-1.json')"+wantFormat,
		copySQL(testLoc, p, "/Volumes/main/raw/stg/_synclet/r1/0-1.json"))
	assert.Equal(t, "CREATE OR REPLACE TABLE `main`.`raw`.`users`"+deltaProps+" AS SELECT * FROM `main`.`raw`.`_synclet_tmp_users`", swapSQL(testLoc, p))
}

func TestQuotingInSQL(t *testing.T) {
	p := planOf(t, synclet.DestinationSyncModeAppend, `{"type":"object","properties":{"we`+"`"+`ird name":{"type":"string"}}}`, nil, nil)
	loc := location{catalog: "c`at", schema: "s'ch"}
	assert.Equal(t, "INSERT INTO `c``at`.`s'ch`.`users` (`_airbyte_raw_id`, `_airbyte_extracted_at`, `we``ird name`) SELECT `_airbyte_raw_id`, `_airbyte_extracted_at`, `we``ird name` FROM `c``at`.`s'ch`.`_synclet_stage_users`", insertSQL(loc, p))
	assert.Contains(t, copySQL(loc, p, `/Volumes/c`+"`"+`at/s'ch/v\x/f.json`), ` FROM '/Volumes/c`+"`"+`at/s\'ch/v\\x/f.json')`)
}
