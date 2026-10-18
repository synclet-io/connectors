package main

import (
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTableSQL(t *testing.T) {
	cols := []columnDef{
		{name: "id", bqType: "INT64"},
		{name: "name", bqType: "STRING"},
	}
	colTypes := map[string]bigquery.FieldType{
		"id":   bigquery.IntegerFieldType,
		"name": bigquery.StringFieldType,
	}

	sql := createTableSQL("my-project", "my_dataset", "my_table", cols, []string{"id"}, colTypes)

	assert.Contains(t, sql, "CREATE TABLE `my-project`.`my_dataset`.`my_table`")
	assert.Contains(t, sql, "_airbyte_raw_id STRING NOT NULL")
	assert.Contains(t, sql, "_airbyte_extracted_at TIMESTAMP NOT NULL")
	assert.Contains(t, sql, "_airbyte_meta JSON NOT NULL")
	assert.Contains(t, sql, "_airbyte_generation_id INT64")
	assert.Contains(t, sql, "`id` INT64")
	assert.Contains(t, sql, "`name` STRING")
	assert.Contains(t, sql, "PARTITION BY (DATE_TRUNC(_airbyte_extracted_at, DAY))")
	assert.Contains(t, sql, "CLUSTER BY")
}

func TestCreateTableSQLNoPK(t *testing.T) {
	cols := []columnDef{
		{name: "val", bqType: "STRING"},
	}

	sql := createTableSQL("p", "d", "t", cols, nil, nil)
	assert.Contains(t, sql, "PARTITION BY")
	assert.Contains(t, sql, "CLUSTER BY `_airbyte_extracted_at`")
}

func TestGenerateMergeSQL(t *testing.T) {
	sql := generateMergeSQL(
		"proj", "ds", "target", "ds", "staging",
		[]string{"id", "name", "email"},
		[]string{"id"},
		"updated_at",
		false,
	)

	// Basic MERGE structure
	assert.Contains(t, sql, "MERGE `proj`.`ds`.`target`")
	assert.Contains(t, sql, "AS target")
	assert.Contains(t, sql, "AS source")

	// NULL-safe PK matching
	assert.Contains(t, sql, "target.`id` = source.`id`")
	assert.Contains(t, sql, "target.`id` IS NULL AND source.`id` IS NULL")

	// Cursor comparison
	assert.Contains(t, sql, "updated_at")

	// WHEN MATCHED / WHEN NOT MATCHED
	assert.Contains(t, sql, "WHEN MATCHED")
	assert.Contains(t, sql, "THEN UPDATE SET")
	assert.Contains(t, sql, "WHEN NOT MATCHED")
	assert.Contains(t, sql, "THEN INSERT")

	// Should NOT contain CDC delete
	assert.NotContains(t, sql, "_ab_cdc_deleted_at")
}

func TestGenerateMergeSQLWithCDCHardDelete(t *testing.T) {
	sql := generateMergeSQL(
		"proj", "ds", "target", "ds", "staging",
		[]string{"id", "name"},
		[]string{"id"},
		"updated_at",
		true,
	)

	// CDC hard delete clause
	assert.Contains(t, sql, "_ab_cdc_deleted_at IS NOT NULL")
	assert.Contains(t, sql, "THEN DELETE")

	// CDC skip insert for deleted records
	assert.Contains(t, sql, "_ab_cdc_deleted_at IS NULL")
}

func TestGenerateMergeSQLNoCursor(t *testing.T) {
	sql := generateMergeSQL(
		"proj", "ds", "target", "ds", "staging",
		[]string{"id"},
		[]string{"id"},
		"",
		false,
	)

	// Without cursor, falls back to _airbyte_extracted_at comparison
	assert.Contains(t, sql, "_airbyte_extracted_at")
	assert.Contains(t, sql, "WHEN MATCHED")
}

func TestSelectDedupedSQL(t *testing.T) {
	sql := selectDedupedSQL("proj", "ds", "staging", []string{"id"}, "updated_at")

	assert.Contains(t, sql, "ROW_NUMBER() OVER")
	assert.Contains(t, sql, "PARTITION BY `id`")
	assert.Contains(t, sql, "`updated_at` DESC NULLS LAST")
	assert.Contains(t, sql, "_airbyte_extracted_at DESC")
	assert.Contains(t, sql, "row_number = 1")
	assert.Contains(t, sql, "`proj`.`ds`.`staging`")
}

func TestSelectDedupedSQLNoCursor(t *testing.T) {
	sql := selectDedupedSQL("proj", "ds", "staging", []string{"id"}, "")

	assert.Contains(t, sql, "PARTITION BY `id`")
	assert.Contains(t, sql, "_airbyte_extracted_at DESC")
	// Should not have a cursor column in ORDER BY
	assert.NotContains(t, sql, "NULLS LAST")
}

func TestAlterTableSQL(t *testing.T) {
	diff := schemaDiff{
		added: []columnDef{
			{name: "email", bqType: "STRING"},
		},
		removed: []string{"old_col"},
	}

	stmts := alterTableSQL("proj", "ds", "tbl", diff)
	require.True(t, len(stmts) >= 2)

	// Separate statements for DROP and ADD
	var hasAdd, hasDrop bool
	for _, s := range stmts {
		if strings.Contains(s, "ADD COLUMN `email` STRING") {
			hasAdd = true
		}
		if strings.Contains(s, "DROP COLUMN `old_col`") {
			hasDrop = true
		}
	}
	assert.True(t, hasAdd, "should have ADD COLUMN statement")
	assert.True(t, hasDrop, "should have DROP COLUMN statement")
}

func TestAlterTableSQLTypeChange(t *testing.T) {
	diff := schemaDiff{
		typeChanged: []typeChange{
			{name: "count", oldType: "INT64", newType: "NUMERIC"},
		},
	}

	stmts := alterTableSQL("proj", "ds", "tbl", diff)
	require.NotEmpty(t, stmts)

	// Type change uses temp column pattern: ADD temp, UPDATE SET temp = CAST, RENAME, DROP
	combined := strings.Join(stmts, "\n")
	assert.Contains(t, combined, "ADD COLUMN")
	assert.Contains(t, combined, "CAST")
	assert.Contains(t, combined, "NUMERIC")
}

func TestAlterTableSQLEmpty(t *testing.T) {
	diff := schemaDiff{}
	stmts := alterTableSQL("proj", "ds", "tbl", diff)
	assert.Empty(t, stmts)
}
