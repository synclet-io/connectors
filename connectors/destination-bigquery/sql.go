package main

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
)

// createTableSQL generates a CREATE TABLE statement with DAY partitioning on
// _airbyte_extracted_at and clustering on PK columns (D-07).
func createTableSQL(projectID, dataset, table string, columns []columnDef, pkColumns []string, columnTypes map[string]bigquery.FieldType) string {
	var colDefs []string

	// Metadata columns first.
	colDefs = append(colDefs,
		"_airbyte_raw_id STRING NOT NULL",
		"_airbyte_extracted_at TIMESTAMP NOT NULL",
		"_airbyte_meta JSON NOT NULL",
		"_airbyte_generation_id INT64",
	)

	// User columns.
	for _, col := range columns {
		colDefs = append(colDefs, fmt.Sprintf("`%s` %s", col.name, fieldTypeToString(col.bqType)))
	}

	clusterCols := clusteringColumns(pkColumns, columnTypes)
	clusterList := strings.Join(wrapBacktick(clusterCols), ", ")

	return fmt.Sprintf(
		"CREATE TABLE `%s`.`%s`.`%s` (\n  %s\n)\nPARTITION BY (DATE_TRUNC(_airbyte_extracted_at, DAY))\nCLUSTER BY %s",
		projectID, dataset, table,
		strings.Join(colDefs, ",\n  "),
		clusterList,
	)
}

// generateMergeSQL generates a MERGE statement for append_dedup mode (D-03)
// with CDC hard delete support (D-10) and soft delete (D-11).
func generateMergeSQL(projectID, targetDataset, targetTable, sourceDataset, sourceTable string,
	columns []string, pkColumns []string, cursorColumn string, cdcHardDelete bool) string {

	// Source subquery: deduplicated records from staging table.
	sourceQuery := selectDedupedSQL(projectID, sourceDataset, sourceTable, pkColumns, cursorColumn)

	// PK matching: NULL-safe comparison.
	pkConditions := make([]string, len(pkColumns))
	for i, pk := range pkColumns {
		pkConditions[i] = fmt.Sprintf(
			"(target.`%s` = source.`%s` OR (target.`%s` IS NULL AND source.`%s` IS NULL))",
			pk, pk, pk, pk,
		)
	}
	pkMatch := strings.Join(pkConditions, "\n    AND ")

	// Cursor comparison: determine if source is newer than target.
	cursorComparison := buildCursorComparison(cursorColumn)

	// All columns for UPDATE SET and INSERT.
	allCols := make([]string, 0, len(columns)+4)
	allCols = append(allCols, columns...)

	metaCols := []string{"_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta", "_airbyte_generation_id"}
	allCols = append(allCols, metaCols...)

	// UPDATE SET assignments.
	var updateAssignments []string
	for _, col := range allCols {
		updateAssignments = append(updateAssignments, fmt.Sprintf("`%s` = source.`%s`", col, col))
	}

	// INSERT columns and values.
	insertCols := strings.Join(wrapBacktick(allCols), ", ")
	insertVals := make([]string, len(allCols))
	for i, col := range allCols {
		insertVals[i] = fmt.Sprintf("source.`%s`", col)
	}
	insertValues := strings.Join(insertVals, ", ")

	// CDC clauses.
	cdcDeleteClause := ""
	cdcSkipInsert := ""
	if cdcHardDelete {
		cdcDeleteClause = fmt.Sprintf(
			"\nWHEN MATCHED AND source._ab_cdc_deleted_at IS NOT NULL AND %s THEN DELETE",
			cursorComparison,
		)
		cdcSkipInsert = " AND source._ab_cdc_deleted_at IS NULL"
	}

	return fmt.Sprintf(`MERGE `+"`%s`.`%s`.`%s`"+` AS target
USING (%s) AS source
ON %s%s
WHEN MATCHED AND %s THEN UPDATE SET
  %s
WHEN NOT MATCHED%s THEN INSERT (%s) VALUES (%s)`,
		projectID, targetDataset, targetTable,
		sourceQuery,
		pkMatch,
		cdcDeleteClause,
		cursorComparison,
		strings.Join(updateAssignments, ",\n  "),
		cdcSkipInsert,
		insertCols,
		insertValues,
	)
}

// buildCursorComparison builds the cursor comparison expression for MERGE.
func buildCursorComparison(cursorColumn string) string {
	if cursorColumn == "" {
		return "target._airbyte_extracted_at < source._airbyte_extracted_at"
	}

	return fmt.Sprintf(`(
      target.`+"`%[1]s`"+` < source.`+"`%[1]s`"+`
      OR (target.`+"`%[1]s`"+` = source.`+"`%[1]s`"+` AND target._airbyte_extracted_at < source._airbyte_extracted_at)
      OR (target.`+"`%[1]s`"+` IS NULL AND source.`+"`%[1]s`"+` IS NULL AND target._airbyte_extracted_at < source._airbyte_extracted_at)
      OR (target.`+"`%[1]s`"+` IS NULL AND source.`+"`%[1]s`"+` IS NOT NULL)
    )`, cursorColumn)
}

// selectDedupedSQL generates a deduplication subquery using ROW_NUMBER()
// partitioned by PK columns, ordered by cursor (if any) and _airbyte_extracted_at.
func selectDedupedSQL(projectID, dataset, table string, pkColumns []string, cursorColumn string) string {
	pkList := strings.Join(wrapBacktick(pkColumns), ", ")

	cursorOrder := ""
	if cursorColumn != "" {
		cursorOrder = fmt.Sprintf("`%s` DESC NULLS LAST, ", cursorColumn)
	}

	return fmt.Sprintf(`SELECT * EXCEPT(row_number) FROM (
    SELECT *, ROW_NUMBER() OVER (
      PARTITION BY %s ORDER BY %s_airbyte_extracted_at DESC
    ) AS row_number
    FROM `+"`%s`.`%s`.`%s`"+`
  ) WHERE row_number = 1`,
		pkList, cursorOrder,
		projectID, dataset, table,
	)
}

// alterTableSQL generates ALTER TABLE statements for schema evolution (D-08).
// Returns separate statements for DROP, ADD, and type changes (Pitfall 4).
func alterTableSQL(projectID, dataset, table string, diff schemaDiff) []string {
	if len(diff.added) == 0 && len(diff.removed) == 0 && len(diff.typeChanged) == 0 {
		return nil
	}

	fqTable := fmt.Sprintf("`%s`.`%s`.`%s`", projectID, dataset, table)
	var stmts []string

	// DROP columns (separate statements per Pitfall 4).
	for _, col := range diff.removed {
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s DROP COLUMN `%s`", fqTable, col))
	}

	// ADD columns.
	for _, col := range diff.added {
		stmts = append(stmts, fmt.Sprintf("ALTER TABLE %s ADD COLUMN `%s` %s", fqTable, col.name, fieldTypeToString(col.bqType)))
	}

	// Type changes via temp column rename pattern.
	for _, tc := range diff.typeChanged {
		tempCol := tempColumnName(tc.name)
		stmts = append(stmts,
			// Step 1: Add temp column with new type.
			fmt.Sprintf("ALTER TABLE %s ADD COLUMN `%s` %s", fqTable, tempCol, tc.newType),
			// Step 2: Copy data with CAST.
			fmt.Sprintf("UPDATE %s SET `%s` = CAST(`%s` AS %s) WHERE 1=1", fqTable, tempCol, tc.name, tc.newType),
			// Step 3: Drop old column.
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN `%s`", fqTable, tc.name),
			// Step 4: Rename temp to original.
			fmt.Sprintf("ALTER TABLE %s RENAME COLUMN `%s` TO `%s`", fqTable, tempCol, tc.name),
		)
	}

	return stmts
}

// tempColumnName generates a deterministic temp column name using SHA256 hash prefix.
func tempColumnName(col string) string {
	h := sha256.Sum256([]byte(col))
	return fmt.Sprintf("_airbyte_tmp_%x_%s", h[:4], col)
}
