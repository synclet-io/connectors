package main

import (
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// tableProps makes every table a Delta table with column mapping, so a column
// may carry any name a schema gives it.
const tableProps = " USING DELTA TBLPROPERTIES ('delta.columnMapping.mode' = 'name')"

// jsonFormat reads a staged file: every value is a JSON string or null, and a
// malformed line fails the COPY instead of loading as nulls.
const jsonFormat = " FILEFORMAT = JSON FORMAT_OPTIONS ('mode' = 'FAILFAST', 'primitivesAsString' = 'true')"

// location is the catalog and schema every table is written to.
type location struct{ catalog, schema string }

// table renders the qualified name of a table of the schema.
func (l location) table(name string) string {
	return ident(l.catalog) + "." + ident(l.schema) + "." + ident(name)
}

func columnDefs(cols []column) string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = ident(c.name) + " " + string(c.typ)
	}
	return strings.Join(defs, ", ")
}

func identList(prefix string, cols []column) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = prefix + ident(c.name)
	}
	return strings.Join(names, ", ")
}

// stageColumns are the columns of a batch: the order column first for a
// stage table, the table's columns alone for a shadow table.
func (p *streamPlan) stageColumns() []column {
	if p.mode == synclet.DestinationSyncModeOverwrite {
		return p.columns
	}
	return append([]column{p.seq}, p.columns...)
}

func createTableSQL(l location, p *streamPlan) string {
	return "CREATE TABLE IF NOT EXISTS " + l.table(p.table) + " (" + columnDefs(p.columns) + ")" + tableProps
}

func createShadowSQL(l location, p *streamPlan) string {
	return "CREATE OR REPLACE TABLE " + l.table(p.shadow) + " (" + columnDefs(p.columns) + ")" + tableProps
}

func createStageSQL(l location, p *streamPlan) string {
	return "CREATE OR REPLACE TABLE " + l.table(p.stage) + " (" + columnDefs(p.stageColumns()) + ")" + tableProps
}

func addColumnsSQL(l location, p *streamPlan, cols []column) string {
	return "ALTER TABLE " + l.table(p.table) + " ADD COLUMNS (" + columnDefs(cols) + ")"
}

// castExpr turns the string a staged file holds for c into c's type.
func castExpr(c column) string {
	switch c.typ {
	case colString:
		return c.key
	case colVariant:
		return "parse_json(" + c.key + ")"
	}
	return "CAST(" + c.key + " AS " + string(c.typ) + ")"
}

// copySQL loads one staged file into the shadow table of an overwrite stream
// or the stage table of another.
func copySQL(l location, p *streamPlan, file string) string {
	into := p.stage
	if p.mode == synclet.DestinationSyncModeOverwrite {
		into = p.shadow
	}
	cols := p.stageColumns()
	sel := make([]string, len(cols))
	for i, c := range cols {
		sel[i] = castExpr(c) + " AS " + ident(c.name)
	}
	return "COPY INTO " + l.table(into) + " FROM (SELECT " + strings.Join(sel, ", ") + " FROM " + sqlString(file) + ")" + jsonFormat
}

// insertSQL appends the stage table's batch to the table.
func insertSQL(l location, p *streamPlan) string {
	cols := identList("", p.columns)
	return "INSERT INTO " + l.table(p.table) + " (" + cols + ") SELECT " + cols + " FROM " + l.table(p.stage)
}

// mergeSQL merges the stage table's batch into the table on the primary key.
// Within the batch the row with the highest cursor wins, the latest arrival
// among equal cursors; a stored row is replaced only when the incoming cursor
// is not lower than its own.
func mergeSQL(l location, p *streamPlan) string {
	pk := make([]column, len(p.pk))
	for i, idx := range p.pk {
		pk[i] = p.columns[idx]
	}
	order := ""
	if p.cursor >= 0 {
		order = ident(p.columns[p.cursor].name) + " DESC NULLS LAST, "
	}
	order += ident(colSeq) + " DESC"
	cols := identList("", p.columns)
	source := "SELECT " + cols + " FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + identList("", pk) +
		" ORDER BY " + order + ") AS " + ident(colRowNum) + " FROM " + l.table(p.stage) + ") WHERE " + ident(colRowNum) + " = 1"

	on := make([]string, len(pk))
	for i, c := range pk {
		on[i] = "t." + ident(c.name) + " = s." + ident(c.name)
	}
	var set []string
	for _, c := range p.columns {
		if slices.ContainsFunc(pk, func(k column) bool { return k.name == c.name }) {
			continue
		}
		set = append(set, "t."+ident(c.name)+" = s."+ident(c.name))
	}
	matched := " WHEN MATCHED"
	if p.cursor >= 0 {
		cur := ident(p.columns[p.cursor].name)
		matched += " AND (t." + cur + " IS NULL OR s." + cur + " >= t." + cur + ")"
	}
	return "MERGE INTO " + l.table(p.table) + " AS t USING (" + source + ") AS s ON " + strings.Join(on, " AND ") +
		matched + " THEN UPDATE SET " + strings.Join(set, ", ") +
		" WHEN NOT MATCHED THEN INSERT (" + cols + ") VALUES (" + identList("s.", p.columns) + ")"
}

// swapSQL replaces the table of an overwrite stream with its shadow table in
// one Delta commit.
func swapSQL(l location, p *streamPlan) string {
	return "CREATE OR REPLACE TABLE " + l.table(p.table) + tableProps + " AS SELECT * FROM " + l.table(p.shadow)
}

func dropSQL(l location, table string) string {
	return "DROP TABLE IF EXISTS " + l.table(table)
}
