package main

import (
	"fmt"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// csvFormat is how COPY INTO reads a staged batch: every value quoted, an
// empty unquoted field is NULL, and no text is read as NULL.
const csvFormat = `FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' EMPTY_FIELD_AS_NULL = TRUE NULL_IF = () ESCAPE_UNENCLOSED_FIELD = NONE COMPRESSION = GZIP ENCODING = 'UTF8') ON_ERROR = ABORT_STATEMENT PURGE = TRUE`

func columnDefs(cols []column) []string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = ident(c.ident) + " " + string(c.typ)
	}
	return defs
}

func identList(cols []column) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = ident(c.ident)
	}
	return strings.Join(names, ", ")
}

// tableDDL is the column list of a stream's table, with an informational
// primary key for append_dedup.
func tableDDL(p *streamPlan) string {
	defs := columnDefs(p.allColumns())
	if p.mode == synclet.DestinationSyncModeAppendDedup {
		defs = append(defs, "PRIMARY KEY ("+identList(p.pkColumns())+")")
	}
	return "(" + strings.Join(defs, ", ") + ")"
}

// stageDDL is the column list of a stream's staging table: the batch order
// and every column as text.
func stageDDL(p *streamPlan) string {
	defs := []string{ident(colSeq) + " NUMBER(38,0)"}
	for _, c := range p.allColumns() {
		defs = append(defs, ident(c.ident)+" VARCHAR")
	}
	return "(" + strings.Join(defs, ", ") + ")"
}

// copySQL loads one file of a stage path into the staging table.
func copySQL(stage, from, file string) string {
	return "COPY INTO " + stage + " FROM " + from + " FILES = (" + sqlString(file) + ") " + csvFormat
}

// castList converts each staged column to its type, aliased to its name
// when the expression is not the bare column.
func castList(cols []column, alias bool) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		col := ident(c.ident)
		out[i] = castSQL(c.typ, col)
		if alias && out[i] != col {
			out[i] += " AS " + col
		}
	}
	return strings.Join(out, ", ")
}

// insertSQL moves a staged batch into target in arrival order.
func insertSQL(target string, p *streamPlan, stage string) string {
	cols := p.allColumns()
	return "INSERT INTO " + target + " (" + identList(cols) + ") SELECT " + castList(cols, false) +
		" FROM " + stage + " ORDER BY " + ident(colSeq)
}

// mergeSQL merges a staged batch into an append_dedup table. The batch is
// reduced to one row per primary key, the one with the highest cursor and
// the latest arrival among equal cursors; a stored row is replaced only when
// the incoming cursor is not lower.
func mergeSQL(target string, p *streamPlan, stage string) string {
	cols := p.allColumns()
	pk := p.pkColumns()
	order := ""
	if p.cursor >= 0 {
		order = ident(p.columns[p.cursor].ident) + " DESC NULLS LAST, "
	}
	order += ident(colSeq) + " DESC"
	var on, set, values []string
	for _, c := range pk {
		on = append(on, "t."+ident(c.ident)+" = s."+ident(c.ident))
	}
	for _, c := range cols {
		values = append(values, "s."+ident(c.ident))
		if slices.ContainsFunc(pk, func(k column) bool { return k.ident == c.ident }) {
			continue
		}
		set = append(set, "t."+ident(c.ident)+" = s."+ident(c.ident))
	}
	matched := "WHEN MATCHED"
	if p.cursor >= 0 {
		cur := ident(p.columns[p.cursor].ident)
		matched += fmt.Sprintf(" AND (t.%s IS NULL OR s.%s >= t.%s)", cur, cur, cur)
	}
	return "MERGE INTO " + target + " AS t USING (SELECT * FROM (SELECT " + ident(colSeq) + ", " + castList(cols, true) + " FROM " + stage + ")" +
		" QUALIFY ROW_NUMBER() OVER (PARTITION BY " + identList(pk) + " ORDER BY " + order + ") = 1) AS s" +
		" ON " + strings.Join(on, " AND ") +
		" " + matched + " THEN UPDATE SET " + strings.Join(set, ", ") +
		" WHEN NOT MATCHED THEN INSERT (" + identList(cols) + ") VALUES (" + strings.Join(values, ", ") + ")"
}
