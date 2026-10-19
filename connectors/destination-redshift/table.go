package main

import (
	"context"
	"fmt"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func columnDefs(cols []column) []string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = quote(c.ident) + " " + string(c.typ)
	}
	return defs
}

func identList(cols []column) string {
	return qualifiedList("", cols)
}

// qualifiedList renders the columns, each prefixed with alias and a dot
// when alias is set.
func qualifiedList(alias string, cols []column) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quote(c.ident)
		if alias != "" {
			names[i] = alias + "." + names[i]
		}
	}
	return strings.Join(names, ", ")
}

func (p *streamPlan) pkColumns() []column {
	cols := make([]column, len(p.pk))
	for i, idx := range p.pk {
		cols[i] = p.columns[idx]
	}
	return cols
}

// prepareTable makes the table a stream is written to ready: a fresh shadow
// table for overwrite; for append and append_dedup the table is created when
// missing, and a new schema property adds a nullable column. A column whose
// type differs from the catalog's fails the sync. The PRIMARY KEY of a dedup
// table is informational in Redshift; MERGE keeps the key unique.
func prepareTable(ctx context.Context, w warehouse, schema string, p *streamPlan) error {
	cols := p.allColumns()
	if p.mode == synclet.DestinationSyncModeOverwrite {
		shadow := quote(schema, p.shadow)
		if err := w.exec(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
			return fmt.Errorf("drop leftover table %s: %w", shadow, err)
		}
		if err := w.exec(ctx, "CREATE TABLE "+shadow+" ("+strings.Join(columnDefs(cols), ", ")+")"); err != nil {
			return fmt.Errorf("create table %s: %w", shadow, err)
		}
		return nil
	}

	table := quote(schema, p.table)
	existing, err := tableColumns(ctx, w, schema, p.table)
	if err != nil {
		return err
	}
	if existing == nil {
		defs := columnDefs(cols)
		if p.mode == synclet.DestinationSyncModeAppendDedup {
			defs = append(defs, "PRIMARY KEY ("+identList(p.pkColumns())+")")
		}
		if err := w.exec(ctx, "CREATE TABLE IF NOT EXISTS "+table+" ("+strings.Join(defs, ", ")+")"); err != nil {
			return fmt.Errorf("create table %s: %w", table, err)
		}
		return nil
	}
	for _, c := range cols {
		typ, ok := existing[c.ident]
		switch {
		case !ok:
			if err := w.exec(ctx, "ALTER TABLE "+table+" ADD COLUMN "+quote(c.ident)+" "+string(c.typ)); err != nil {
				return fmt.Errorf("add column %q to table %s: %w", c.ident, table, err)
			}
		case typ != string(c.typ):
			return fmt.Errorf("column %q of table %s is %s, the catalog needs %s; changing a column type is not supported", c.ident, table, typ, c.typ)
		}
	}
	return nil
}

// tableColumns returns the columns of a table and their types, or nil when
// the table does not exist.
func tableColumns(ctx context.Context, w warehouse, schema, table string) (map[string]string, error) {
	rows, err := w.query(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod)
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = `+literal(schema)+` AND c.relname = `+literal(table)+` AND a.attnum > 0 AND NOT a.attisdropped`)
	if err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", quote(schema, table), err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	cols := map[string]string{}
	for _, r := range rows {
		if len(r) != 2 {
			return nil, fmt.Errorf("read columns of table %s: got %d values in a row, expected 2", quote(schema, table), len(r))
		}
		cols[fmt.Sprint(r[0])] = fmt.Sprint(r[1])
	}
	return cols, nil
}

// dedupSQL merges a staged batch into an append_dedup table. The batch is
// first reduced to one row per primary key, the one with the highest
// cursor and the latest arrival among equal cursors, leaving out rows whose
// cursor is lower than the stored row's; Redshift's MERGE fails when two
// source rows match one target row and has no conditional WHEN MATCHED.
func dedupSQL(schema string, p *streamPlan, stage string) []string {
	dedup := quote(fmt.Sprintf("_synclet_dedup_%d", p.index))
	target := quote(schema, p.table)
	all := p.allColumns()
	cols := identList(all)
	pk := p.pkColumns()

	order := ""
	if p.cursor >= 0 {
		order = quote(p.columns[p.cursor].ident) + " DESC NULLS LAST, "
	}
	order += quote(colSeq) + " DESC"
	sel := "CREATE TEMP TABLE " + dedup + " AS SELECT " + cols + " FROM (SELECT " + quote(colSeq) + ", " + cols +
		", ROW_NUMBER() OVER (PARTITION BY " + identList(pk) + " ORDER BY " + order + ") AS " + quote(colRowNumber) +
		" FROM " + stage + ") AS src WHERE src." + quote(colRowNumber) + " = 1"
	if p.cursor >= 0 {
		cur := quote(p.columns[p.cursor].ident)
		match := make([]string, len(pk))
		for i, c := range pk {
			match[i] = "tgt." + quote(c.ident) + " = src." + quote(c.ident)
		}
		sel += " AND NOT EXISTS (SELECT 1 FROM " + target + " AS tgt WHERE " + strings.Join(match, " AND ") +
			" AND tgt." + cur + " IS NOT NULL AND (src." + cur + " IS NULL OR src." + cur + " < tgt." + cur + "))"
	}

	on := make([]string, len(pk))
	for i, c := range pk {
		on[i] = target + "." + quote(c.ident) + " = src." + quote(c.ident)
	}
	var set []string
	for _, c := range all {
		if isPK(p, c) {
			continue
		}
		set = append(set, quote(c.ident)+" = src."+quote(c.ident))
	}
	merge := "MERGE INTO " + target + " USING " + dedup + " AS src ON " + strings.Join(on, " AND ") +
		" WHEN MATCHED THEN UPDATE SET " + strings.Join(set, ", ") +
		" WHEN NOT MATCHED THEN INSERT (" + cols + ") VALUES (" + qualifiedList("src", all) + ")"
	return []string{"DROP TABLE IF EXISTS " + dedup, sel, merge}
}

func isPK(p *streamPlan, c column) bool {
	for _, i := range p.pk {
		if p.columns[i].ident == c.ident {
			return true
		}
	}
	return false
}

// swapTables replaces the table of every overwrite stream with its shadow
// table in one transaction.
func swapTables(ctx context.Context, w warehouse, schema string, plans []*streamPlan) (rerr error) {
	if err := w.exec(ctx, "BEGIN"); err != nil {
		return fmt.Errorf("begin swap: %w", err)
	}
	defer func() {
		if rerr != nil {
			_ = w.exec(context.WithoutCancel(ctx), "ROLLBACK")
		}
	}()
	for _, p := range plans {
		if p.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		table := quote(schema, p.table)
		if err := w.exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return fmt.Errorf("drop table %s: %w", table, err)
		}
		if err := w.exec(ctx, "ALTER TABLE "+quote(schema, p.shadow)+" RENAME TO "+quote(p.table)); err != nil {
			return fmt.Errorf("rename %s to %s: %w", quote(schema, p.shadow), table, err)
		}
	}
	if err := w.exec(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit swap: %w", err)
	}
	return nil
}
