package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
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
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quote(c.ident)
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
// type differs from the catalog's, or a dedup table without the primary key
// on the stream's key, fails the sync.
func prepareTable(ctx context.Context, conn *pgx.Conn, schema string, p *streamPlan) error {
	cols := p.allColumns()
	if p.mode == synclet.DestinationSyncModeOverwrite {
		shadow := quote(schema, p.shadow)
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
			return fmt.Errorf("drop leftover table %s: %w", shadow, err)
		}
		if _, err := conn.Exec(ctx, "CREATE TABLE "+shadow+" ("+strings.Join(columnDefs(cols), ", ")+")"); err != nil {
			return fmt.Errorf("create table %s: %w", shadow, err)
		}
		return nil
	}

	table := quote(schema, p.table)
	existing, err := tableColumns(ctx, conn, table)
	if err != nil {
		return err
	}
	if existing == nil {
		defs := columnDefs(cols)
		if p.mode == synclet.DestinationSyncModeAppendDedup {
			defs = append(defs, "PRIMARY KEY ("+identList(p.pkColumns())+")")
		}
		if _, err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+table+" ("+strings.Join(defs, ", ")+")"); err != nil {
			return fmt.Errorf("create table %s: %w", table, err)
		}
		return nil
	}

	for _, c := range cols {
		typ, ok := existing[c.ident]
		switch {
		case !ok:
			if _, err := conn.Exec(ctx, "ALTER TABLE "+table+" ADD COLUMN "+quote(c.ident)+" "+string(c.typ)); err != nil {
				return fmt.Errorf("add column %q to table %s: %w", c.ident, table, err)
			}
		case typ != string(c.typ):
			return fmt.Errorf("column %q of table %s is %s, the catalog needs %s; changing a column type is not supported", c.ident, table, typ, c.typ)
		}
	}
	if p.mode == synclet.DestinationSyncModeAppendDedup {
		pk, err := primaryKey(ctx, conn, table)
		if err != nil {
			return err
		}
		want := make([]string, len(p.pk))
		for i, c := range p.pkColumns() {
			want[i] = c.ident
		}
		slices.Sort(pk)
		slices.Sort(want)
		if !slices.Equal(pk, want) {
			return fmt.Errorf("table %s has no primary key on (%s), which append_dedup needs", table, identList(p.pkColumns()))
		}
	}
	return nil
}

// tableColumns returns the columns of a table and their types, or nil when
// the table does not exist.
func tableColumns(ctx context.Context, conn *pgx.Conn, table string) (map[string]string, error) {
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT to_regclass($1::text) IS NOT NULL`, table).Scan(&exists); err != nil {
		return nil, fmt.Errorf("look up table %s: %w", table, err)
	}
	if !exists {
		return nil, nil
	}
	rows, err := conn.Query(ctx, `
SELECT a.attname::text, format_type(a.atttypid, a.atttypmod)
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = to_regclass($1::text) AND a.attnum > 0 AND NOT a.attisdropped`, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", table, err)
	}
	cols := map[string]string{}
	var name, typ string
	_, err = pgx.ForEachRow(rows, []any{&name, &typ}, func() error {
		cols[name] = typ
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", table, err)
	}
	return cols, nil
}

// primaryKey returns the columns of a table's primary key.
func primaryKey(ctx context.Context, conn *pgx.Conn, table string) ([]string, error) {
	rows, err := conn.Query(ctx, `
SELECT a.attname::text
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = to_regclass($1::text) AND i.indisprimary`, table)
	if err != nil {
		return nil, fmt.Errorf("read primary key of table %s: %w", table, err)
	}
	cols, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("read primary key of table %s: %w", table, err)
	}
	return cols, nil
}

// swapTables replaces the table of every overwrite stream with its shadow
// table in one transaction.
func swapTables(ctx context.Context, conn *pgx.Conn, schema string, plans []*streamPlan) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin swap: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	for _, p := range plans {
		if p.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		table := quote(schema, p.table)
		if _, err := tx.Exec(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return fmt.Errorf("drop table %s: %w", table, err)
		}
		if _, err := tx.Exec(ctx, "ALTER TABLE "+quote(schema, p.shadow)+" RENAME TO "+quote(p.table)); err != nil {
			return fmt.Errorf("rename %s to %s: %w", quote(schema, p.shadow), table, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit swap: %w", err)
	}
	return nil
}
