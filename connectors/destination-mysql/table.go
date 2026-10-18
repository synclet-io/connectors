package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// collation compares strings by code point and, unlike utf8mb4_bin, does not
// ignore trailing spaces.
const collation = "utf8mb4_0900_bin"

// tableOptions are the options of every table the connector creates.
const tableOptions = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=" + collation

func columnDefs(cols []column) []string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = quote(c.ident) + " " + c.sqlType
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
func prepareTable(ctx context.Context, conn *sql.Conn, p *streamPlan) error {
	cols := p.allColumns()
	if p.mode == synclet.DestinationSyncModeOverwrite {
		for _, t := range []string{p.shadow, p.old} {
			if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quote(t)); err != nil {
				return fmt.Errorf("drop leftover table %s: %w", quote(t), err)
			}
		}
		if _, err := conn.ExecContext(ctx, "CREATE TABLE "+quote(p.shadow)+" ("+strings.Join(columnDefs(cols), ", ")+")"+tableOptions); err != nil {
			return fmt.Errorf("create table %s: %w", quote(p.shadow), err)
		}
		return nil
	}

	table := quote(p.table)
	existing, err := tableColumns(ctx, conn, p.table)
	if err != nil {
		return err
	}
	if existing == nil {
		defs := columnDefs(cols)
		if p.mode == synclet.DestinationSyncModeAppendDedup {
			defs = append(defs, "PRIMARY KEY ("+identList(p.pkColumns())+")")
		}
		if _, err := conn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+table+" ("+strings.Join(defs, ", ")+")"+tableOptions); err != nil {
			return fmt.Errorf("create table %s: %w", table, err)
		}
		return nil
	}

	for _, c := range cols {
		typ, ok := existing[strings.ToLower(c.ident)]
		switch {
		case !ok:
			if _, err := conn.ExecContext(ctx, "ALTER TABLE "+table+" ADD COLUMN "+quote(c.ident)+" "+c.sqlType); err != nil {
				return fmt.Errorf("add column %q to table %s: %w", c.ident, table, err)
			}
		case typ != infoType(c.sqlType):
			return fmt.Errorf("column %q of table %s is %s, the catalog needs %s; changing a column type is not supported", c.ident, table, typ, infoType(c.sqlType))
		}
	}
	if p.mode == synclet.DestinationSyncModeAppendDedup {
		pk, err := primaryKey(ctx, conn, p.table)
		if err != nil {
			return err
		}
		want := make([]string, len(p.pk))
		for i, c := range p.pkColumns() {
			want[i] = strings.ToLower(c.ident)
		}
		slices.Sort(pk)
		slices.Sort(want)
		if !slices.Equal(pk, want) {
			return fmt.Errorf("table %s has no primary key on (%s), which append_dedup needs", table, identList(p.pkColumns()))
		}
	}
	return nil
}

// tableColumns returns the columns of a table of the current database, by
// lower-case name, and their types, or nil when the table does not exist.
func tableColumns(ctx context.Context, conn *sql.Conn, table string) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT COLUMN_NAME, COLUMN_TYPE FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", quote(table), err)
	}
	defer func() { _ = rows.Close() }()
	var cols map[string]string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, fmt.Errorf("read columns of table %s: %w", quote(table), err)
		}
		if cols == nil {
			cols = map[string]string{}
		}
		cols[strings.ToLower(name)] = strings.ToLower(typ)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", quote(table), err)
	}
	return cols, nil
}

// primaryKey returns the lower-case columns of a table's primary key.
func primaryKey(ctx context.Context, conn *sql.Conn, table string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT COLUMN_NAME FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = 'PRIMARY'`, table)
	if err != nil {
		return nil, fmt.Errorf("read primary key of table %s: %w", quote(table), err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read primary key of table %s: %w", quote(table), err)
		}
		cols = append(cols, strings.ToLower(name))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read primary key of table %s: %w", quote(table), err)
	}
	return cols, nil
}

// tableExists reports whether the current database has a table of that name.
func tableExists(ctx context.Context, conn *sql.Conn, table string) (bool, error) {
	var n int
	err := conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, table).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("look up table %s: %w", quote(table), err)
	}
	return n > 0, nil
}

// swapTables replaces the table of every overwrite stream with its shadow
// table in one RENAME TABLE, which MySQL applies atomically, then drops the
// old tables. A failed drop only warns: the new tables are in place, and the
// next sync drops what is left.
func swapTables(ctx context.Context, conn *sql.Conn, plans []*streamPlan) error {
	var renames, olds []string
	for _, p := range plans {
		if p.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		exists, err := tableExists(ctx, conn, p.table)
		if err != nil {
			return err
		}
		if exists {
			renames = append(renames, quote(p.table)+" TO "+quote(p.old))
			olds = append(olds, p.old)
		}
		renames = append(renames, quote(p.shadow)+" TO "+quote(p.table))
	}
	if len(renames) == 0 {
		return nil
	}
	if _, err := conn.ExecContext(ctx, "RENAME TABLE "+strings.Join(renames, ", ")); err != nil {
		return fmt.Errorf("swap overwritten tables: %w", err)
	}
	for _, old := range olds {
		if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quote(old)); err != nil {
			synclet.Logger(ctx).Warn(fmt.Sprintf("drop replaced table %s: %v", quote(old), err))
		}
	}
	return nil
}
