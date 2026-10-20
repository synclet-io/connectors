package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// qualified renders schema.table, both quoted.
func qualified(schema, table string) string {
	return quote(schema) + "." + quote(table)
}

func columnDef(c column) string {
	def := quote(c.ident) + " " + c.sqlType
	if isText(c.sqlType) {
		def += " COLLATE " + textCollation
	}
	if c.key {
		def += " NOT NULL"
	}
	return def
}

func columnDefs(cols []column) []string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = columnDef(c)
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

// createDefs are the column definitions of a new table; an append_dedup
// table has a primary key on the stream's key.
func (p *streamPlan) createDefs() []string {
	defs := columnDefs(p.allColumns())
	if p.mode != synclet.DestinationSyncModeAppendDedup {
		return defs
	}
	return append(defs, "PRIMARY KEY NONCLUSTERED ("+identList(p.pkColumns())+")")
}

// createSchemaSQL creates schema @p1, quoted as @p2, when it does not exist.
const createSchemaSQL = `IF SCHEMA_ID(@p1) IS NULL EXEC(N'CREATE SCHEMA ' + @p2)`

// renameSQL renames table @p1, schema-qualified and quoted, to @p2.
const renameSQL = `EXEC sp_rename @objname = @p1, @newname = @p2, @objtype = N'OBJECT'`

// ensureSchema creates the schema when it does not exist.
func ensureSchema(ctx context.Context, conn *sql.Conn, schema string) error {
	_, err := conn.ExecContext(ctx, createSchemaSQL, schema, quote(schema))
	if err != nil {
		return fmt.Errorf("create schema %s: %w", quote(schema), err)
	}
	return nil
}

// prepareTable makes the table a stream is written to ready: a fresh shadow
// table for overwrite; for append and append_dedup the table is created when
// missing, and a new schema property adds a nullable column. A column whose
// type differs from the catalog's, or a dedup table without the primary key
// on the stream's key, fails the sync.
func prepareTable(ctx context.Context, conn *sql.Conn, schema string, p *streamPlan) error {
	if p.mode == synclet.DestinationSyncModeOverwrite {
		shadow := qualified(schema, p.shadow)
		if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
			return fmt.Errorf("drop leftover table %s: %w", shadow, err)
		}
		if _, err := conn.ExecContext(ctx, "CREATE TABLE "+shadow+" ("+strings.Join(p.createDefs(), ", ")+")"); err != nil {
			return fmt.Errorf("create table %s: %w", shadow, err)
		}
		return nil
	}

	table := qualified(schema, p.table)
	existing, err := tableColumns(ctx, conn, schema, p.table)
	if err != nil {
		return err
	}
	if existing == nil {
		if _, err := conn.ExecContext(ctx, "CREATE TABLE "+table+" ("+strings.Join(p.createDefs(), ", ")+")"); err != nil {
			return fmt.Errorf("create table %s: %w", table, err)
		}
		return nil
	}

	for _, c := range p.allColumns() {
		typ, ok := existing[strings.ToLower(c.ident)]
		switch {
		case !ok:
			if _, err := conn.ExecContext(ctx, "ALTER TABLE "+table+" ADD "+columnDef(c)); err != nil {
				return fmt.Errorf("add column %q to table %s: %w", c.ident, table, err)
			}
		case typ != strings.ToLower(c.sqlType):
			return fmt.Errorf("column %q of table %s is %s, the catalog needs %s; changing a column type is not supported", c.ident, table, typ, strings.ToLower(c.sqlType))
		}
	}
	if p.mode == synclet.DestinationSyncModeAppendDedup {
		pk, err := primaryKey(ctx, conn, schema, p.table)
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

// tableColumns returns the columns of a table, by lower-case name, and their
// types as the connector writes them in lower case (nvarchar(max),
// decimal(38,0), datetime2(7)), or nil when the table does not exist.
func tableColumns(ctx context.Context, conn *sql.Conn, schema, table string) (map[string]string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT c.name, t.name, c.max_length, c.precision, c.scale
FROM sys.columns c JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE c.object_id = OBJECT_ID(@p1, N'U')`, qualified(schema, table))
	if err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", qualified(schema, table), err)
	}
	defer func() { _ = rows.Close() }()
	var cols map[string]string
	for rows.Next() {
		var name, typ string
		var maxLen, prec, scale int
		if err := rows.Scan(&name, &typ, &maxLen, &prec, &scale); err != nil {
			return nil, fmt.Errorf("read columns of table %s: %w", qualified(schema, table), err)
		}
		if cols == nil {
			cols = map[string]string{}
		}
		cols[strings.ToLower(name)] = typeName(strings.ToLower(typ), maxLen, prec, scale)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", qualified(schema, table), err)
	}
	return cols, nil
}

// typeName renders a column type of sys.columns the way sqlType spells it.
func typeName(typ string, maxLen, prec, scale int) string {
	switch typ {
	case "nvarchar", "nchar":
		if maxLen < 0 {
			return typ + "(max)"
		}
		return typ + "(" + strconv.Itoa(maxLen/2) + ")"
	case "varchar", "char", "varbinary", "binary":
		if maxLen < 0 {
			return typ + "(max)"
		}
		return typ + "(" + strconv.Itoa(maxLen) + ")"
	case "decimal", "numeric":
		return typ + "(" + strconv.Itoa(prec) + "," + strconv.Itoa(scale) + ")"
	case "float":
		return typ + "(" + strconv.Itoa(prec) + ")"
	case "datetime2", "datetimeoffset", "time":
		return typ + "(" + strconv.Itoa(scale) + ")"
	}
	return typ
}

// primaryKey returns the lower-case columns of a table's primary key.
func primaryKey(ctx context.Context, conn *sql.Conn, schema, table string) ([]string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT c.name
FROM sys.indexes i
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID(@p1, N'U') AND i.is_primary_key = 1`, qualified(schema, table))
	if err != nil {
		return nil, fmt.Errorf("read primary key of table %s: %w", qualified(schema, table), err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("read primary key of table %s: %w", qualified(schema, table), err)
		}
		cols = append(cols, strings.ToLower(name))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read primary key of table %s: %w", qualified(schema, table), err)
	}
	return cols, nil
}

// swapTables replaces the table of every overwrite stream with its shadow
// table in one transaction: readers see either every old table or every new
// one.
func swapTables(ctx context.Context, conn *sql.Conn, schema string, plans []*streamPlan) (rerr error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("swap overwritten tables: %w", err)
	}
	defer func() {
		if rerr != nil {
			_ = tx.Rollback()
		}
	}()
	for _, p := range plans {
		if p.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		table := qualified(schema, p.table)
		if _, err := tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
			return fmt.Errorf("drop replaced table %s: %w", table, err)
		}
		if _, err := tx.ExecContext(ctx, renameSQL, qualified(schema, p.shadow), p.table); err != nil {
			return fmt.Errorf("rename %s to %s: %w", qualified(schema, p.shadow), table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("swap overwritten tables: %w", err)
	}
	return nil
}
