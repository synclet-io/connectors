package main

import (
	"context"
	"database/sql"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// stageTable is the session's staging table of the stream at index.
func stageTable(index int) string { return fmt.Sprintf("_SYNCLET_STAGE_%d", index) }

// prepareTable makes a stream's target table ready, and its staging table.
// An overwrite stream gets a fresh shadow table. An append or append_dedup
// table is created when missing; an existing one gets the columns it lacks,
// and a column of another type fails the sync.
func (c *conn) prepareTable(ctx context.Context, p *streamPlan) error {
	if p.mode == synclet.DestinationSyncModeOverwrite {
		if err := c.prepareShadow(ctx, p); err != nil {
			return err
		}
	} else if err := c.prepareExisting(ctx, p); err != nil {
		return err
	}
	if err := c.exec(ctx, "CREATE OR REPLACE TEMPORARY TABLE "+c.tableRef(stageTable(p.index))+" "+stageDDL(p)); err != nil {
		return c.createError(stageTable(p.index), err)
	}
	return nil
}

// prepareShadow creates the shadow table of an overwrite stream. When the
// target exists, the shadow is created LIKE it with COPY GRANTS, so the
// table swapped in keeps the target's grants, and its columns are then
// altered to the catalog's.
func (c *conn) prepareShadow(ctx context.Context, p *streamPlan) error {
	shadow := c.tableRef(p.shadow)
	existing, err := c.tableColumns(ctx, p.table)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		if err := c.exec(ctx, "CREATE OR REPLACE TABLE "+shadow+" "+tableDDL(p)); err != nil {
			return c.createError(p.shadow, err)
		}
		return nil
	}
	if err := c.exec(ctx, "CREATE OR REPLACE TABLE "+shadow+" LIKE "+c.tableRef(p.table)+" COPY GRANTS"); err != nil {
		return c.createError(p.shadow, err)
	}
	for _, q := range shadowAlters(shadow, existing, p.allColumns()) {
		if err := c.exec(ctx, q); err != nil {
			return fmt.Errorf("alter %s to the catalog's columns: %w", shadow, err)
		}
	}
	return nil
}

// shadowAlters turns a table of the existing columns into one of cols:
// new columns are added first, so a table never loses its last column,
// then columns the catalog lacks or types differently are dropped, and the
// retyped ones added back.
func shadowAlters(table string, existing []existingColumn, cols []column) []string {
	have := make(map[string]colType, len(existing))
	for _, e := range existing {
		have[e.name] = e.typ
	}
	want := make(map[string]colType, len(cols))
	for _, col := range cols {
		want[col.ident] = col.typ
	}
	var out, retyped []string
	for _, col := range cols {
		if _, ok := have[col.ident]; !ok {
			out = append(out, "ALTER TABLE "+table+" ADD COLUMN "+ident(col.ident)+" "+string(col.typ))
		}
	}
	for _, e := range existing {
		typ, ok := want[e.name]
		if ok && typ == e.typ {
			continue
		}
		out = append(out, "ALTER TABLE "+table+" DROP COLUMN "+ident(e.name))
		if ok {
			retyped = append(retyped, "ALTER TABLE "+table+" ADD COLUMN "+ident(e.name)+" "+string(typ))
		}
	}
	return append(out, retyped...)
}

func (c *conn) prepareExisting(ctx context.Context, p *streamPlan) error {
	ref := c.tableRef(p.table)
	existing, err := c.tableColumns(ctx, p.table)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		if err := c.exec(ctx, "CREATE TABLE IF NOT EXISTS "+ref+" "+tableDDL(p)); err != nil {
			return c.createError(p.table, err)
		}
		return nil
	}
	types := make(map[string]colType, len(existing))
	for _, e := range existing {
		types[e.name] = e.typ
	}
	for _, col := range p.allColumns() {
		typ, ok := types[col.ident]
		if !ok {
			if err := c.exec(ctx, "ALTER TABLE "+ref+" ADD COLUMN "+ident(col.ident)+" "+string(col.typ)); err != nil {
				return fmt.Errorf("add column %q to %s: %w", col.ident, ref, err)
			}
			continue
		}
		if typ != col.typ {
			return fmt.Errorf("column %q of %s is %s, the catalog maps it to %s", col.ident, ref, typ, col.typ)
		}
	}
	return nil
}

// existingColumn is a column of a table as INFORMATION_SCHEMA describes it.
type existingColumn struct {
	name string
	typ  colType
}

// tableColumns returns the columns of a table of the configured schema in
// table order; none when the table does not exist.
func (c *conn) tableColumns(ctx context.Context, table string) ([]existingColumn, error) {
	q := `SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM ` + ident(c.database) +
		`.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ` + sqlString(c.schema) + ` AND TABLE_NAME = ` + sqlString(table) +
		` ORDER BY ORDINAL_POSITION`
	rows, err := c.c.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("read columns of %s: %w", c.tableRef(table), c.sessionError(err))
	}
	defer func() { _ = rows.Close() }()
	var out []existingColumn
	for rows.Next() {
		var name, dataType string
		var precision, scale sql.NullInt64
		if err := rows.Scan(&name, &dataType, &precision, &scale); err != nil {
			return nil, fmt.Errorf("read columns of %s: %w", c.tableRef(table), err)
		}
		out = append(out, existingColumn{name: name, typ: infoSchemaType(dataType, precision.Int64, scale.Int64)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read columns of %s: %w", c.tableRef(table), err)
	}
	return out, nil
}

// swapTables replaces the table of every overwrite stream with its shadow
// table and drops the old rows.
func (c *conn) swapTables(ctx context.Context, plans []*streamPlan) error {
	for _, p := range plans {
		if p.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		table, shadow := c.tableRef(p.table), c.tableRef(p.shadow)
		if err := c.exec(ctx, "CREATE TABLE IF NOT EXISTS "+table+" "+tableDDL(p)); err != nil {
			return fmt.Errorf("stream %q: %w", p.desc.Name, c.createError(p.table, err))
		}
		if err := c.exec(ctx, "ALTER TABLE "+shadow+" SWAP WITH "+table); err != nil {
			return fmt.Errorf("stream %q: swap %s with %s: %w", p.desc.Name, shadow, table, err)
		}
		if err := c.exec(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
			return fmt.Errorf("stream %q: drop the replaced rows in %s: %w", p.desc.Name, shadow, err)
		}
	}
	return nil
}
