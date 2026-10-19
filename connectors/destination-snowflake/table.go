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
		if err := c.exec(ctx, "CREATE OR REPLACE TABLE "+c.tableRef(p.shadow)+" "+tableDDL(p)); err != nil {
			return c.createError(p.shadow, err)
		}
	} else if err := c.prepareExisting(ctx, p); err != nil {
		return err
	}
	if err := c.exec(ctx, "CREATE OR REPLACE TEMPORARY TABLE "+c.tableRef(stageTable(p.index))+" "+stageDDL(p)); err != nil {
		return c.createError(stageTable(p.index), err)
	}
	return nil
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
	for _, col := range p.allColumns() {
		typ, ok := existing[col.ident]
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

// tableColumns returns the column types of a table of the configured
// schema; none when the table does not exist.
func (c *conn) tableColumns(ctx context.Context, table string) (map[string]colType, error) {
	q := `SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE FROM ` + ident(c.database) +
		`.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ` + sqlString(c.schema) + ` AND TABLE_NAME = ` + sqlString(table) +
		` ORDER BY ORDINAL_POSITION`
	rows, err := c.c.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("read columns of %s: %w", c.tableRef(table), c.sessionError(err))
	}
	defer func() { _ = rows.Close() }()
	out := map[string]colType{}
	for rows.Next() {
		var name, dataType string
		var precision, scale sql.NullInt64
		if err := rows.Scan(&name, &dataType, &precision, &scale); err != nil {
			return nil, fmt.Errorf("read columns of %s: %w", c.tableRef(table), err)
		}
		out[name] = infoSchemaType(dataType, precision.Int64, scale.Int64)
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
