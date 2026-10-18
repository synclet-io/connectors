package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func columnDefs(cols []column) []string {
	defs := make([]string, len(cols))
	for i, c := range cols {
		defs[i] = quote(c.ident) + " " + c.sqlType()
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

// engine is the table engine a stream's table is created with:
// ReplacingMergeTree ordered by the primary key with the cursor as version
// for append_dedup, an unordered MergeTree otherwise.
func (p *streamPlan) engine() string {
	if p.mode != synclet.DestinationSyncModeAppendDedup {
		return "MergeTree ORDER BY tuple()"
	}
	version := ""
	if p.cursor >= 0 {
		version = quote(p.columns[p.cursor].ident)
	}
	return "ReplacingMergeTree(" + version + ") ORDER BY (" + identList(p.pkColumns()) + ")"
}

func createSQL(table string, p *streamPlan) string {
	return "CREATE TABLE " + table + " (" + strings.Join(columnDefs(p.allColumns()), ", ") + ") ENGINE = " + p.engine()
}

// tableInfo is what system.tables and system.columns say about a table.
type tableInfo struct {
	engine     string
	engineFull string
	columns    map[string]string
	sortingKey []string
}

// prepareTable makes the table a stream is written to ready: a fresh shadow
// table for overwrite; for append and append_dedup the table is created when
// missing, and a new schema property adds a Nullable column. A column whose
// type differs from the catalog's, or a dedup table whose engine, sorting key
// or version differs from the stream's, fails the sync.
func prepareTable(ctx context.Context, conn driver.Conn, database string, p *streamPlan) error {
	if p.mode == synclet.DestinationSyncModeOverwrite {
		shadow := quote(database, p.shadow)
		if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+shadow+" SYNC"); err != nil {
			return fmt.Errorf("drop leftover table %s: %w", shadow, err)
		}
		if err := conn.Exec(ctx, createSQL(shadow, p)); err != nil {
			return fmt.Errorf("create table %s: %w", shadow, err)
		}
		return nil
	}

	table := quote(database, p.table)
	info, err := describeTable(ctx, conn, database, p.table)
	if err != nil {
		return err
	}
	if info == nil {
		if err := conn.Exec(ctx, createSQL(table, p)); err != nil {
			return fmt.Errorf("create table %s: %w", table, err)
		}
		return nil
	}

	if p.mode == synclet.DestinationSyncModeAppendDedup {
		if err := checkDedupTable(table, info, p); err != nil {
			return err
		}
	}
	var missing []column
	for _, c := range p.allColumns() {
		typ, ok := info.columns[c.ident]
		switch {
		case !ok:
			missing = append(missing, c)
		case typ != c.sqlType():
			return fmt.Errorf("column %q of table %s is %s, the catalog needs %s; changing a column type is not supported", c.ident, table, typ, c.sqlType())
		}
	}
	for _, c := range missing {
		add := c
		add.nullable = true
		if err := conn.Exec(ctx, "ALTER TABLE "+table+" ADD COLUMN "+quote(add.ident)+" "+add.sqlType()); err != nil {
			return fmt.Errorf("add column %q to table %s: %w", add.ident, table, err)
		}
	}
	return nil
}

// checkDedupTable fails unless an existing table is a ReplacingMergeTree
// sorted by the stream's primary key with the stream's cursor as version.
func checkDedupTable(table string, info *tableInfo, p *streamPlan) error {
	if !strings.HasSuffix(info.engine, "ReplacingMergeTree") {
		return fmt.Errorf("table %s is a %s table; append_dedup needs a ReplacingMergeTree", table, info.engine)
	}
	want := make([]string, len(p.pk))
	for i, c := range p.pkColumns() {
		want[i] = c.ident
	}
	slices.Sort(want)
	if !slices.Equal(info.sortingKey, want) {
		return fmt.Errorf("table %s is sorted by (%s); append_dedup needs it sorted by the primary key (%s)", table, strings.Join(info.sortingKey, ", "), strings.Join(want, ", "))
	}
	wantVersion := ""
	if p.cursor >= 0 {
		wantVersion = p.columns[p.cursor].ident
	}
	if got := replacingVersion(info.engineFull); got != wantVersion {
		return fmt.Errorf("table %s has version column %q; append_dedup needs the cursor %q", table, got, wantVersion)
	}
	return nil
}

// replacingVersion returns the version column in a ReplacingMergeTree's
// engine_full, or "" when it has none.
func replacingVersion(engineFull string) string {
	_, args, ok := strings.Cut(engineFull, "ReplacingMergeTree(")
	if !ok {
		return ""
	}
	args, _, _ = strings.Cut(args, ")")
	parts := strings.Split(args, ",")
	// Replicated engines take the ZooKeeper path and replica name first.
	last := strings.TrimSpace(parts[len(parts)-1])
	if strings.HasPrefix(last, "'") {
		return ""
	}
	return strings.Trim(last, "`")
}

// describeTable returns the engine, columns and sorting key of a table, or
// nil when the table does not exist.
func describeTable(ctx context.Context, conn driver.Conn, database, table string) (*tableInfo, error) {
	info := &tableInfo{columns: map[string]string{}}
	rows, err := conn.Query(ctx, `SELECT engine, engine_full FROM system.tables WHERE database = ? AND name = ?`, database, table)
	if err != nil {
		return nil, fmt.Errorf("look up table %s: %w", quote(database, table), err)
	}
	found := false
	for rows.Next() {
		if err := rows.Scan(&info.engine, &info.engineFull); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("look up table %s: %w", quote(database, table), err)
		}
		found = true
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("look up table %s: %w", quote(database, table), err)
	}
	if !found {
		return nil, nil
	}

	rows, err = conn.Query(ctx, `SELECT name, type, is_in_sorting_key FROM system.columns WHERE database = ? AND table = ? ORDER BY position`, database, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", quote(database, table), err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var name, typ string
		var inKey uint8
		if err := rows.Scan(&name, &typ, &inKey); err != nil {
			return nil, fmt.Errorf("read columns of table %s: %w", quote(database, table), err)
		}
		info.columns[name] = typ
		if inKey != 0 {
			info.sortingKey = append(info.sortingKey, name)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read columns of table %s: %w", quote(database, table), err)
	}
	slices.Sort(info.sortingKey)
	return info, nil
}

// swapTables replaces the table of every overwrite stream with its shadow
// table. Each swap is atomic, EXCHANGE TABLES when the table exists and
// RENAME TABLE when it does not; ClickHouse has no transaction across
// tables, so a failure leaves the streams swapped before it swapped.
func swapTables(ctx context.Context, conn driver.Conn, database string, plans []*streamPlan) error {
	for _, p := range plans {
		if p.mode != synclet.DestinationSyncModeOverwrite {
			continue
		}
		table, shadow := quote(database, p.table), quote(database, p.shadow)
		info, err := describeTable(ctx, conn, database, p.table)
		if err != nil {
			return err
		}
		if info == nil {
			if err := conn.Exec(ctx, "RENAME TABLE "+shadow+" TO "+table); err != nil {
				return fmt.Errorf("rename %s to %s: %w", shadow, table, err)
			}
			continue
		}
		if err := conn.Exec(ctx, "EXCHANGE TABLES "+shadow+" AND "+table); err != nil {
			return fmt.Errorf("exchange %s and %s: %w", shadow, table, err)
		}
		if err := conn.Exec(ctx, "DROP TABLE "+shadow+" SYNC"); err != nil {
			return fmt.Errorf("drop the old table, now %s: %w", shadow, err)
		}
	}
	return nil
}
