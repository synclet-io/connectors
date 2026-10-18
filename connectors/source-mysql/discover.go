package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

type table struct {
	schema     string
	name       string
	columns    []column
	primaryKey []string
}

func (t *table) column(name string) (column, bool) {
	for _, c := range t.columns {
		if c.name == name {
			return c, true
		}
	}
	return column{}, false
}

// loadTables returns the tables and views of the configured database that
// the user has a privilege on, with their columns in table order, ordered by
// name. table_patterns keeps the tables matching one of the LIKE patterns; a
// non-empty name keeps only the table so named instead.
func loadTables(ctx context.Context, conn *sql.DB, cfg Config, name string) (_ []*table, rerr error) {
	query := `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?`
	args := []any{cfg.Database}
	switch {
	case name != "":
		query += " AND TABLE_NAME = ?"
		args = append(args, name)
	case len(cfg.TableFilter.TablePatterns) > 0:
		like := make([]string, len(cfg.TableFilter.TablePatterns))
		for i, p := range cfg.TableFilter.TablePatterns {
			like[i] = "TABLE_NAME LIKE ?"
			args = append(args, p)
		}
		query += " AND (" + strings.Join(like, " OR ") + ")"
	}
	query += " ORDER BY TABLE_NAME, ORDINAL_POSITION"

	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))
	var tables []*table
	byName := map[string]*table{}
	for rows.Next() {
		var rel, col, dataType, columnType string
		if err := rows.Scan(&rel, &col, &dataType, &columnType); err != nil {
			return nil, fmt.Errorf("list tables: %w", err)
		}
		if name != "" && rel != name {
			continue
		}
		t := byName[rel]
		if t == nil {
			t = &table{schema: cfg.Database, name: rel}
			byName[rel] = t
			tables = append(tables, t)
		}
		t.columns = append(t.columns, newColumn(col, dataType, columnType))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	if err := loadPrimaryKeys(ctx, conn, cfg.Database, name, byName); err != nil {
		return nil, err
	}
	return tables, nil
}

func loadPrimaryKeys(ctx context.Context, conn *sql.DB, database, name string, tables map[string]*table) (rerr error) {
	rows, err := conn.QueryContext(ctx, `SELECT TABLE_NAME, COLUMN_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ? AND (? = '' OR TABLE_NAME = ?)
ORDER BY TABLE_NAME, ORDINAL_POSITION`, database, name, name)
	if err != nil {
		return fmt.Errorf("list primary keys: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))
	for rows.Next() {
		var rel, col string
		if err := rows.Scan(&rel, &col); err != nil {
			return fmt.Errorf("list primary keys: %w", err)
		}
		if t := tables[rel]; t != nil {
			t.primaryKey = append(t.primaryKey, col)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list primary keys: %w", err)
	}
	for _, t := range tables {
		for _, c := range t.primaryKey {
			if _, ok := t.column(c); !ok {
				t.primaryKey = nil
				break
			}
		}
	}
	return nil
}

func (t *table) stream(cdc bool) synclet.Stream {
	props := make(map[synclet.PropertyName]synclet.PropertySpec, len(t.columns))
	for _, c := range t.columns {
		props[synclet.PropertyName(c.name)] = c.schema()
	}
	s := synclet.Stream{
		Name:                t.name,
		Namespace:           t.schema,
		JSONSchema:          synclet.ObjectSchema(props),
		SupportedSyncModes:  []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
		SourceDefinedCursor: cdc,
	}
	for _, c := range t.primaryKey {
		s.SourceDefinedPrimaryKey = append(s.SourceDefinedPrimaryKey, []string{c})
	}
	return s
}

// Discover returns one stream per readable table or view of the configured
// database.
func (Source) Discover(ctx context.Context, cfg Config) (_ *synclet.Catalog, rerr error) {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(d.close))
	tables, err := loadTables(ctx, d.DB, cfg, "")
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{Streams: make([]synclet.Stream, 0, len(tables))}
	for _, t := range tables {
		cat.Streams = append(cat.Streams, t.stream(cfg.cdc()))
	}
	return cat, nil
}
