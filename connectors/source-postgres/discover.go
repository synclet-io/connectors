package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

type column struct {
	name    string
	typ     colType
	sqlType string
}

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

// loadTables returns the tables, partitioned tables, views, materialized
// views and foreign tables of schemas the user may SELECT from, ordered by
// schema and name. A non-empty name keeps only the relations so named.
func loadTables(ctx context.Context, conn *pgx.Conn, schemas []string, name string) ([]*table, error) {
	rows, err := conn.Query(ctx, `
SELECT n.nspname, c.relname, a.attname, a.atttypid, format_type(a.atttypid, NULL)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
  AND NOT c.relispartition
  AND n.nspname = ANY($1::text[])
  AND ($2::text = '' OR c.relname = $2::text)
  AND has_table_privilege(c.oid, 'SELECT')
ORDER BY n.nspname, c.relname, a.attnum`, schemas, name)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	var tables []*table
	byName := map[[2]string]*table{}
	var oids []uint32
	for rows.Next() {
		var schema, rel, col, sqlType string
		var oid uint32
		if err := rows.Scan(&schema, &rel, &col, &oid, &sqlType); err != nil {
			rows.Close()
			return nil, fmt.Errorf("list tables: %w", err)
		}
		key := [2]string{schema, rel}
		t := byName[key]
		if t == nil {
			t = &table{schema: schema, name: rel}
			byName[key] = t
			tables = append(tables, t)
		}
		t.columns = append(t.columns, column{name: col, sqlType: sqlType})
		oids = append(oids, oid)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}

	types := newTypeResolver(conn)
	if err := types.load(ctx, oids); err != nil {
		return nil, err
	}
	i := 0
	for _, t := range tables {
		for j := range t.columns {
			t.columns[j].typ = types.resolve(oids[i])
			i++
		}
	}

	if err := loadPrimaryKeys(ctx, conn, schemas, name, byName); err != nil {
		return nil, err
	}
	return tables, nil
}

func loadPrimaryKeys(ctx context.Context, conn *pgx.Conn, schemas []string, name string, tables map[[2]string]*table) error {
	rows, err := conn.Query(ctx, `
SELECT n.nspname, c.relname, a.attname
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class c ON c.oid = i.indrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid AND a.attnum = k.attnum
WHERE i.indisprimary
  AND n.nspname = ANY($1::text[])
  AND ($2::text = '' OR c.relname = $2::text)
ORDER BY n.nspname, c.relname, k.ord`, schemas, name)
	if err != nil {
		return fmt.Errorf("list primary keys: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var schema, rel, col string
		if err := rows.Scan(&schema, &rel, &col); err != nil {
			return fmt.Errorf("list primary keys: %w", err)
		}
		if t := tables[[2]string{schema, rel}]; t != nil {
			t.primaryKey = append(t.primaryKey, col)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list primary keys: %w", err)
	}
	return nil
}

func (t *table) stream() synclet.Stream {
	props := make(map[synclet.PropertyName]synclet.PropertySpec, len(t.columns))
	for _, c := range t.columns {
		props[synclet.PropertyName(c.name)] = c.typ.schema()
	}
	s := synclet.Stream{
		Name:               t.name,
		Namespace:          t.schema,
		JSONSchema:         synclet.ObjectSchema(props),
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
	}
	for _, c := range t.primaryKey {
		s.SourceDefinedPrimaryKey = append(s.SourceDefinedPrimaryKey, []string{c})
	}
	return s
}

// Discover returns one stream per readable table or view of the configured
// schemas.
func (Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	cfg.applyDefaults()
	d, err := connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer d.close()
	tables, err := loadTables(ctx, d.Conn, cfg.Schemas, "")
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{Streams: make([]synclet.Stream, 0, len(tables))}
	for _, t := range tables {
		cat.Streams = append(cat.Streams, t.stream())
	}
	return cat, nil
}
