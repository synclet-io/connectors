package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

type column struct {
	name string
	typ  colType
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

// inSchemas is a condition on the schema name expression col and the
// arguments it binds from the first placeholder number first on.
func inSchemas(col string, schemas []string, first int) (string, []any) {
	marks := make([]string, len(schemas))
	args := make([]any, len(schemas))
	for i, s := range schemas {
		marks[i] = fmt.Sprintf("@p%d", first+i)
		args[i] = s
	}
	return col + " IN (" + strings.Join(marks, ", ") + ")", args
}

// loadTables returns the tables and views of schemas with the columns the
// user may SELECT, through an object, schema or database grant or a column
// grant, ordered by schema and name. A table or view without such a column
// is left out. A non-empty name keeps only the ones so named. Alias types
// map as their base type.
func loadTables(ctx context.Context, conn *sql.DB, schemas []string, name string) ([]*table, error) {
	cond, args := inSchemas("s.name", schemas, 2)
	args = append([]any{name}, args...)
	rows, err := conn.QueryContext(ctx, `
SELECT s.name, o.name, c.name,
       CASE WHEN t.is_user_defined = 1 AND t.is_assembly_type = 0 THEN TYPE_NAME(c.system_type_id) ELSE t.name END,
       c.max_length, c.precision, c.scale
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.columns c ON c.object_id = o.object_id
JOIN sys.types t ON t.user_type_id = c.user_type_id
WHERE o.type IN ('U', 'V')
  AND o.is_ms_shipped = 0
  AND `+cond+`
  AND (@p1 = N'' OR o.name = @p1)
  AND HAS_PERMS_BY_NAME(QUOTENAME(s.name) + N'.' + QUOTENAME(o.name), N'OBJECT', N'SELECT', QUOTENAME(c.name), N'COLUMN') = 1
ORDER BY s.name, o.name, c.column_id`, args...)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var tables []*table
	byName := map[[2]string]*table{}
	for rows.Next() {
		var schema, obj, col, typ string
		var maxLength, precision, scale int
		if err := rows.Scan(&schema, &obj, &col, &typ, &maxLength, &precision, &scale); err != nil {
			return nil, fmt.Errorf("list tables: %w", err)
		}
		key := [2]string{schema, obj}
		t := byName[key]
		if t == nil {
			t = &table{schema: schema, name: obj}
			byName[key] = t
			tables = append(tables, t)
		}
		t.columns = append(t.columns, column{name: col, typ: colType{name: typ, maxLength: maxLength, precision: precision, scale: scale}})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	if err := loadPrimaryKeys(ctx, conn, schemas, name, byName); err != nil {
		return nil, err
	}
	return tables, nil
}

func loadPrimaryKeys(ctx context.Context, conn *sql.DB, schemas []string, name string, tables map[[2]string]*table) error {
	cond, args := inSchemas("s.name", schemas, 2)
	args = append([]any{name}, args...)
	rows, err := conn.QueryContext(ctx, `
SELECT s.name, o.name, c.name
FROM sys.indexes i
JOIN sys.objects o ON o.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.key_ordinal > 0
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE i.is_primary_key = 1
  AND `+cond+`
  AND (@p1 = N'' OR o.name = @p1)
ORDER BY s.name, o.name, ic.key_ordinal`, args...)
	if err != nil {
		return fmt.Errorf("list primary keys: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var schema, obj, col string
		if err := rows.Scan(&schema, &obj, &col); err != nil {
			return fmt.Errorf("list primary keys: %w", err)
		}
		if t := tables[[2]string{schema, obj}]; t != nil {
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
	tables, err := loadTables(ctx, d.DB, cfg.Schemas, "")
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{Streams: make([]synclet.Stream, 0, len(tables))}
	for _, t := range tables {
		cat.Streams = append(cat.Streams, t.stream())
	}
	return cat, nil
}
