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

// table is a table or view and its columns in ordinal order.
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

func (t *table) stream() (synclet.Stream, error) {
	props := make(map[synclet.PropertyName]synclet.PropertySpec, len(t.columns))
	for _, c := range t.columns {
		k, err := c.kind()
		if err != nil {
			return synclet.Stream{}, err
		}
		props[synclet.PropertyName(c.name)] = k.schema()
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
	return s, nil
}

// parseColumn is the column of an INFORMATION_SCHEMA.COLUMNS row whose first
// cells are name, data type, precision and scale.
func parseColumn(cells []string) (column, error) {
	c := column{name: cells[0], dataType: cells[1]}
	for i, dst := range []*sql.NullInt64{&c.precision, &c.scale} {
		v := cells[2+i]
		if v == "" {
			continue
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return column{}, fmt.Errorf("column %q: numeric precision or scale %q", c.name, v)
		}
		*dst = sql.NullInt64{Int64: n, Valid: true}
	}
	return c, nil
}

// Discover returns one stream per table, view and materialized view of the
// configured schemas that the role can see, ordered by schema and name.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	c, err := s.connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer c.close()
	tables, err := c.tables(ctx)
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{Streams: []synclet.Stream{}}
	for _, t := range tables {
		st, err := t.stream()
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", c.tableRef(t.schema, t.name), err)
		}
		cat.Streams = append(cat.Streams, st)
	}
	return cat, nil
}

// tables lists the tables of the configured schemas with their columns and
// primary keys.
func (c *conn) tables(ctx context.Context) ([]*table, error) {
	db := ident(c.database)
	q := `SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, c.DATA_TYPE, c.NUMERIC_PRECISION, c.NUMERIC_SCALE FROM ` + db + `.INFORMATION_SCHEMA.COLUMNS c ` +
		`JOIN ` + db + `.INFORMATION_SCHEMA.TABLES t ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME ` +
		`WHERE c.TABLE_SCHEMA <> 'INFORMATION_SCHEMA' AND t.TABLE_TYPE IN ` + tableTypesSQL
	var args []any
	if len(c.cfg.Schemas) > 0 {
		q += ` AND c.TABLE_SCHEMA IN (` + strings.TrimSuffix(strings.Repeat("?, ", len(c.cfg.Schemas)), ", ") + `)`
		for _, s := range c.cfg.Schemas {
			args = append(args, s)
		}
	}
	q += ` ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION`
	rows, err := c.rows(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list columns of database %q: %w", c.database, err)
	}
	var tables []*table
	for _, r := range rows {
		col, err := parseColumn(r[2:])
		if err != nil {
			return nil, fmt.Errorf("table %s: %w", c.tableRef(r[0], r[1]), err)
		}
		if n := len(tables); n == 0 || tables[n-1].schema != r[0] || tables[n-1].name != r[1] {
			tables = append(tables, &table{schema: r[0], name: r[1]})
		}
		t := tables[len(tables)-1]
		t.columns = append(t.columns, col)
	}
	keys, err := c.primaryKeys(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		pk := keys[[2]string{t.schema, t.name}]
		if slices.ContainsFunc(pk, func(name string) bool { _, ok := t.column(name); return !ok }) {
			continue
		}
		t.primaryKey = pk
	}
	return tables, nil
}

// primaryKeys returns the declared primary key columns of every table of
// the database, by schema and table, in key order.
func (c *conn) primaryKeys(ctx context.Context) (map[[2]string][]string, error) {
	recs, err := c.records(ctx, `SHOW PRIMARY KEYS IN DATABASE `+ident(c.database))
	if err != nil {
		return nil, fmt.Errorf("list primary keys of database %q: %w", c.database, err)
	}
	type keyColumn struct {
		name string
		seq  int
	}
	byTable := map[[2]string][]keyColumn{}
	for _, r := range recs {
		seq, err := strconv.Atoi(r["key_sequence"])
		if err != nil {
			return nil, fmt.Errorf("primary key of %s: key_sequence %q", c.tableRef(r["schema_name"], r["table_name"]), r["key_sequence"])
		}
		k := [2]string{r["schema_name"], r["table_name"]}
		byTable[k] = append(byTable[k], keyColumn{r["column_name"], seq})
	}
	out := make(map[[2]string][]string, len(byTable))
	for k, cols := range byTable {
		slices.SortFunc(cols, func(a, b keyColumn) int { return a.seq - b.seq })
		for _, c := range cols {
			out[k] = append(out[k], c.name)
		}
	}
	return out, nil
}
