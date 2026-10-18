package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// TableInfo holds metadata about a discovered MySQL table.
type TableInfo struct {
	Schema     string
	Name       string
	Columns    []ColumnInfo
	PrimaryKey []string // ordered PK column names
	DataLength int64    // approximate table size in bytes
}

// ColumnInfo holds metadata about a single column.
type ColumnInfo struct {
	Name         string
	DataType     string // MySQL data type (e.g., "varchar", "int", "decimal")
	ColumnType   string // Full column type (e.g., "int unsigned", "decimal(10,2)")
	IsNullable   bool
	OrdinalPos   int
	NumericScale *int // for DECIMAL/NUMERIC
}

func discoverTables(ctx context.Context, db *sql.DB, cfg Config) ([]TableInfo, error) {
	tableMap := make(map[string]*TableInfo)

	// Build column query with optional filters.
	colQuery := `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, COLUMN_TYPE,
		IS_NULLABLE, ORDINAL_POSITION, NUMERIC_SCALE
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?`
	args := []interface{}{cfg.Database}

	if len(cfg.TableFilter.TablePatterns) > 0 {
		placeholders := make([]string, len(cfg.TableFilter.TablePatterns))
		for i, p := range cfg.TableFilter.TablePatterns {
			placeholders[i] = "TABLE_NAME LIKE ?"
			args = append(args, p)
		}
		colQuery += " AND (" + strings.Join(placeholders, " OR ") + ")"
	}

	colQuery += " ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"

	rows, err := db.QueryContext(ctx, colQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			schema, tableName, colName, dataType, colType, isNullable string
			ordinalPos                                                int
			numericScale                                              *int
		)
		if err := rows.Scan(&schema, &tableName, &colName, &dataType, &colType, &isNullable, &ordinalPos, &numericScale); err != nil {
			return nil, fmt.Errorf("scanning column row: %w", err)
		}

		key := schema + "." + tableName
		tbl, ok := tableMap[key]
		if !ok {
			tbl = &TableInfo{Schema: schema, Name: tableName}
			tableMap[key] = tbl
		}
		tbl.Columns = append(tbl.Columns, ColumnInfo{
			Name:         colName,
			DataType:     dataType,
			ColumnType:   colType,
			IsNullable:   strings.EqualFold(isNullable, "YES"),
			OrdinalPos:   ordinalPos,
			NumericScale: numericScale,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating columns: %w", err)
	}

	// Query primary keys.
	pkQuery := `SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ?
		ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION`
	pkRows, err := db.QueryContext(ctx, pkQuery, cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("querying primary keys: %w", err)
	}
	defer pkRows.Close()

	for pkRows.Next() {
		var schema, tableName, colName string
		if err := pkRows.Scan(&schema, &tableName, &colName); err != nil {
			return nil, fmt.Errorf("scanning pk row: %w", err)
		}
		key := schema + "." + tableName
		if tbl, ok := tableMap[key]; ok {
			tbl.PrimaryKey = append(tbl.PrimaryKey, colName)
		}
	}
	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("iterating primary keys: %w", err)
	}

	// Query table sizes.
	sizeQuery := `SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(DATA_LENGTH, 0)
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'`
	sizeRows, err := db.QueryContext(ctx, sizeQuery, cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("querying table sizes: %w", err)
	}
	defer sizeRows.Close()

	for sizeRows.Next() {
		var schema, tableName string
		var dataLength int64
		if err := sizeRows.Scan(&schema, &tableName, &dataLength); err != nil {
			return nil, fmt.Errorf("scanning table size row: %w", err)
		}
		key := schema + "." + tableName
		if tbl, ok := tableMap[key]; ok {
			tbl.DataLength = dataLength
		}
	}
	if err := sizeRows.Err(); err != nil {
		return nil, fmt.Errorf("iterating table sizes: %w", err)
	}

	// Assemble result.
	tables := make([]TableInfo, 0, len(tableMap))
	for _, tbl := range tableMap {
		tables = append(tables, *tbl)
	}
	return tables, nil
}

func buildStreamSchema(table TableInfo) airbyte.Properties {
	props := make(map[airbyte.PropertyName]airbyte.PropertySpec, len(table.Columns))
	for _, col := range table.Columns {
		pt := mapMySQLTypeToAirbyte(col)
		pt.Type = append(pt.Type, airbyte.Null)
		props[airbyte.PropertyName(col.Name)] = airbyte.PropertySpec{
			PropertyType: pt,
		}
	}
	return airbyte.Properties{Properties: props}
}
