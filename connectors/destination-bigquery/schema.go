package main

import (
	"sort"
	"strings"

	"cloud.google.com/go/bigquery"
)

// columnDef represents a column for SQL generation and validation.
type columnDef struct {
	name   string
	bqType bigquery.FieldType
}

// schemaDiff represents differences between existing and desired schemas.
type schemaDiff struct {
	added       []columnDef
	removed     []string
	typeChanged []typeChange
}

// typeChange represents a column type widening.
type typeChange struct {
	name    string
	oldType string
	newType string
}

// metadataSchema returns the 4 metadata fields for typed final tables (D-05).
func metadataSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "_airbyte_raw_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "_airbyte_extracted_at", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "_airbyte_meta", Type: bigquery.JSONFieldType, Required: true},
		{Name: "_airbyte_generation_id", Type: bigquery.IntegerFieldType, Required: false},
	}
}

// rawTableSchema returns the 6 fields for legacy raw-table-only mode (D-14).
func rawTableSchema() bigquery.Schema {
	return bigquery.Schema{
		{Name: "_airbyte_raw_id", Type: bigquery.StringFieldType},
		{Name: "_airbyte_extracted_at", Type: bigquery.TimestampFieldType},
		{Name: "_airbyte_loaded_at", Type: bigquery.TimestampFieldType},
		{Name: "_airbyte_data", Type: bigquery.StringFieldType},
		{Name: "_airbyte_meta", Type: bigquery.StringFieldType},
		{Name: "_airbyte_generation_id", Type: bigquery.IntegerFieldType},
	}
}

// buildSchema parses an Airbyte JSON schema and returns a BigQuery schema
// (metadata columns + user columns sorted alphabetically) and column definitions for SQL.
func buildSchema(jsonSchema map[string]interface{}) (bigquery.Schema, []columnDef) {
	props, ok := jsonSchema["properties"].(map[string]interface{})
	if !ok {
		return metadataSchema(), nil
	}

	// Collect user columns sorted alphabetically.
	var names []string
	for name := range props {
		names = append(names, name)
	}
	sort.Strings(names)

	var userFields bigquery.Schema
	var cols []columnDef

	for _, name := range names {
		prop, ok := props[name].(map[string]interface{})
		if !ok {
			continue
		}

		airbyteType, format := extractTypeAndFormat(prop)
		bqType := airbyteTypeToBigQuery(airbyteType, format)

		userFields = append(userFields, &bigquery.FieldSchema{
			Name: name,
			Type: bqType,
		})

		cols = append(cols, columnDef{
			name:   name,
			bqType: bqType,
		})
	}

	schema := append(metadataSchema(), userFields...)
	return schema, cols
}

// extractTypeAndFormat extracts the Airbyte type and format from a property definition.
// Handles both string type ("string") and array type (["string", "null"]).
func extractTypeAndFormat(prop map[string]interface{}) (string, string) {
	var airbyteType string

	switch t := prop["type"].(type) {
	case string:
		airbyteType = t
	case []interface{}:
		// Take first non-null type.
		for _, item := range t {
			s, ok := item.(string)
			if ok && s != "null" {
				airbyteType = s
				break
			}
		}
		if airbyteType == "" && len(t) > 0 {
			if s, ok := t[0].(string); ok {
				airbyteType = s
			}
		}
	}

	format, _ := prop["format"].(string)
	return airbyteType, format
}

// fieldTypeToString converts a BigQuery FieldType to its SQL string representation.
func fieldTypeToString(ft bigquery.FieldType) string {
	switch ft {
	case bigquery.StringFieldType:
		return "STRING"
	case bigquery.IntegerFieldType:
		return "INT64"
	case bigquery.FloatFieldType:
		return "FLOAT64"
	case bigquery.BooleanFieldType:
		return "BOOLEAN"
	case bigquery.TimestampFieldType:
		return "TIMESTAMP"
	case bigquery.DateFieldType:
		return "DATE"
	case bigquery.TimeFieldType:
		return "TIME"
	case bigquery.NumericFieldType:
		return "NUMERIC"
	case bigquery.JSONFieldType:
		return "JSON"
	default:
		return string(ft)
	}
}

// stringToFieldType converts a SQL type string back to bigquery.FieldType.
func stringToFieldType(s string) bigquery.FieldType {
	switch s {
	case "STRING":
		return bigquery.StringFieldType
	case "INT64":
		return bigquery.IntegerFieldType
	case "FLOAT64":
		return bigquery.FloatFieldType
	case "BOOLEAN":
		return bigquery.BooleanFieldType
	case "TIMESTAMP":
		return bigquery.TimestampFieldType
	case "DATE":
		return bigquery.DateFieldType
	case "TIME":
		return bigquery.TimeFieldType
	case "NUMERIC":
		return bigquery.NumericFieldType
	case "JSON":
		return bigquery.JSONFieldType
	default:
		return bigquery.FieldType(s)
	}
}

// diffSchema compares existing table schema with desired schema from current catalog.
// Identifies columns to add, remove, and type changes (only widening conversions).
func diffSchema(existing bigquery.Schema, desired bigquery.Schema) schemaDiff {
	existingMap := make(map[string]bigquery.FieldType)
	for _, f := range existing {
		existingMap[f.Name] = f.Type
	}

	desiredMap := make(map[string]bigquery.FieldType)
	for _, f := range desired {
		desiredMap[f.Name] = f.Type
	}

	var diff schemaDiff

	// Find added and type-changed columns.
	for _, f := range desired {
		existType, exists := existingMap[f.Name]
		if !exists {
			diff.added = append(diff.added, columnDef{
				name:   f.Name,
				bqType: f.Type,
			})
		} else if existType != f.Type && isWideningConversion(existType, f.Type) {
			diff.typeChanged = append(diff.typeChanged, typeChange{
				name:    f.Name,
				oldType: fieldTypeToString(existType),
				newType: fieldTypeToString(f.Type),
			})
		}
	}

	// Find removed columns.
	for _, f := range existing {
		if _, exists := desiredMap[f.Name]; !exists {
			diff.removed = append(diff.removed, f.Name)
		}
	}

	return diff
}

// clusteringColumns returns up to 3 PK columns (excluding JSON-typed) plus
// _airbyte_extracted_at, for a max of 4 total per BigQuery limits (D-07).
func clusteringColumns(pkColumns []string, columnTypes map[string]bigquery.FieldType) []string {
	var cols []string
	for i, pk := range pkColumns {
		if i >= 3 {
			break
		}
		if columnTypes[pk] == bigquery.JSONFieldType {
			continue
		}
		cols = append(cols, pk)
	}
	cols = append(cols, "_airbyte_extracted_at")
	return cols
}

// wrapBacktick wraps each string with backticks for BigQuery SQL identifier quoting.
func wrapBacktick(names []string) []string {
	result := make([]string, len(names))
	for i, n := range names {
		result[i] = "`" + n + "`"
	}
	return result
}

// wrapBacktickStr wraps a single string with backticks.
func wrapBacktickStr(name string) string {
	return "`" + name + "`"
}

// joinBackticked joins names with backtick quoting and the given separator.
func joinBackticked(names []string, sep string) string {
	return strings.Join(wrapBacktick(names), sep)
}
