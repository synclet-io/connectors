package main

import (
	"strings"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// mapMySQLTypeToAirbyte maps a MySQL column to its Airbyte property type.
func mapMySQLTypeToAirbyte(col ColumnInfo) airbyte.PropertyType {
	dt := strings.ToLower(col.DataType)

	switch dt {
	// Boolean-like types represented as integer (0/1).
	case "boolean", "bool":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Integer}}

	case "tinyint":
		// tinyint(1) is MySQL's boolean representation.
		if strings.Contains(strings.ToLower(col.ColumnType), "(1)") {
			return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Integer}}
		}
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Integer}}

	case "bit":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Integer}}

	// Integer types.
	case "smallint", "mediumint", "int", "integer":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Integer}}

	case "bigint":
		// bigint unsigned can exceed int64 range.
		if strings.Contains(strings.ToLower(col.ColumnType), "unsigned") {
			return airbyte.PropertyType{
				Type:        []airbyte.PropType{airbyte.Integer},
				AirbyteType: airbyte.BigInteger,
			}
		}
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Integer}}

	// Floating-point types.
	case "float":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Number}}

	case "double", "real":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Number}}

	// Decimal/numeric types — depends on scale.
	case "decimal", "numeric":
		if col.NumericScale != nil && *col.NumericScale == 0 {
			return airbyte.PropertyType{
				Type:        []airbyte.PropType{airbyte.Integer},
				AirbyteType: airbyte.BigInteger,
			}
		}
		return airbyte.PropertyType{
			Type:        []airbyte.PropType{airbyte.Number},
			AirbyteType: airbyte.BigNumber,
		}

	// Date and time types.
	case "date":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.String}}

	case "datetime":
		return airbyte.PropertyType{
			Type:        []airbyte.PropType{airbyte.String},
			AirbyteType: airbyte.TimestampWOTZ,
		}

	case "timestamp":
		return airbyte.PropertyType{
			Type:        []airbyte.PropType{airbyte.String},
			AirbyteType: airbyte.TimestampWithTZ,
		}

	case "time":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.String}}

	case "year":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.Integer}}

	// String types.
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.String}}

	case "enum", "set":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.String}}

	// JSON.
	case "json":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.String}}

	// Binary types (will be base64-encoded at read time).
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.String}}

	// Spatial types.
	case "geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon", "geometrycollection":
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.String}}

	default:
		// Fallback: treat unknown types as string.
		return airbyte.PropertyType{Type: []airbyte.PropType{airbyte.String}}
	}
}

// isBinaryType returns true if the MySQL data type is a binary/blob type.
func isBinaryType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}
