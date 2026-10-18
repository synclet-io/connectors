package main

import (
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// mapMySQLTypeToAirbyte maps a MySQL column to its Airbyte property type.
func mapMySQLTypeToAirbyte(col ColumnInfo) synclet.PropertyType {
	dt := strings.ToLower(col.DataType)

	switch dt {
	// Boolean-like types represented as integer (0/1).
	case "boolean", "bool":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}}

	case "tinyint":
		// tinyint(1) is MySQL's boolean representation.
		if strings.Contains(strings.ToLower(col.ColumnType), "(1)") {
			return synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}}
		}
		return synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}}

	case "bit":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}}

	// Integer types.
	case "smallint", "mediumint", "int", "integer":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}}

	case "bigint":
		// bigint unsigned can exceed int64 range.
		if strings.Contains(strings.ToLower(col.ColumnType), "unsigned") {
			return synclet.PropertyType{
				Type:        []synclet.PropType{synclet.Integer},
				AirbyteType: synclet.BigInteger,
			}
		}
		return synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}}

	// Floating-point types.
	case "float":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.Number}}

	case "double", "real":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.Number}}

	// Decimal/numeric types — depends on scale.
	case "decimal", "numeric":
		if col.NumericScale != nil && *col.NumericScale == 0 {
			return synclet.PropertyType{
				Type:        []synclet.PropType{synclet.Integer},
				AirbyteType: synclet.BigInteger,
			}
		}
		return synclet.PropertyType{
			Type:        []synclet.PropType{synclet.Number},
			AirbyteType: synclet.BigNumber,
		}

	// Date and time types.
	case "date":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}

	case "datetime":
		return synclet.PropertyType{
			Type:        []synclet.PropType{synclet.String},
			AirbyteType: synclet.TimestampWOTZ,
		}

	case "timestamp":
		return synclet.PropertyType{
			Type:        []synclet.PropType{synclet.String},
			AirbyteType: synclet.TimestampWithTZ,
		}

	case "time":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}

	case "year":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}}

	// String types.
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}

	case "enum", "set":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}

	// JSON.
	case "json":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}

	// Binary types (will be base64-encoded at read time).
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}

	// Spatial types.
	case "geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon", "geometrycollection":
		return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}

	default:
		// Fallback: treat unknown types as string.
		return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}
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
