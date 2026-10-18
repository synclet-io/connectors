package main

import (
	"fmt"
	"strconv"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// kind is how a column's value is rendered and declared.
type kind int

const (
	kindOther kind = iota
	kindBool
	kindInt
	kindFloat
	kindReal
	kindDecimal
	kindMoney
	kindString
	kindText
	kindUUID
	kindBinary
	kindDate
	kindTime
	kindDateTime
	kindDateTimeOffset
	kindHierarchyID
	kindSpatial
)

var builtinKinds = map[string]kind{
	"bit":              kindBool,
	"tinyint":          kindInt,
	"smallint":         kindInt,
	"int":              kindInt,
	"bigint":           kindInt,
	"decimal":          kindDecimal,
	"numeric":          kindDecimal,
	"money":            kindMoney,
	"smallmoney":       kindMoney,
	"real":             kindReal,
	"float":            kindFloat,
	"char":             kindString,
	"varchar":          kindString,
	"nchar":            kindString,
	"nvarchar":         kindString,
	"sysname":          kindString,
	"text":             kindText,
	"ntext":            kindText,
	"xml":              kindText,
	"sql_variant":      kindText,
	"uniqueidentifier": kindUUID,
	"binary":           kindBinary,
	"varbinary":        kindBinary,
	"image":            kindBinary,
	"timestamp":        kindBinary,
	"date":             kindDate,
	"time":             kindTime,
	"datetime":         kindDateTime,
	"datetime2":        kindDateTime,
	"smalldatetime":    kindDateTime,
	"datetimeoffset":   kindDateTimeOffset,
	"hierarchyid":      kindHierarchyID,
	"geography":        kindSpatial,
	"geometry":         kindSpatial,
}

// unorderable are the types SQL Server cannot compare or sort, so they
// cannot be a cursor.
var unorderable = map[string]bool{
	"text": true, "ntext": true, "image": true, "xml": true,
	"geography": true, "geometry": true, "sql_variant": true,
}

// colType is a column's type after alias types are resolved to their base
// type: its name as sys.types has it, with the length, precision and scale
// of sys.columns.
type colType struct {
	name      string
	maxLength int
	precision int
	scale     int
}

func (c colType) kind() kind {
	if k, ok := builtinKinds[c.name]; ok {
		return k
	}
	return kindOther
}

// declared is the type as a column declaration spells it. rowversion
// (sys.types name timestamp) is declared as binary(8), the type of its values.
func (c colType) declared() string {
	switch c.name {
	case "varchar", "char", "varbinary", "binary":
		if c.maxLength == -1 {
			return c.name + "(max)"
		}
		return c.name + "(" + strconv.Itoa(c.maxLength) + ")"
	case "nvarchar", "nchar":
		if c.maxLength == -1 {
			return c.name + "(max)"
		}
		return c.name + "(" + strconv.Itoa(c.maxLength/2) + ")"
	case "decimal", "numeric":
		return fmt.Sprintf("%s(%d,%d)", c.name, c.precision, c.scale)
	case "datetime2", "time", "datetimeoffset":
		return fmt.Sprintf("%s(%d)", c.name, c.scale)
	case "timestamp":
		return "binary(8)"
	case "sysname":
		return "nvarchar(128)"
	default:
		return c.name
	}
}

func nullable(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}
}

// schema is the JSON schema of a column of type c. Values without a time
// zone carry no format: JSON schema's time and date-time need an offset.
func (c colType) schema() synclet.PropertySpec {
	switch c.kind() {
	case kindBool:
		return synclet.PropertySpec{PropertyType: nullable("boolean")}
	case kindInt:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Integer)}
	case kindFloat, kindReal, kindDecimal, kindMoney:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Number)}
	case kindDate:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String), Format: synclet.Date}
	case kindTime:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: "time_without_timezone"}}
	case kindDateTime:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWOTZ}}
	case kindDateTimeOffset:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWithTZ}, Format: synclet.DateTime}
	default:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String)}
	}
}

// expr is the SQL expression that reads column ref as the value its schema
// declares. SQL Server renders exact and awkward types as text, so decimals
// keep every digit and uniqueidentifier reads in its canonical form;
// binary values are read as bytes.
func (c colType) expr(ref string) string {
	switch c.kind() {
	case kindBool, kindInt, kindFloat, kindReal, kindString, kindBinary:
		return ref
	case kindDecimal:
		return "CONVERT(varchar(48), " + ref + ")"
	case kindMoney:
		return "CONVERT(varchar(32), " + ref + ", 2)"
	case kindUUID:
		return "LOWER(CONVERT(char(36), " + ref + "))"
	case kindDate:
		return "CONVERT(char(10), " + ref + ", 23)"
	case kindTime:
		return "CONVERT(varchar(16), " + ref + ")"
	case kindDateTime:
		return "CONVERT(varchar(27), " + ref + ", 126)"
	case kindDateTimeOffset:
		return "CONVERT(varchar(34), " + ref + ", 126)"
	case kindHierarchyID:
		return ref + ".ToString()"
	case kindSpatial:
		return ref + ".STAsText()"
	default:
		return "CONVERT(nvarchar(max), " + ref + ")"
	}
}

// cursorExpr is the cursor column ref as text that cursorParam turns back
// into the same value.
func (c colType) cursorExpr(ref string) string {
	switch c.kind() {
	case kindBinary:
		return "CONVERT(varchar(max), CONVERT(varbinary(max), " + ref + "), 1)"
	case kindFloat, kindReal:
		return "CONVERT(varchar(30), " + ref + ", 3)"
	case kindInt, kindBool, kindString:
		return "CONVERT(nvarchar(max), " + ref + ")"
	case kindHierarchyID:
		return ref + ".ToString()"
	default:
		return c.expr(ref)
	}
}

// cursorParam is the SQL expression that converts the saved cursor text
// param to the column's declared type, so comparisons run in that type.
func (c colType) cursorParam(param string) string {
	style := ""
	switch c.kind() {
	case kindBinary:
		style = ", 1"
	case kindDate:
		style = ", 23"
	case kindDateTime:
		style = ", 126"
	case kindDateTimeOffset:
		style = ", 126"
	case kindHierarchyID:
		return "hierarchyid::Parse(" + param + ")"
	}
	return "CONVERT(" + c.declared() + ", " + param + style + ")"
}

// orderable reports whether a column of type c can be a cursor.
func (c colType) orderable() bool {
	return !unorderable[c.name] && c.kind() != kindOther
}
