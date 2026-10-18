package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// kind is how a column's value is selected, rendered and declared.
type kind int

const (
	kindOther kind = iota
	kindInt
	kindDecimal
	kindFloat
	kindBool
	kindBit
	kindString
	kindEnum
	kindSet
	kindJSON
	kindBinary
	kindDate
	kindDatetime
	kindTimestamp
	kindTime
	kindGeometry
)

var dataTypeKinds = map[string]kind{
	"tinyint":            kindInt,
	"smallint":           kindInt,
	"mediumint":          kindInt,
	"int":                kindInt,
	"integer":            kindInt,
	"bigint":             kindInt,
	"year":               kindInt,
	"decimal":            kindDecimal,
	"numeric":            kindDecimal,
	"float":              kindFloat,
	"double":             kindFloat,
	"real":               kindFloat,
	"char":               kindString,
	"varchar":            kindString,
	"tinytext":           kindString,
	"text":               kindString,
	"mediumtext":         kindString,
	"longtext":           kindString,
	"enum":               kindEnum,
	"set":                kindSet,
	"json":               kindJSON,
	"binary":             kindBinary,
	"varbinary":          kindBinary,
	"tinyblob":           kindBinary,
	"blob":               kindBinary,
	"mediumblob":         kindBinary,
	"longblob":           kindBinary,
	"date":               kindDate,
	"datetime":           kindDatetime,
	"timestamp":          kindTimestamp,
	"time":               kindTime,
	"geometry":           kindGeometry,
	"point":              kindGeometry,
	"linestring":         kindGeometry,
	"polygon":            kindGeometry,
	"multipoint":         kindGeometry,
	"multilinestring":    kindGeometry,
	"multipolygon":       kindGeometry,
	"geometrycollection": kindGeometry,
	"geomcollection":     kindGeometry,
}

type column struct {
	name string
	kind kind
	// values are the members of an ENUM or SET column, in declaration order.
	values []string
	// scale is the number of fraction digits of a DECIMAL column.
	scale int32
}

// newColumn derives a column from information_schema.COLUMNS: DATA_TYPE
// names the type, COLUMN_TYPE adds its width and members.
func newColumn(name, dataType, columnType string) column {
	c := column{name: name, kind: dataTypeKinds[strings.ToLower(dataType)]}
	switch c.kind {
	case kindEnum, kindSet:
		c.values = typeMembers(columnType)
	case kindDecimal:
		if _, s, ok := strings.Cut(strings.TrimSuffix(columnType, ")"), ","); ok {
			if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32); err == nil {
				c.scale = int32(n)
			}
		}
	}
	if strings.EqualFold(dataType, "bit") {
		c.kind = kindBit
		if strings.EqualFold(columnType, "bit(1)") {
			c.kind = kindBool
		}
	}
	return c
}

// typeMembers parses the members of a COLUMN_TYPE such as
// enum('a','it”s').
func typeMembers(columnType string) []string {
	open := strings.IndexByte(columnType, '(')
	if open < 0 || !strings.HasSuffix(columnType, ")") {
		return nil
	}
	s := columnType[open+1 : len(columnType)-1]
	var members []string
	for len(s) > 0 && s[0] == '\'' {
		var b strings.Builder
		i := 1
		for i < len(s) {
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					b.WriteByte('\'')
					i += 2
					continue
				}
				break
			}
			b.WriteByte(s[i])
			i++
		}
		members = append(members, b.String())
		s = strings.TrimPrefix(s[min(i+1, len(s)):], ",")
	}
	return members
}

func nullable(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}
}

// schema is the JSON schema of the column. A DATETIME carries no format: JSON
// schema's date-time needs an offset.
func (c column) schema() synclet.PropertySpec {
	switch c.kind {
	case kindInt, kindBit:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Integer)}
	case kindDecimal, kindFloat:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Number)}
	case kindBool:
		return synclet.PropertySpec{PropertyType: nullable("boolean")}
	case kindJSON:
		return synclet.PropertySpec{}
	case kindDate:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String), Format: synclet.Date}
	case kindDatetime:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWOTZ}}
	case kindTimestamp:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWithTZ}, Format: synclet.DateTime}
	default:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String)}
	}
}

// expr is the select expression of the column: geometry is read as WKT.
func (c column) expr() string {
	if c.kind == kindGeometry {
		return "ST_AsText(" + ident(c.name) + ")"
	}
	return ident(c.name)
}

// errZeroDate is returned for a DATE, DATETIME or TIMESTAMP whose month or
// day is zero when zero_date_handling is fail.
var errZeroDate = errors.New("zero date")

var jsonNull = json.RawMessage("null")

// encode renders raw, the column's value as MySQL's text protocol sends it,
// as the JSON value the column's schema declares; nil is JSON null. A zero
// date reads as null, or fails with errZeroDate when failZeroDate is set.
func (c column) encode(raw []byte, failZeroDate bool) (json.RawMessage, error) {
	if raw == nil {
		return jsonNull, nil
	}
	switch c.kind {
	case kindInt, kindDecimal, kindFloat:
		return json.RawMessage(trimZeros(raw)), nil
	case kindBool:
		return json.RawMessage(strconv.FormatBool(len(raw) > 0 && raw[len(raw)-1] != 0)), nil
	case kindBit:
		var b [8]byte
		copy(b[8-min(len(raw), 8):], raw[max(len(raw)-8, 0):])
		return json.RawMessage(strconv.FormatUint(binary.BigEndian.Uint64(b[:]), 10)), nil
	case kindJSON:
		if json.Valid(raw) {
			return json.RawMessage(raw), nil
		}
		return jsonString(raw), nil
	case kindBinary:
		out := make([]byte, base64.StdEncoding.EncodedLen(len(raw))+2)
		out[0] = '"'
		base64.StdEncoding.Encode(out[1:], raw)
		out[len(out)-1] = '"'
		return out, nil
	case kindDate, kindDatetime, kindTimestamp:
		if zeroDate(raw) {
			if failZeroDate {
				return nil, errZeroDate
			}
			return jsonNull, nil
		}
		if c.kind == kindDate {
			return jsonString(raw), nil
		}
		v := bytes.Replace(raw, []byte(" "), []byte("T"), 1)
		if c.kind == kindTimestamp {
			v = append(v, 'Z')
		}
		return jsonString(v), nil
	default:
		return jsonString(raw), nil
	}
}

// trimZeros drops the leading zeros a ZEROFILL column pads its numbers
// with, keeping the sign and the digit before the decimal point.
func trimZeros(raw []byte) []byte {
	start := 0
	if len(raw) > 0 && raw[0] == '-' {
		start = 1
	}
	i := start
	for i+1 < len(raw) && raw[i] == '0' && raw[i+1] >= '0' && raw[i+1] <= '9' {
		i++
	}
	if i == start {
		return raw
	}
	return append(raw[:start:start], raw[i:]...)
}

// cursorText is the column's value as saved in the state: binary values
// base64-encoded, BIT values as an unsigned integer, others as MySQL's text.
func (c column) cursorText(raw []byte) string {
	switch c.kind {
	case kindBinary:
		return base64.StdEncoding.EncodeToString(raw)
	case kindBit, kindBool:
		var b [8]byte
		copy(b[8-min(len(raw), 8):], raw[max(len(raw)-8, 0):])
		return strconv.FormatUint(binary.BigEndian.Uint64(b[:]), 10)
	case kindInt, kindDecimal, kindFloat:
		return string(trimZeros(raw))
	default:
		return string(raw)
	}
}

// cursorArg is the query argument of a cursor saved by cursorText.
func (c column) cursorArg(saved string) (any, error) {
	switch c.kind {
	case kindBinary:
		b, err := base64.StdEncoding.DecodeString(saved)
		if err != nil {
			return nil, fmt.Errorf("saved cursor %q of binary column %q is not base64", saved, c.name)
		}
		return b, nil
	case kindBit, kindBool:
		n, err := strconv.ParseUint(saved, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("saved cursor %q of BIT column %q is not an unsigned integer", saved, c.name)
		}
		return n, nil
	default:
		return saved, nil
	}
}

// zeroDate reports whether a date text YYYY-MM-DD... has a zero month or
// day, as 0000-00-00 and 2024-00-15 have.
func zeroDate(raw []byte) bool {
	if len(raw) < 10 {
		return false
	}
	return string(raw[5:7]) == "00" || string(raw[8:10]) == "00"
}

func jsonString(b []byte) json.RawMessage {
	out, _ := json.Marshal(string(b))
	return out
}

// field is one property of a record.
type field struct {
	key   json.RawMessage
	value json.RawMessage
}

func fieldKey(name string) json.RawMessage { return jsonString([]byte(name)) }

// appendRecord appends the JSON object of fields to buf.
func appendRecord(buf []byte, fields []field) []byte {
	buf = append(buf, '{')
	for i, f := range fields {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, f.key...)
		buf = append(buf, ':')
		buf = append(buf, f.value...)
	}
	return append(buf, '}')
}
