package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// column is a column as INFORMATION_SCHEMA.COLUMNS describes it.
type column struct {
	name      string
	dataType  string
	precision sql.NullInt64
	scale     sql.NullInt64
}

// kind is how a column is selected, typed and decoded.
type kind int

const (
	kindInteger kind = iota + 1
	kindDecimal
	kindFloat
	kindText
	kindBool
	kindDate
	kindTime
	kindTimestampNTZ
	kindTimestampTZ
	kindVariant
	kindObject
	kindArray
	kindVector
	kindBinary
	kindGeo
)

// Snowflake formats the connector renders values in and parses cursors from,
// and the Go layouts of the same text.
const (
	formatDate = `YYYY-MM-DD`
	formatTime = `HH24:MI:SS.FF9`
	formatNTZ  = `YYYY-MM-DD"T"HH24:MI:SS.FF9`
	formatTZ   = `YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM`

	layoutTime = "15:04:05.000000000"
	layoutNTZ  = "2006-01-02T15:04:05.000000000"
	layoutTZ   = "2006-01-02T15:04:05.000000000-07:00"
)

// kind returns the column's kind, or an error naming a type the connector
// cannot read.
func (c column) kind() (kind, error) {
	switch c.dataType {
	case "NUMBER":
		if c.scale.Int64 == 0 {
			return kindInteger, nil
		}
		return kindDecimal, nil
	case "DECFLOAT":
		return kindDecimal, nil
	case "FLOAT":
		return kindFloat, nil
	case "TEXT":
		return kindText, nil
	case "BOOLEAN":
		return kindBool, nil
	case "DATE":
		return kindDate, nil
	case "TIME":
		return kindTime, nil
	case "TIMESTAMP_NTZ":
		return kindTimestampNTZ, nil
	case "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		return kindTimestampTZ, nil
	case "VARIANT":
		return kindVariant, nil
	case "OBJECT", "MAP":
		return kindObject, nil
	case "ARRAY":
		return kindArray, nil
	case "VECTOR":
		return kindVector, nil
	case "BINARY":
		return kindBinary, nil
	case "GEOGRAPHY", "GEOMETRY":
		return kindGeo, nil
	}
	return 0, fmt.Errorf("column %q has type %s, which the connector cannot read", c.name, c.dataType)
}

func nullable(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}
}

// schema is the JSON schema of a value of kind k. Values without a time
// zone carry no format: JSON schema's time and date-time need an offset.
func (k kind) schema() synclet.PropertySpec {
	switch k {
	case kindInteger:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Integer)}
	case kindDecimal, kindFloat:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Number)}
	case kindBool:
		return synclet.PropertySpec{PropertyType: nullable("boolean")}
	case kindDate:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String), Format: synclet.Date}
	case kindTime:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: "time_without_timezone"}}
	case kindTimestampNTZ:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWOTZ}}
	case kindTimestampTZ:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWithTZ}, Format: synclet.DateTime}
	case kindVariant:
		return synclet.PropertySpec{}
	case kindObject:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Object)}
	case kindArray:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Array)}
	case kindVector:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Array), Items: map[string]any{"type": "number"}}
	default:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String)}
	}
}

// expr is the SQL expression that selects column ref of kind k as text in
// the form value decodes, so that every result format yields the same
// string. FLOAT is selected as is: the driver renders it exactly.
func (k kind) expr(ref string) string {
	switch k {
	case kindInteger, kindDecimal, kindBool:
		return "TO_VARCHAR(" + ref + ")"
	case kindDate:
		return "TO_VARCHAR(" + ref + ", '" + formatDate + "')"
	case kindTime:
		return "TO_VARCHAR(" + ref + ", '" + formatTime + "')"
	case kindTimestampNTZ:
		return "TO_VARCHAR(" + ref + ", '" + formatNTZ + "')"
	case kindTimestampTZ:
		return "TO_VARCHAR(" + ref + ", '" + formatTZ + "')"
	case kindVariant, kindObject, kindArray:
		return "TO_JSON(" + ref + ")"
	case kindVector:
		return "TO_JSON(" + ref + "::ARRAY)"
	case kindBinary:
		return "TO_VARCHAR(" + ref + ", 'BASE64')"
	case kindGeo:
		return "ST_ASWKT(" + ref + ")"
	default:
		return ref
	}
}

var (
	integerText = regexp.MustCompile(`^-?[0-9]+$`)
	decimalText = regexp.MustCompile(`^-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)
)

// value decodes v, a value selected by expr, into its JSON value. NaN and
// infinite floats are NULL: JSON cannot hold them. Text that is not in the
// form expr renders is an error, never a guess.
func (k kind) value(v sql.NullString) (any, error) {
	if !v.Valid {
		return nil, nil
	}
	s := v.String
	switch k {
	case kindInteger:
		if !integerText.MatchString(s) {
			return nil, fmt.Errorf("integer value %q", s)
		}
		return json.Number(s), nil
	case kindDecimal:
		if !decimalText.MatchString(s) {
			return nil, fmt.Errorf("number value %q", s)
		}
		return json.Number(s), nil
	case kindFloat:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("float value %q", s)
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, nil
		}
		return f, nil
	case kindBool:
		switch s {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("boolean value %q", s)
	case kindDate:
		if _, err := time.Parse(time.DateOnly, s); err != nil {
			return nil, fmt.Errorf("date value %q", s)
		}
		return s, nil
	case kindTime:
		t, err := time.Parse(layoutTime, s)
		if err != nil {
			return nil, fmt.Errorf("time value %q", s)
		}
		return trimFraction(t.Format(layoutTime)), nil
	case kindTimestampNTZ:
		t, err := time.Parse(layoutNTZ, s)
		if err != nil {
			return nil, fmt.Errorf("timestamp value %q", s)
		}
		return trimFraction(t.Format(layoutNTZ)), nil
	case kindTimestampTZ:
		t, err := time.Parse(layoutTZ, s)
		if err != nil {
			return nil, fmt.Errorf("timestamp value %q", s)
		}
		return t.UTC().Format(time.RFC3339Nano), nil
	case kindVariant, kindObject, kindArray, kindVector:
		s = strings.TrimSpace(s)
		if !json.Valid([]byte(s)) {
			return nil, fmt.Errorf("invalid JSON value %.40q", s)
		}
		if (k == kindObject && !strings.HasPrefix(s, "{")) || ((k == kindArray || k == kindVector) && !strings.HasPrefix(s, "[")) {
			return nil, fmt.Errorf("JSON value %.40q does not match the column type", s)
		}
		return json.RawMessage(s), nil
	case kindBinary:
		if _, err := base64.StdEncoding.DecodeString(s); err != nil {
			return nil, fmt.Errorf("base64 value %.40q", s)
		}
		return s, nil
	default:
		return s, nil
	}
}

// trimFraction drops the trailing zeros of a time's fraction, and the
// point when nothing is left of it.
func trimFraction(s string) string {
	head, frac, ok := strings.Cut(s, ".")
	if !ok {
		return s
	}
	frac = strings.TrimRight(frac, "0")
	if frac == "" {
		return head
	}
	return head + "." + frac
}

// cursorKinds are the kinds an incremental read may use as cursor.
var cursorKinds = map[kind]bool{
	kindInteger: true, kindDecimal: true, kindFloat: true, kindText: true,
	kindDate: true, kindTime: true, kindTimestampNTZ: true, kindTimestampTZ: true,
}

// cursorParam is the SQL expression that turns the bound saved cursor, in
// the text form expr selects, back into a value of column c.
func cursorParam(c column, k kind) string {
	switch k {
	case kindInteger, kindDecimal:
		if c.dataType == "DECFLOAT" {
			return "CAST(? AS DECFLOAT)"
		}
		return fmt.Sprintf("CAST(? AS NUMBER(38, %d))", c.scale.Int64)
	case kindFloat:
		return "CAST(? AS FLOAT)"
	case kindDate:
		return "TO_DATE(?, '" + formatDate + "')"
	case kindTime:
		return "TO_TIME(?, '" + formatTime + "')"
	case kindTimestampNTZ:
		return "TO_TIMESTAMP_NTZ(?, '" + formatNTZ + "')"
	case kindTimestampTZ:
		return "TO_TIMESTAMP_TZ(?, '" + formatTZ + "')"
	default:
		return "?"
	}
}
