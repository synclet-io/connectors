package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func nullable(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}
}

// fieldSchema is the JSON schema of a column: a REPEATED column is an array
// of its element schema.
func fieldSchema(f *bigquery.FieldSchema) synclet.PropertySpec {
	el := elementSchema(f)
	if !f.Repeated {
		return el
	}
	return synclet.PropertySpec{PropertyType: nullable(synclet.Array), Items: specMap(el)}
}

// elementSchema is the JSON schema of one value of f. Values without a time
// zone carry no format: JSON schema's time and date-time need an offset.
func elementSchema(f *bigquery.FieldSchema) synclet.PropertySpec {
	switch f.Type {
	case bigquery.IntegerFieldType:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Integer)}
	case bigquery.NumericFieldType, bigquery.BigNumericFieldType, bigquery.FloatFieldType:
		return synclet.PropertySpec{PropertyType: nullable(synclet.Number)}
	case bigquery.BooleanFieldType:
		return synclet.PropertySpec{PropertyType: nullable("boolean")}
	case bigquery.DateFieldType:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String), Format: synclet.Date}
	case bigquery.TimeFieldType:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: "time_without_timezone"}}
	case bigquery.DateTimeFieldType:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWOTZ}}
	case bigquery.TimestampFieldType:
		return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWithTZ}, Format: synclet.DateTime}
	case bigquery.JSONFieldType:
		return synclet.PropertySpec{}
	case bigquery.RecordFieldType:
		props := make(map[synclet.PropertyName]synclet.PropertySpec, len(f.Schema))
		for _, sub := range f.Schema {
			props[synclet.PropertyName(sub.Name)] = fieldSchema(sub)
		}
		return synclet.PropertySpec{PropertyType: nullable(synclet.Object), Properties: props}
	default:
		return synclet.PropertySpec{PropertyType: nullable(synclet.String)}
	}
}

// specMap is s as the generic JSON object an array's items hold.
func specMap(s synclet.PropertySpec) map[string]any {
	b, err := json.Marshal(s)
	if err != nil {
		panic(fmt.Sprintf("marshal property spec: %v", err))
	}
	m := map[string]any{}
	if err := json.Unmarshal(b, &m); err != nil {
		panic(fmt.Sprintf("unmarshal property spec: %v", err))
	}
	return m
}

// fieldValue is the JSON value of v, a value of column f as the BigQuery
// client decodes it. A NULL REPEATED column is an empty array.
func fieldValue(f *bigquery.FieldSchema, v bigquery.Value) (any, error) {
	if !f.Repeated {
		return scalarValue(f, v)
	}
	vs, ok := v.([]bigquery.Value)
	if v != nil && !ok {
		return nil, fmt.Errorf("column %q: REPEATED value is %T", f.Name, v)
	}
	out := make([]any, len(vs))
	for i, e := range vs {
		var err error
		if out[i], err = scalarValue(f, e); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func scalarValue(f *bigquery.FieldSchema, v bigquery.Value) (any, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case int64, bool, []byte:
		return x, nil
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil, nil
		}
		return x, nil
	case string:
		if f.Type == bigquery.JSONFieldType {
			if !json.Valid([]byte(x)) {
				return nil, fmt.Errorf("column %q: invalid JSON value", f.Name)
			}
			return json.RawMessage(x), nil
		}
		return x, nil
	case *big.Rat:
		scale := bigquery.NumericScaleDigits
		if f.Type == bigquery.BigNumericFieldType {
			scale = bigquery.BigNumericScaleDigits
		}
		return json.Number(trimFraction(x.FloatString(scale))), nil
	case civil.Date:
		return x.String(), nil
	case civil.Time:
		return timeString(x), nil
	case civil.DateTime:
		return x.Date.String() + "T" + timeString(x.Time), nil
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), nil
	case *bigquery.IntervalValue:
		return x.String(), nil
	case *bigquery.RangeValue:
		return rangeString(f, x)
	case []bigquery.Value:
		if f.Type != bigquery.RecordFieldType || len(x) != len(f.Schema) {
			return nil, fmt.Errorf("column %q: unexpected RECORD value", f.Name)
		}
		rec := make(map[string]any, len(x))
		for i, sub := range f.Schema {
			val, err := fieldValue(sub, x[i])
			if err != nil {
				return nil, err
			}
			rec[sub.Name] = val
		}
		return rec, nil
	default:
		return nil, fmt.Errorf("column %q: unsupported %s value %T", f.Name, f.Type, v)
	}
}

// trimFraction drops the trailing zeros of a decimal's fraction, and the
// point when nothing is left of it.
func trimFraction(s string) string {
	if !strings.Contains(s, ".") {
		return s
	}
	return strings.TrimSuffix(strings.TrimRight(s, "0"), ".")
}

func timeString(t civil.Time) string {
	s := fmt.Sprintf("%02d:%02d:%02d", t.Hour, t.Minute, t.Second)
	if t.Nanosecond == 0 {
		return s
	}
	return s + strings.TrimRight(fmt.Sprintf(".%09d", t.Nanosecond), "0")
}

// rangeString renders a RANGE as BigQuery prints it: [start, end), an
// unbounded end as UNBOUNDED.
func rangeString(f *bigquery.FieldSchema, r *bigquery.RangeValue) (string, error) {
	var el bigquery.FieldType
	if f.RangeElementType != nil {
		el = f.RangeElementType.Type
	}
	bound := func(v bigquery.Value) (string, error) {
		if v == nil {
			return "UNBOUNDED", nil
		}
		s, err := scalarValue(&bigquery.FieldSchema{Name: f.Name, Type: el}, v)
		if err != nil {
			return "", err
		}
		return fmt.Sprint(s), nil
	}
	start, err := bound(r.Start)
	if err != nil {
		return "", err
	}
	end, err := bound(r.End)
	if err != nil {
		return "", err
	}
	return "[" + start + ", " + end + ")", nil
}

// cursorTypes are the column types an incremental read may use as cursor,
// with the SQL type its saved value is cast to.
var cursorTypes = map[bigquery.FieldType]string{
	bigquery.IntegerFieldType:    "INT64",
	bigquery.NumericFieldType:    "NUMERIC",
	bigquery.BigNumericFieldType: "BIGNUMERIC",
	bigquery.FloatFieldType:      "FLOAT64",
	bigquery.StringFieldType:     "STRING",
	bigquery.DateFieldType:       "DATE",
	bigquery.TimeFieldType:       "TIME",
	bigquery.DateTimeFieldType:   "DATETIME",
	bigquery.TimestampFieldType:  "TIMESTAMP",
}

// cursorString is a cursor column's JSON value as saved in the state; ok is
// false for NULL.
func cursorString(v any) (string, bool) {
	switch x := v.(type) {
	case int64:
		return strconv.FormatInt(x, 10), true
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), true
	case json.Number:
		return string(x), true
	case string:
		return x, true
	default:
		return "", false
	}
}
