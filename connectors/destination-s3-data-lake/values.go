package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go"
)

var (
	decimalLimit = new(big.Int).Exp(big.NewInt(10), big.NewInt(maxDecimalPrecision), nil)

	timestampLayoutsTZ = []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999Z07:00",
	}
	timestampLayoutsNTZ = []string{
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999",
	}
	timeLayouts = []string{"15:04:05.999999999", "15:04"}
)

// convert parses a non-null JSON value into the Go value of its column:
// bool, int64, float64, decimal128.Num, string, arrow.Date32,
// arrow.Timestamp (microseconds, UTC) or arrow.Time64 (microseconds).
func convert(c column, raw json.RawMessage) (any, error) {
	switch c.kind {
	case kindBoolean:
		switch string(raw) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, errors.New("is not a boolean")
	case kindInteger:
		return integerValue(raw)
	case kindNumber:
		if dt, ok := c.typ.(iceberg.DecimalType); ok {
			return decimalValue(raw, dt.Scale())
		}
		return doubleValue(raw)
	case kindBigInteger:
		if len(raw) > 0 && raw[0] == '"' {
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				return nil, err
			}
			raw = json.RawMessage(s)
		}
		n, err := decimalValue(raw, 0)
		if err != nil {
			return nil, fmt.Errorf("is not an integer of at most %d digits: %w", maxDecimalPrecision, err)
		}
		return n, nil
	case kindString, kindTimeTZ:
		if len(raw) > 0 && raw[0] == '"' {
			return stringValue(raw)
		}
		return compactJSON(raw)
	case kindJSON:
		return compactJSON(raw)
	case kindDate:
		s, err := stringValue(raw)
		if err != nil {
			return nil, err
		}
		t, err := time.Parse(time.DateOnly, s)
		if err != nil {
			return nil, errors.New("is not a date (YYYY-MM-DD)")
		}
		return arrow.Date32FromTime(t), nil
	case kindTimestampTZ, kindTimestampNTZ:
		s, err := stringValue(raw)
		if err != nil {
			return nil, err
		}
		t, err := parseTimestamp(s, c.kind == kindTimestampTZ)
		if err != nil {
			return nil, err
		}
		return arrow.Timestamp(t.UnixMicro()), nil
	case kindTime:
		s, err := stringValue(raw)
		if err != nil {
			return nil, err
		}
		return timeValue(s)
	}
	return nil, fmt.Errorf("unsupported column kind %d", c.kind)
}

func stringValue(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", errors.New("is not a string")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", err
	}
	return s, nil
}

func compactJSON(raw json.RawMessage) (string, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// parseTimestamp parses a date-time; withTZ accepts an offset and reads a
// value without one as UTC, !withTZ accepts only values without an offset.
func parseTimestamp(s string, withTZ bool) (time.Time, error) {
	layouts := timestampLayoutsNTZ
	if withTZ {
		layouts = append(append([]string{}, timestampLayoutsTZ...), timestampLayoutsNTZ...)
	}
	for _, l := range layouts {
		t, err := time.Parse(l, s)
		if err != nil {
			continue
		}
		if t.Nanosecond()%1000 != 0 {
			return time.Time{}, errors.New("has digits below microseconds")
		}
		return t, nil
	}
	if withTZ {
		return time.Time{}, errors.New("is not a date-time (RFC 3339)")
	}
	return time.Time{}, errors.New("is not a date-time without time zone (YYYY-MM-DDTHH:MM:SS[.ffffff])")
}

func timeValue(s string) (arrow.Time64, error) {
	for _, l := range timeLayouts {
		t, err := time.Parse(l, s)
		if err != nil {
			continue
		}
		if t.Nanosecond()%1000 != 0 {
			return 0, errors.New("has digits below microseconds")
		}
		midnight := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		return arrow.Time64(t.Sub(midnight).Microseconds()), nil
	}
	return 0, errors.New("is not a time without time zone (HH:MM:SS[.ffffff])")
}

func isNumber(raw json.RawMessage) bool {
	if len(raw) == 0 || (raw[0] != '-' && (raw[0] < '0' || raw[0] > '9')) {
		return false
	}
	var n json.Number
	return json.Unmarshal(raw, &n) == nil
}

func parseRat(raw json.RawMessage) (*big.Rat, error) {
	if !isNumber(raw) {
		return nil, errors.New("is not a number")
	}
	r, ok := new(big.Rat).SetString(string(raw))
	if !ok {
		return nil, errors.New("is not a number")
	}
	return r, nil
}

func integerValue(raw json.RawMessage) (int64, error) {
	r, err := parseRat(raw)
	if err != nil {
		return 0, err
	}
	if !r.IsInt() {
		return 0, errors.New("is not an integer")
	}
	if !r.Num().IsInt64() {
		return 0, errors.New("does not fit a long (64-bit integer)")
	}
	return r.Num().Int64(), nil
}

func doubleValue(raw json.RawMessage) (float64, error) {
	if !isNumber(raw) {
		return 0, errors.New("is not a number")
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsInf(f, 0) {
		return 0, errors.New("does not fit a double")
	}
	return f, nil
}

// decimalValue is the unscaled value of a number in decimal(38, scale); a
// number with more decimal places or digits fails.
func decimalValue(raw json.RawMessage, scale int) (decimal128.Num, error) {
	r, err := parseRat(raw)
	if err != nil {
		return decimal128.Num{}, err
	}
	r.Mul(r, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)))
	if !r.IsInt() {
		return decimal128.Num{}, fmt.Errorf("has more than %d decimal places", scale)
	}
	if new(big.Int).Abs(r.Num()).Cmp(decimalLimit) >= 0 {
		return decimal128.Num{}, fmt.Errorf("does not fit decimal(%d, %d)", maxDecimalPrecision, scale)
	}
	return decimal128.FromBigInt(r.Num()), nil
}

// appendValue appends a value of convert to its column's builder; nil
// appends null.
func appendValue(b array.Builder, v any) error {
	if v == nil {
		b.AppendNull()
		return nil
	}
	switch b := b.(type) {
	case *array.BooleanBuilder:
		b.Append(v.(bool))
	case *array.Int64Builder:
		b.Append(v.(int64))
	case *array.Float64Builder:
		b.Append(v.(float64))
	case *array.Decimal128Builder:
		b.Append(v.(decimal128.Num))
	case *array.StringBuilder:
		b.Append(v.(string))
	case *array.Date32Builder:
		b.Append(v.(arrow.Date32))
	case *array.TimestampBuilder:
		b.Append(v.(arrow.Timestamp))
	case *array.Time64Builder:
		b.Append(v.(arrow.Time64))
	default:
		return fmt.Errorf("unsupported Arrow builder %T", b)
	}
	return nil
}

// compareValues orders two non-nil values of one column.
func compareValues(a, b any) int {
	switch a := a.(type) {
	case bool:
		bb := b.(bool)
		switch {
		case a == bb:
			return 0
		case !a:
			return -1
		}
		return 1
	case int64:
		return cmpOrdered(a, b.(int64))
	case float64:
		return cmpOrdered(a, b.(float64))
	case decimal128.Num:
		return a.Cmp(b.(decimal128.Num))
	case string:
		return cmpOrdered(a, b.(string))
	case arrow.Date32:
		return cmpOrdered(a, b.(arrow.Date32))
	case arrow.Timestamp:
		return cmpOrdered(a, b.(arrow.Timestamp))
	case arrow.Time64:
		return cmpOrdered(a, b.(arrow.Time64))
	}
	return 0
}

func cmpOrdered[T int64 | float64 | string | arrow.Date32 | arrow.Timestamp | arrow.Time64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
