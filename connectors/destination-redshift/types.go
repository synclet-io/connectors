package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// colType is a column type as Redshift's format_type prints it, so a type
// read back from the catalog compares equal to the one the connector wants.
type colType string

const (
	colBoolean     colType = "boolean"
	colBigint      colType = "bigint"
	colDouble      colType = "double precision"
	colNumeric     colType = "numeric(38,0)"
	colVarchar     colType = "character varying(65535)"
	colVarchar36   colType = "character varying(36)"
	colDate        colType = "date"
	colTimestamptz colType = "timestamp with time zone"
	colTimestamp   colType = "timestamp without time zone"
	colTime        colType = "time without time zone"
	colTimetz      colType = "time with time zone"
	colSuper       colType = "super"
)

// maxVarcharBytes is the largest VARCHAR Redshift has; longer strings are
// truncated and the truncation is recorded in _airbyte_meta.
const maxVarcharBytes = 65535

// maxNumericDigits is the precision of NUMERIC(38,0).
const maxNumericDigits = 38

// wellKnownTypes maps the definitions of Airbyte's WellKnownTypes.json that
// a property may reference with "$ref".
var wellKnownTypes = map[string]colType{
	"String":                   colVarchar,
	"BinaryData":               colVarchar,
	"Boolean":                  colBoolean,
	"Integer":                  colBigint,
	"Number":                   colDouble,
	"Date":                     colDate,
	"TimestampWithTimezone":    colTimestamptz,
	"TimestampWithoutTimezone": colTimestamp,
	"TimeWithTimezone":         colTimetz,
	"TimeWithoutTimezone":      colTime,
}

// columnType maps the JSON schema of one property to its column type. A
// property with more than one non-null type, a union (anyOf, oneOf) or no
// type at all lands as SUPER.
func columnType(prop map[string]any) colType {
	if ref, ok := prop["$ref"].(string); ok {
		if _, def, found := strings.Cut(ref, "#/definitions/"); found {
			if t, ok := wellKnownTypes[def]; ok {
				return t
			}
		}
		return colSuper
	}
	types := schemaTypes(prop["type"])
	if len(types) != 1 {
		return colSuper
	}
	airbyteType, _ := prop["airbyte_type"].(string)
	format, _ := prop["format"].(string)
	switch types[0] {
	case "boolean":
		return colBoolean
	case "integer":
		return colBigint
	case "number":
		if airbyteType == "integer" {
			return colBigint
		}
		return colDouble
	case "string":
		switch {
		case airbyteType == "big_integer":
			return colNumeric
		case airbyteType == "integer":
			return colBigint
		case airbyteType == "timestamp_without_timezone":
			return colTimestamp
		case airbyteType == "timestamp_with_timezone":
			return colTimestamptz
		case airbyteType == "time_with_timezone":
			return colTimetz
		case airbyteType == "time_without_timezone":
			return colTime
		case format == "date":
			return colDate
		case format == "date-time":
			return colTimestamptz
		case format == "time":
			return colTime
		}
		return colVarchar
	}
	return colSuper
}

// schemaTypes returns the non-null types of a "type" keyword.
func schemaTypes(v any) []string {
	var out []string
	switch t := v.(type) {
	case string:
		if t != "null" {
			out = append(out, t)
		}
	case []any:
		for _, item := range t {
			if s, ok := item.(string); ok && s != "null" {
				out = append(out, s)
			}
		}
	}
	return out
}

var (
	numberText  = regexp.MustCompile(`^-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?$`)
	integerText = regexp.MustCompile(`^-?[0-9]+$`)
	dateText    = regexp.MustCompile(`^([0-9]{4}-[0-9]{2}-[0-9]{2})$`)
	tsText      = regexp.MustCompile(`^([0-9]{4}-[0-9]{2}-[0-9]{2})[Tt ]([0-9]{2}:[0-9]{2}(?::[0-9]{2})?)(?:\.([0-9]+))?(Z|z|[+-][0-9]{2}(?::?[0-9]{2})?)?$`)
	timeText    = regexp.MustCompile(`^([0-9]{2}:[0-9]{2}(?::[0-9]{2})?)(?:\.([0-9]+))?(Z|z|[+-][0-9]{2}(?::?[0-9]{2})?)?$`)
)

// encodeValue renders one JSON value as the text COPY parses into a column
// of type t; null reports a JSON null and truncated a VARCHAR value cut to
// maxVarcharBytes. Every other value Redshift could not hold as given is an
// error, so COPY never rounds, truncates or reinterprets a value.
func encodeValue(t colType, raw json.RawMessage) (val string, null, truncated bool, err error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return "", true, false, nil
	}
	isString := raw[0] == '"'
	var s string
	if isString {
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", false, false, fmt.Errorf("invalid JSON string: %w", err)
		}
	}
	if badUnicode(raw) {
		return "", false, false, errors.New("value is not valid UTF-8 (invalid bytes or a lone UTF-16 surrogate)")
	}
	text := s
	if !isString {
		text = string(raw)
	}
	switch t {
	case colSuper:
		var b bytes.Buffer
		if err := json.Compact(&b, raw); err != nil {
			return "", false, false, fmt.Errorf("invalid JSON: %w", err)
		}
		return b.String(), false, false, nil
	case colBoolean:
		switch strings.ToLower(text) {
		case "true":
			return "true", false, false, nil
		case "false":
			return "false", false, false, nil
		}
		return "", false, false, fmt.Errorf("value %s is not a boolean", raw)
	case colBigint:
		if !integerText.MatchString(text) {
			return "", false, false, fmt.Errorf("value %s is not an integer", raw)
		}
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			return "", false, false, fmt.Errorf("value %s is outside the BIGINT range", raw)
		}
		return text, false, false, nil
	case colNumeric:
		if !integerText.MatchString(text) {
			return "", false, false, fmt.Errorf("value %s is not an integer", raw)
		}
		if len(strings.TrimLeft(strings.TrimPrefix(text, "-"), "0")) > maxNumericDigits {
			return "", false, false, fmt.Errorf("value %s has more than %d digits, which NUMERIC(38,0) cannot hold", raw, maxNumericDigits)
		}
		return text, false, false, nil
	case colDouble:
		return encodeDouble(raw, text)
	case colVarchar:
		if strings.ContainsRune(text, 0) {
			return "", false, false, errors.New(`value contains \u0000, which VARCHAR cannot hold`)
		}
		if len(text) <= maxVarcharBytes {
			return text, false, false, nil
		}
		cut := maxVarcharBytes
		for cut > 0 && !utf8.RuneStart(text[cut]) {
			cut--
		}
		return text[:cut], false, true, nil
	}
	if !isString {
		return "", false, false, fmt.Errorf("value %s is not a string", raw)
	}
	switch t {
	case colDate:
		if !dateText.MatchString(s) {
			return "", false, false, fmt.Errorf("value %s is not a date (YYYY-MM-DD)", raw)
		}
		d, err := time.Parse(time.DateOnly, s)
		if err != nil || d.Year() < 1 {
			return "", false, false, fmt.Errorf("value %s is not a date (YYYY-MM-DD)", raw)
		}
		return s, false, false, nil
	case colTimestamptz, colTimestamp:
		return encodeTimestamp(t, raw, s)
	default:
		return encodeTime(t, raw, s)
	}
}

// encodeDouble accepts a number only when the float64 nearest to it prints
// back as the same decimal value, so DOUBLE PRECISION holds it exactly as
// the source wrote it.
func encodeDouble(raw json.RawMessage, text string) (string, bool, bool, error) {
	if !numberText.MatchString(text) {
		return "", false, false, fmt.Errorf("value %s is not a number", raw)
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return "", false, false, fmt.Errorf("value %s is outside the DOUBLE PRECISION range", raw)
	}
	out := strconv.FormatFloat(f, 'g', -1, 64)
	want, ok1 := new(big.Rat).SetString(text)
	got, ok2 := new(big.Rat).SetString(out)
	if !ok1 || !ok2 || want.Cmp(got) != 0 {
		return "", false, false, fmt.Errorf("value %s has more precision than DOUBLE PRECISION, which cannot hold it exactly (nearest is %s)", raw, out)
	}
	return out, false, false, nil
}

func encodeTimestamp(t colType, raw json.RawMessage, s string) (string, bool, bool, error) {
	m := tsText.FindStringSubmatch(s)
	if m == nil {
		return "", false, false, fmt.Errorf("value %s is not a timestamp (YYYY-MM-DDTHH:MM:SS[.ffffff][zone])", raw)
	}
	clock := m[2]
	if len(clock) == 5 {
		clock += ":00"
	}
	frac, err := fraction(raw, m[3])
	if err != nil {
		return "", false, false, err
	}
	if t == colTimestamp {
		if m[4] != "" {
			return "", false, false, fmt.Errorf("value %s carries a time zone, which TIMESTAMP cannot hold", raw)
		}
		v, err := time.Parse(time.DateTime, m[1]+" "+clock)
		if err != nil || v.Year() < 1 {
			return "", false, false, fmt.Errorf("value %s is not a timestamp: %v", raw, err)
		}
		return v.Format(time.DateTime) + frac, false, false, nil
	}
	off, err := zoneOffset(m[4])
	if err != nil {
		return "", false, false, fmt.Errorf("value %s: %w", raw, err)
	}
	v, err := time.Parse(time.DateTime, m[1]+" "+clock)
	if err != nil {
		return "", false, false, fmt.Errorf("value %s is not a timestamp: %v", raw, err)
	}
	v = v.Add(-time.Duration(off) * time.Second)
	if v.Year() < 1 {
		return "", false, false, fmt.Errorf("value %s is before year 1", raw)
	}
	return v.Format(time.DateTime) + frac + "+00", false, false, nil
}

func encodeTime(t colType, raw json.RawMessage, s string) (string, bool, bool, error) {
	m := timeText.FindStringSubmatch(s)
	if m == nil {
		return "", false, false, fmt.Errorf("value %s is not a time (HH:MM:SS[.ffffff][zone])", raw)
	}
	clock := m[1]
	if len(clock) == 5 {
		clock += ":00"
	}
	if _, err := time.Parse(time.TimeOnly, clock); err != nil {
		return "", false, false, fmt.Errorf("value %s is not a time (HH:MM:SS[.ffffff][zone])", raw)
	}
	frac, err := fraction(raw, m[2])
	if err != nil {
		return "", false, false, err
	}
	if t == colTime {
		if m[3] != "" {
			return "", false, false, fmt.Errorf("value %s carries a time zone, which TIME cannot hold", raw)
		}
		return clock + frac, false, false, nil
	}
	off, err := zoneOffset(m[3])
	if err != nil {
		return "", false, false, fmt.Errorf("value %s: %w", raw, err)
	}
	sign := '+'
	if off < 0 {
		sign, off = '-', -off
	}
	return fmt.Sprintf("%s%s%c%02d:%02d", clock, frac, sign, off/3600, off%3600/60), false, false, nil
}

// fraction returns the fractional seconds as ".dddddd" without trailing
// zeros, or "" for none. Redshift keeps microseconds; a value with a
// non-zero digit past the sixth would be cut, so it fails.
func fraction(raw json.RawMessage, digits string) (string, error) {
	if len(digits) > 6 {
		if strings.Trim(digits[6:], "0") != "" {
			return "", fmt.Errorf("value %s has more than 6 fractional digits of seconds, which Redshift cannot hold", raw)
		}
		digits = digits[:6]
	}
	digits = strings.TrimRight(digits, "0")
	if digits == "" {
		return "", nil
	}
	return "." + digits, nil
}

// zoneOffset returns the offset in seconds of "Z", "+hh", "+hhmm" or
// "+hh:mm"; no zone is UTC.
func zoneOffset(z string) (int, error) {
	if z == "" || z == "Z" || z == "z" {
		return 0, nil
	}
	digits := strings.ReplaceAll(z[1:], ":", "")
	h, _ := strconv.Atoi(digits[:2])
	mins := 0
	if len(digits) == 4 {
		mins, _ = strconv.Atoi(digits[2:])
	}
	if h > 15 || mins > 59 {
		return 0, fmt.Errorf("time zone offset %s is out of range", z)
	}
	off := h*3600 + mins*60
	if z[0] == '-' {
		off = -off
	}
	return off, nil
}

// badUnicode reports whether the JSON text raw holds bytes that are not
// UTF-8 or a \u escape of a UTF-16 surrogate without its partner; decoding
// would replace either with U+FFFD.
func badUnicode(raw json.RawMessage) bool {
	if !utf8.Valid(raw) {
		return true
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' || i+1 >= len(raw) {
			continue
		}
		if raw[i+1] != 'u' {
			i++
			continue
		}
		r, ok := hexEscape(raw[i:])
		if !ok {
			return true
		}
		switch {
		case r >= 0xD800 && r <= 0xDBFF:
			low, ok := hexEscape(raw[i+6:])
			if !ok || low < 0xDC00 || low > 0xDFFF {
				return true
			}
			i += 11
		case r >= 0xDC00 && r <= 0xDFFF:
			return true
		default:
			i += 5
		}
	}
	return false
}

// hexEscape decodes the \uXXXX escape at the start of b.
func hexEscape(b []byte) (rune, bool) {
	if len(b) < 6 || b[0] != '\\' || b[1] != 'u' {
		return 0, false
	}
	v, err := strconv.ParseUint(string(b[2:6]), 16, 16)
	if err != nil {
		return 0, false
	}
	return rune(v), true
}
