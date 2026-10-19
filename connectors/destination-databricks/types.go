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

// colType is a Delta column type, upper-cased as information_schema prints
// it in full_data_type.
type colType string

const (
	colBoolean      colType = "BOOLEAN"
	colBigint       colType = "BIGINT"
	colDecimal      colType = "DECIMAL(38,9)"
	colBigDecimal   colType = "DECIMAL(38,0)"
	colString       colType = "STRING"
	colDate         colType = "DATE"
	colTimestamp    colType = "TIMESTAMP"
	colTimestampNTZ colType = "TIMESTAMP_NTZ"
	colVariant      colType = "VARIANT"
)

// decimalScale and decimalPrecision are those of colDecimal.
const (
	decimalPrecision = 38
	decimalScale     = 9
)

// wellKnownTypes maps the definitions of Airbyte's WellKnownTypes.json that
// a property may reference with "$ref". Databricks has no time type, so
// times land as strings.
var wellKnownTypes = map[string]colType{
	"String":                   colString,
	"BinaryData":               colString,
	"Boolean":                  colBoolean,
	"Integer":                  colBigint,
	"Number":                   colDecimal,
	"Date":                     colDate,
	"TimestampWithTimezone":    colTimestamp,
	"TimestampWithoutTimezone": colTimestampNTZ,
	"TimeWithTimezone":         colString,
	"TimeWithoutTimezone":      colString,
}

// columnType maps the JSON schema of one property to its column type. An
// object, an array, a property with more than one non-null type, a union
// (anyOf, oneOf) or no type at all lands as VARIANT.
func columnType(prop map[string]any) colType {
	if ref, ok := prop["$ref"].(string); ok {
		if _, def, found := strings.Cut(ref, "#/definitions/"); found {
			if t, ok := wellKnownTypes[def]; ok {
				return t
			}
		}
		return colVariant
	}
	types := schemaTypes(prop["type"])
	if len(types) != 1 {
		return colVariant
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
		return colDecimal
	case "string":
		switch {
		case airbyteType == "big_integer":
			return colBigDecimal
		case airbyteType == "integer":
			return colBigint
		case airbyteType == "timestamp_without_timezone":
			return colTimestampNTZ
		case airbyteType == "timestamp_with_timezone":
			return colTimestamp
		case airbyteType == "time_with_timezone", airbyteType == "time_without_timezone":
			return colString
		case format == "date":
			return colDate
		case format == "date-time":
			return colTimestamp
		}
		return colString
	}
	return colVariant
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

var numberText = regexp.MustCompile(`^-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?$`)

// encodeValue renders one JSON value as the text the load casts into a
// column of type t; null reports a JSON null. A value the column cannot hold
// exactly is an error: numbers are never rounded, timestamps never truncated
// and zones never dropped.
func encodeValue(t colType, raw json.RawMessage) (val string, null bool, err error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return "", true, nil
	}
	if badUnicode(raw) {
		return "", false, errors.New("value is not valid UTF-8 (invalid bytes or a lone UTF-16 surrogate)")
	}
	isString := raw[0] == '"'
	var str string
	if isString {
		if err := json.Unmarshal(raw, &str); err != nil {
			return "", false, fmt.Errorf("invalid JSON string: %w", err)
		}
	}
	text := string(raw)
	if isString {
		text = str
	}
	switch t {
	case colVariant:
		if !json.Valid(raw) {
			return "", false, fmt.Errorf("value %s is not valid JSON", raw)
		}
		return string(raw), false, nil
	case colString:
		return text, false, nil
	case colBoolean:
		switch {
		case string(raw) == "true" || (isString && strings.EqualFold(str, "true")):
			return "true", false, nil
		case string(raw) == "false" || (isString && strings.EqualFold(str, "false")):
			return "false", false, nil
		}
		return "", false, fmt.Errorf("value %s is not a boolean", raw)
	case colBigint:
		if _, err := strconv.ParseInt(text, 10, 64); err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return "", false, fmt.Errorf("value %s is outside the BIGINT range", raw)
			}
			return "", false, fmt.Errorf("value %s is not an integer", raw)
		}
		return text, false, nil
	case colDecimal, colBigDecimal:
		return encodeDecimal(t, text, raw)
	case colDate:
		if !isString {
			return "", false, fmt.Errorf("value %s is not a string", raw)
		}
		if _, err := time.Parse(time.DateOnly, str); err != nil {
			return "", false, fmt.Errorf("value %s is not a date (YYYY-MM-DD)", raw)
		}
		return str, false, nil
	case colTimestamp, colTimestampNTZ:
		if !isString {
			return "", false, fmt.Errorf("value %s is not a string", raw)
		}
		ts, zoned, err := parseTimestamp(str)
		if err != nil {
			return "", false, fmt.Errorf("value %s %w", raw, err)
		}
		if t == colTimestampNTZ {
			if zoned {
				return "", false, fmt.Errorf("value %s carries a time zone, which TIMESTAMP_NTZ cannot hold", raw)
			}
			return ts.Format("2006-01-02T15:04:05.000000"), false, nil
		}
		return ts.UTC().Format("2006-01-02T15:04:05.000000Z"), false, nil
	}
	return "", false, fmt.Errorf("unknown column type %s", t)
}

var pow10Scale = new(big.Int).Exp(big.NewInt(10), big.NewInt(decimalScale), nil)

// encodeDecimal renders a number with the scale of its column; the digits it
// needs must fit the column's precision and scale.
func encodeDecimal(t colType, text string, raw json.RawMessage) (string, bool, error) {
	if !numberText.MatchString(text) {
		return "", false, fmt.Errorf("value %s is not a number", raw)
	}
	r, ok := new(big.Rat).SetString(text)
	if !ok {
		return "", false, fmt.Errorf("value %s is not a number", raw)
	}
	scale, intDigits := decimalScale, decimalPrecision-decimalScale
	if t == colBigDecimal {
		scale, intDigits = 0, decimalPrecision
	}
	scaled := new(big.Rat).Set(r)
	if scale > 0 {
		scaled.Mul(scaled, new(big.Rat).SetInt(pow10Scale))
	}
	if !scaled.IsInt() {
		if scale == 0 {
			return "", false, fmt.Errorf("value %s is not an integer, which %s needs", raw, t)
		}
		return "", false, fmt.Errorf("value %s has more than %d digits after the point, which %s cannot hold", raw, scale, t)
	}
	whole := new(big.Int).Quo(r.Num(), r.Denom())
	if len(whole.Abs(whole).String()) > intDigits {
		if scale == 0 {
			return "", false, fmt.Errorf("value %s has more than %d digits, which %s cannot hold", raw, intDigits, t)
		}
		return "", false, fmt.Errorf("value %s has more than %d digits before the point, which %s cannot hold", raw, intDigits, t)
	}
	return r.FloatString(scale), false, nil
}

var timestampText = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})(?:[Tt ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,9}))?)?)?\s*(?:([Zz])|([+-])(\d{2})(?::?(\d{2}))?)?$`)

// parseTimestamp reads a date, or a date and time, with an optional zone (Z
// or a numeric offset). A value without a zone is read as UTC; zoned reports
// whether it had one. Digits finer than microseconds are an error.
func parseTimestamp(s string) (ts time.Time, zoned bool, err error) {
	m := timestampText.FindStringSubmatch(s)
	if m == nil {
		return time.Time{}, false, errors.New("is not a timestamp (YYYY-MM-DD[THH:MM[:SS[.ffffff]]][Z|±HH:MM])")
	}
	date, err := time.Parse(time.DateOnly, m[1])
	if err != nil {
		return time.Time{}, false, errors.New("is not a timestamp: invalid date")
	}
	atoi := func(s string) int { n, _ := strconv.Atoi(s); return n }
	hour, minute, sec := atoi(m[2]), atoi(m[3]), atoi(m[4])
	if hour > 23 || minute > 59 || sec > 59 {
		return time.Time{}, false, errors.New("is not a timestamp: invalid time of day")
	}
	frac := m[5]
	if len(frac) > 6 {
		if strings.Trim(frac[6:], "0") != "" {
			return time.Time{}, false, errors.New("has digits finer than microseconds, which Databricks timestamps cannot hold")
		}
		frac = frac[:6]
	}
	micros := 0
	if frac != "" {
		micros = atoi(frac + strings.Repeat("0", 6-len(frac)))
	}
	loc := time.UTC
	switch {
	case m[6] != "":
		zoned = true
	case m[7] != "":
		zoned = true
		offset := atoi(m[8])*3600 + atoi(m[9])*60
		if atoi(m[8]) > 18 || atoi(m[9]) > 59 {
			return time.Time{}, false, errors.New("is not a timestamp: invalid zone offset")
		}
		if m[7] == "-" {
			offset = -offset
		}
		loc = time.FixedZone("", offset)
	}
	ts = time.Date(date.Year(), date.Month(), date.Day(), hour, minute, sec, micros*1000, loc)
	return ts, zoned, nil
}

// badUnicode reports whether the JSON text raw holds bytes that are not UTF-8
// or a \u escape of a UTF-16 surrogate without its partner; decoding would
// replace either with U+FFFD.
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
