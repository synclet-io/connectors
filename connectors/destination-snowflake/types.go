package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// colType is a column type as the connector creates it and as
// infoSchemaType reads it back.
type colType string

const (
	colBoolean      colType = "BOOLEAN"
	colInteger      colType = "NUMBER(38,0)"
	colNumber       colType = "NUMBER(38,9)"
	colVarchar      colType = "VARCHAR"
	colDate         colType = "DATE"
	colTime         colType = "TIME"
	colTimestampTZ  colType = "TIMESTAMP_TZ"
	colTimestampNTZ colType = "TIMESTAMP_NTZ"
	colVariant      colType = "VARIANT"
)

const (
	numberPrecision = 38
	numberScale     = 9
	// maxValueBytes is the largest VARCHAR or VARIANT value Snowflake stores.
	maxValueBytes = 16 << 20
	// maxExponent bounds the exponent of a number before it is expanded.
	maxExponent = 1000
)

// Snowflake formats of the text encodeValue writes for each type.
const (
	fmtDate  = "YYYY-MM-DD"
	fmtTime  = "HH24:MI:SS.FF9"
	fmtNTZ   = "YYYY-MM-DD HH24:MI:SS.FF9"
	fmtTZ    = "YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM"
	maxDigit = 9
)

// wellKnownTypes maps the definitions of Airbyte's WellKnownTypes.json that
// a property may reference with "$ref".
var wellKnownTypes = map[string]colType{
	"String":                   colVarchar,
	"BinaryData":               colVarchar,
	"Boolean":                  colBoolean,
	"Integer":                  colInteger,
	"Number":                   colNumber,
	"Date":                     colDate,
	"TimestampWithTimezone":    colTimestampTZ,
	"TimestampWithoutTimezone": colTimestampNTZ,
	"TimeWithTimezone":         colVarchar,
	"TimeWithoutTimezone":      colTime,
}

// columnType maps the JSON schema of one property to its column type. A
// property with more than one non-null type, a union (anyOf, oneOf) or no
// type at all lands as VARIANT, and so do objects and arrays.
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
		return colInteger
	case "number":
		if airbyteType == "integer" {
			return colInteger
		}
		return colNumber
	case "string":
		switch {
		case airbyteType == "big_integer", airbyteType == "integer":
			return colInteger
		case airbyteType == "timestamp_without_timezone":
			return colTimestampNTZ
		case airbyteType == "timestamp_with_timezone":
			return colTimestampTZ
		case airbyteType == "time_with_timezone":
			return colVarchar
		case airbyteType == "time_without_timezone":
			return colTime
		case format == "date":
			return colDate
		case format == "date-time":
			return colTimestampTZ
		case format == "time":
			return colTime
		}
		return colVarchar
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

// infoSchemaType is the colType of a column as INFORMATION_SCHEMA.COLUMNS
// describes it.
func infoSchemaType(dataType string, precision, scale int64) colType {
	switch dataType {
	case "NUMBER":
		return colType(fmt.Sprintf("NUMBER(%d,%d)", precision, scale))
	case "TEXT":
		return colVarchar
	}
	return colType(dataType)
}

// castSQL converts expr, a staged VARCHAR in the text encodeValue writes,
// to a value of type t.
func castSQL(t colType, expr string) string {
	switch t {
	case colInteger:
		return fmt.Sprintf("TO_NUMBER(%s, %d, 0)", expr, numberPrecision)
	case colNumber:
		return fmt.Sprintf("TO_NUMBER(%s, %d, %d)", expr, numberPrecision, numberScale)
	case colBoolean:
		return "TO_BOOLEAN(" + expr + ")"
	case colDate:
		return "TO_DATE(" + expr + ", '" + fmtDate + "')"
	case colTime:
		return "TO_TIME(" + expr + ", '" + fmtTime + "')"
	case colTimestampNTZ:
		return "TO_TIMESTAMP_NTZ(" + expr + ", '" + fmtNTZ + "')"
	case colTimestampTZ:
		return "TO_TIMESTAMP_TZ(" + expr + ", '" + fmtTZ + "')"
	case colVariant:
		return "PARSE_JSON(" + expr + ")"
	}
	return expr
}

// encodeValue renders one JSON value as the text castSQL converts into a
// column of type t; null reports a JSON null or a missing value. A value the
// column cannot hold exactly is an error, never rounded or truncated.
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
	switch t {
	case colVariant:
		if len(raw) > maxValueBytes {
			return "", false, errors.New("value is longer than 16 MiB")
		}
		if err := checkVariantNumbers(raw); err != nil {
			return "", false, err
		}
		return string(raw), false, nil
	case colVarchar:
		s := string(raw)
		if isString {
			s = str
		}
		if len(s) > maxValueBytes {
			return "", false, errors.New("value is longer than 16 MiB")
		}
		return s, false, nil
	case colBoolean:
		s := string(raw)
		if isString {
			s = str
		}
		if s != "true" && s != "false" {
			return "", false, fmt.Errorf("value %s is not a boolean", raw)
		}
		return s, false, nil
	case colInteger, colNumber:
		s := string(raw)
		if isString {
			s = str
		} else if raw[0] != '-' && (raw[0] < '0' || raw[0] > '9') {
			return "", false, fmt.Errorf("value %s is not a number", raw)
		}
		return encodeDecimal(t, s)
	case colDate:
		if !isString || !validDate(str) {
			return "", false, fmt.Errorf("value %s is not a date (YYYY-MM-DD)", raw)
		}
		return str, false, nil
	case colTime:
		if !isString {
			return "", false, fmt.Errorf("value %s is not a time", raw)
		}
		return encodeTime(str)
	case colTimestampTZ, colTimestampNTZ:
		if !isString {
			return "", false, fmt.Errorf("value %s is not a timestamp", raw)
		}
		return encodeTimestamp(t, str)
	}
	return "", false, fmt.Errorf("unknown column type %s", t)
}

var decimalText = regexp.MustCompile(`^(-?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([-+]?[0-9]+))?$`)

// encodeDecimal expands s, a decimal number with an optional exponent, to
// plain digits and checks that NUMBER(38,0) or NUMBER(38,9) holds it
// exactly.
func encodeDecimal(t colType, s string) (string, bool, error) {
	m := decimalText.FindStringSubmatch(s)
	if m == nil || m[2]+m[3] == "" {
		return "", false, fmt.Errorf("value %q is not a number", s)
	}
	digits := m[2] + m[3]
	point := len(m[2])
	if m[4] != "" {
		exp, err := strconv.Atoi(m[4])
		if err != nil || exp > maxExponent || exp < -maxExponent {
			return "", false, fmt.Errorf("value %q is out of range", s)
		}
		point += exp
	}
	var intPart, frac string
	switch {
	case point <= 0:
		frac = strings.Repeat("0", -point) + digits
	case point >= len(digits):
		intPart = digits + strings.Repeat("0", point-len(digits))
	default:
		intPart, frac = digits[:point], digits[point:]
	}
	intPart = strings.TrimLeft(intPart, "0")
	frac = strings.TrimRight(frac, "0")

	scale := 0
	if t == colNumber {
		scale = numberScale
	}
	switch {
	case t == colInteger && frac != "":
		return "", false, fmt.Errorf("value %q is not an integer", s)
	case t == colInteger && len(intPart) > numberPrecision:
		return "", false, fmt.Errorf("value %q has more than %d digits", s, numberPrecision)
	case len(frac) > scale:
		return "", false, fmt.Errorf("value %q has more than %d digits after the decimal point; %s would round it", s, scale, t)
	case len(intPart) > numberPrecision-scale:
		return "", false, fmt.Errorf("value %q has more than %d digits before the decimal point, more than %s holds", s, numberPrecision-scale, t)
	}
	out := intPart
	if out == "" {
		out = "0"
	}
	if frac != "" {
		out += "." + frac
	}
	if m[1] == "-" && out != "0" {
		out = "-" + out
	}
	return out, false, nil
}

var (
	dateText = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})$`)
	timeText = regexp.MustCompile(`^([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]{1,9}))?)?([zZ]|[+-][0-9]{2}(?::?[0-9]{2})?)?$`)
	tsText   = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]{1,9}))?)?([zZ]|[+-][0-9]{2}(?::?[0-9]{2})?)?$`)
)

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// validDate reports whether s is a calendar date YYYY-MM-DD.
func validDate(s string) bool {
	m := dateText.FindStringSubmatch(s)
	return m != nil && validYMD(atoi(m[1]), atoi(m[2]), atoi(m[3]))
}

func validYMD(y, mo, d int) bool {
	t := time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC)
	return t.Year() == y && int(t.Month()) == mo && t.Day() == d
}

func validClock(h, mi, s int) bool { return h < 24 && mi < 60 && s < 60 }

// nanos pads a fraction of a second to nine digits.
func nanos(frac string) string { return frac + strings.Repeat("0", maxDigit-len(frac)) }

func encodeTime(s string) (string, bool, error) {
	m := timeText.FindStringSubmatch(s)
	if m == nil || !validClock(atoi(m[1]), atoi(m[2]), atoi(m[3])) {
		return "", false, fmt.Errorf("value %q is not a time (HH:MM[:SS[.fffffffff]])", s)
	}
	if m[5] != "" {
		return "", false, fmt.Errorf("value %q has a time zone, which a TIME column would drop", s)
	}
	return fmt.Sprintf("%s:%s:%02d.%s", m[1], m[2], atoi(m[3]), nanos(m[4])), false, nil
}

// encodeTimestamp parses an ISO 8601 timestamp. A TIMESTAMP_TZ value keeps
// its own offset, UTC when it has none; a TIMESTAMP_NTZ value must have no
// zone.
func encodeTimestamp(t colType, s string) (string, bool, error) {
	m := tsText.FindStringSubmatch(s)
	if m == nil || !validYMD(atoi(m[1]), atoi(m[2]), atoi(m[3])) || !validClock(atoi(m[4]), atoi(m[5]), atoi(m[6])) {
		return "", false, fmt.Errorf("value %q is not a timestamp (YYYY-MM-DDTHH:MM[:SS[.fffffffff]][Z|+HH:MM])", s)
	}
	out := fmt.Sprintf("%s-%s-%s %s:%s:%02d.%s", m[1], m[2], m[3], m[4], m[5], atoi(m[6]), nanos(m[7]))
	zone := m[8]
	if t == colTimestampNTZ {
		if zone != "" {
			return "", false, fmt.Errorf("value %q has a time zone, which a TIMESTAMP_NTZ column would drop", s)
		}
		return out, false, nil
	}
	offset := "+00:00"
	if zone != "" && zone != "Z" && zone != "z" {
		hm := strings.ReplaceAll(zone[1:], ":", "")
		mins := "00"
		if len(hm) == 4 {
			mins = hm[2:]
		}
		if atoi(hm[:2]) > 23 || atoi(mins) > 59 {
			return "", false, fmt.Errorf("value %q has an invalid time zone offset", s)
		}
		offset = zone[:1] + hm[:2] + ":" + mins
	}
	return out + " " + offset, false, nil
}

// checkVariantNumbers fails on a number Snowflake would store in a VARIANT
// as a double: one with more than 38 significant digits.
func checkVariantNumbers(raw json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil
		}
		n, ok := tok.(json.Number)
		if !ok {
			continue
		}
		mant, _, _ := strings.Cut(strings.ToLower(n.String()), "e")
		mant = strings.TrimLeft(strings.ReplaceAll(strings.TrimPrefix(mant, "-"), ".", ""), "0")
		mant = strings.TrimRight(mant, "0")
		if len(mant) > numberPrecision {
			return fmt.Errorf("number %s has more than %d significant digits, which a VARIANT stores as a double", n, numberPrecision)
		}
	}
}

// badUnicode reports invalid UTF-8 in raw, or a \u escape of a UTF-16
// surrogate that is not part of a pair.
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
	n, err := strconv.ParseUint(string(b[2:6]), 16, 32)
	if err != nil {
		return 0, false
	}
	return rune(n), true
}
