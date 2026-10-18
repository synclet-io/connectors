package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/shopspring/decimal"
)

// kind is how a property's values are encoded; each kind lands in one
// ClickHouse type.
type kind int

const (
	kindJSON kind = iota
	kindBool
	kindInt
	kindNumber
	kindBigInt
	kindString
	kindDate
	kindTimestampTZ
	kindTimestampNTZ
	kindTime
	kindUUID
)

const (
	numberPrecision = 76
	numberScale     = 38
)

// chType is the ClickHouse type of a kind as system.columns prints it, so a
// type read back from the server compares equal to the one the connector
// wants.
func (k kind) chType() string {
	switch k {
	case kindBool:
		return "Bool"
	case kindInt:
		return "Int64"
	case kindNumber:
		return fmt.Sprintf("Decimal(%d, %d)", numberPrecision, numberScale)
	case kindBigInt:
		return fmt.Sprintf("Decimal(%d, 0)", numberPrecision)
	case kindDate:
		return "Date32"
	case kindTimestampTZ, kindTimestampNTZ:
		return "DateTime64(6, 'UTC')"
	case kindUUID:
		return "UUID"
	default:
		return "String"
	}
}

// versionKind reports whether a column of kind k can be the version of a
// ReplacingMergeTree.
func (k kind) versionKind() bool {
	switch k {
	case kindInt, kindDate, kindTimestampTZ, kindTimestampNTZ:
		return true
	}
	return false
}

// wellKnownTypes maps the definitions of Airbyte's WellKnownTypes.json that
// a property may reference with "$ref".
var wellKnownTypes = map[string]kind{
	"String":                   kindString,
	"BinaryData":               kindString,
	"Boolean":                  kindBool,
	"Integer":                  kindInt,
	"Number":                   kindNumber,
	"Date":                     kindDate,
	"TimestampWithTimezone":    kindTimestampTZ,
	"TimestampWithoutTimezone": kindTimestampNTZ,
	"TimeWithTimezone":         kindTime,
	"TimeWithoutTimezone":      kindTime,
}

// columnType maps the JSON schema of one property to its kind. A property
// with more than one non-null type, a union (anyOf, oneOf) or no type at all
// lands as JSON text.
func columnType(prop map[string]any) kind {
	if ref, ok := prop["$ref"].(string); ok {
		if _, def, found := strings.Cut(ref, "#/definitions/"); found {
			if k, ok := wellKnownTypes[def]; ok {
				return k
			}
		}
		return kindJSON
	}
	types := schemaTypes(prop["type"])
	if len(types) != 1 {
		return kindJSON
	}
	airbyteType, _ := prop["airbyte_type"].(string)
	format, _ := prop["format"].(string)
	switch types[0] {
	case "boolean":
		return kindBool
	case "integer":
		return kindInt
	case "number":
		switch airbyteType {
		case "integer":
			return kindInt
		case "big_integer":
			return kindBigInt
		default:
			return kindNumber
		}
	case "string":
		switch {
		case airbyteType == "big_integer":
			return kindBigInt
		case airbyteType == "integer":
			return kindInt
		case airbyteType == "timestamp_without_timezone":
			return kindTimestampNTZ
		case airbyteType == "timestamp_with_timezone":
			return kindTimestampTZ
		case airbyteType == "time_with_timezone", airbyteType == "time_without_timezone":
			return kindTime
		case format == "date":
			return kindDate
		case format == "date-time":
			return kindTimestampTZ
		case format == "time":
			return kindTime
		}
		return kindString
	}
	return kindJSON
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

// minTime and maxDate bound Date32. DateTime64 holds the same range on the
// server, but the driver encodes a timestamp through time.UnixNano, which
// wraps around after maxTimestamp, so timestamps stop there.
var (
	minTime      = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDate      = time.Date(2299, 12, 31, 0, 0, 0, 0, time.UTC)
	maxTimestamp = time.Unix(0, math.MaxInt64).UTC().Truncate(time.Microsecond)
)

// encodeValue turns one JSON value into the driver value for a column of
// kind k; nil is a JSON null or a missing field. Numbers keep the exact
// digits of the JSON text. A value the column cannot hold exactly is an
// error.
func encodeValue(k kind, raw json.RawMessage) (any, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	if badUnicode(raw) {
		return nil, errors.New("value is not valid UTF-8 (invalid bytes or a lone UTF-16 surrogate)")
	}
	isString := raw[0] == '"'
	var str string
	if isString {
		if err := json.Unmarshal(raw, &str); err != nil {
			return nil, fmt.Errorf("invalid JSON string: %w", err)
		}
	}
	text := string(raw)
	if isString {
		text = str
	}
	switch k {
	case kindJSON:
		if !json.Valid(raw) {
			return nil, fmt.Errorf("value %s is not valid JSON", raw)
		}
		return string(raw), nil
	case kindBool:
		switch text {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("value %s is not a boolean", raw)
	case kindInt:
		if !numberText.MatchString(text) {
			return nil, fmt.Errorf("value %s is not an integer", raw)
		}
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return nil, fmt.Errorf("value %s is outside the Int64 range", raw)
			}
			return nil, fmt.Errorf("value %s is not an integer", raw)
		}
		return n, nil
	case kindNumber:
		return exactDecimal(raw, text, numberScale)
	case kindBigInt:
		return exactDecimal(raw, text, 0)
	case kindString:
		return text, nil
	}

	if !isString {
		return nil, fmt.Errorf("value %s is not a string", raw)
	}
	switch k {
	case kindDate:
		return parseDate(str)
	case kindTimestampTZ:
		return parseTimestamp(str, true)
	case kindTimestampNTZ:
		return parseTimestamp(str, false)
	default:
		return str, nil
	}
}

var ten = big.NewInt(10)

// exactDecimal parses a JSON number, or a number in a JSON string, into a
// decimal of Decimal(76, scale). Digits the column cannot hold fail; trailing
// zeros of the fraction do not count.
func exactDecimal(raw json.RawMessage, text string, scale int) (decimal.Decimal, error) {
	if !numberText.MatchString(text) {
		return decimal.Decimal{}, fmt.Errorf("value %s is not a number", raw)
	}
	d, err := decimal.NewFromString(text)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("value %s is not a number", raw)
	}
	coef := new(big.Int).Abs(d.Coefficient())
	exp := int64(d.Exponent())
	if coef.Sign() == 0 {
		return decimal.Zero, nil
	}
	var rem big.Int
	for {
		q, r := new(big.Int).QuoRem(coef, ten, &rem)
		if r.Sign() != 0 {
			break
		}
		coef = q
		exp++
	}
	if exp < -int64(scale) {
		if scale == 0 {
			return decimal.Decimal{}, fmt.Errorf("value %s has digits after the decimal point, which Decimal(%d, 0) cannot hold", raw, numberPrecision)
		}
		return decimal.Decimal{}, fmt.Errorf("value %s has more than %d digits after the decimal point, which Decimal(%d, %d) cannot hold", raw, scale, numberPrecision, scale)
	}
	intDigits := int64(len(coef.String())) + exp
	if intDigits > int64(numberPrecision-scale) {
		return decimal.Decimal{}, fmt.Errorf("value %s has more than %d digits before the decimal point, which Decimal(%d, %d) cannot hold", raw, numberPrecision-scale, numberPrecision, scale)
	}
	return d, nil
}

var (
	dateText      = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})$`)
	timestampText = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})(?:[Tt ]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?[ ]?([Zz]|[+-][0-9]{2}(?::?[0-9]{2})?)?)?$`)
)

// civil builds a UTC time from its parts and fails when a part is out of
// range, so 2024-02-30 is an error rather than March 1.
func civil(y, mo, d, h, mi, s, ns int) (time.Time, bool) {
	t := time.Date(y, time.Month(mo), d, h, mi, s, ns, time.UTC)
	ok := t.Year() == y && int(t.Month()) == mo && t.Day() == d && t.Hour() == h && t.Minute() == mi && t.Second() == s
	return t, ok
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

// parseDate parses YYYY-MM-DD within Date32's range.
func parseDate(s string) (time.Time, error) {
	m := dateText.FindStringSubmatch(s)
	if m == nil {
		return time.Time{}, fmt.Errorf("value %q is not a date (YYYY-MM-DD)", s)
	}
	t, ok := civil(atoi(m[1]), atoi(m[2]), atoi(m[3]), 0, 0, 0, 0)
	if !ok {
		return time.Time{}, fmt.Errorf("value %q is not a valid date", s)
	}
	if t.Before(minTime) || t.After(maxDate) {
		return time.Time{}, fmt.Errorf("value %q is outside the Date32 range 1900-01-01 to 2299-12-31", s)
	}
	return t, nil
}

// parseTimestamp parses an ISO 8601 date and time into UTC. With withZone a
// value without a zone is UTC; without it, a zone in the value fails, and
// the wall-clock time is stored as UTC. Fraction digits past microseconds
// must be zeros.
func parseTimestamp(s string, withZone bool) (time.Time, error) {
	m := timestampText.FindStringSubmatch(s)
	if m == nil {
		return time.Time{}, fmt.Errorf("value %q is not an ISO 8601 timestamp", s)
	}
	frac := m[7]
	if len(frac) > 6 {
		if strings.Trim(frac[6:], "0") != "" {
			return time.Time{}, fmt.Errorf("value %q has digits past microseconds, which DateTime64(6) cannot hold", s)
		}
		frac = frac[:6]
	}
	ns := 0
	if frac != "" {
		ns = atoi((frac + "000000")[:6]) * 1000
	}
	t, ok := civil(atoi(m[1]), atoi(m[2]), atoi(m[3]), atoi(m[4]), atoi(m[5]), atoi(m[6]), ns)
	if !ok {
		return time.Time{}, fmt.Errorf("value %q is not a valid timestamp", s)
	}
	if zone := m[8]; zone != "" {
		if !withZone {
			return time.Time{}, fmt.Errorf("value %q carries a time zone, which a timestamp without time zone cannot hold", s)
		}
		if zone != "Z" && zone != "z" {
			digits := strings.ReplaceAll(zone[1:], ":", "")
			offset := atoi(digits[:2]) * 3600
			if len(digits) == 4 {
				offset += atoi(digits[2:]) * 60
			}
			if zone[0] == '+' {
				offset = -offset
			}
			t = t.Add(time.Duration(offset) * time.Second)
		}
	}
	if t.Before(minTime) || t.After(maxTimestamp) {
		return time.Time{}, fmt.Errorf("value %q is outside the range 1900-01-01 00:00:00 to 2262-04-11 23:47:16.854775 UTC that the connector writes to DateTime64", s)
	}
	return t, nil
}

// badUnicode reports whether the JSON value raw holds bytes that are not
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
