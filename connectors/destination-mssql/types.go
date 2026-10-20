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

// kind is what a column holds; it decides how a JSON value is checked and
// rendered.
type kind int

const (
	kindBoolean kind = iota
	kindInteger
	kindNumber
	kindBigInteger
	kindText
	kindDate
	kindTimestampTZ
	kindTimestamp
	kindTime
	kindTimeTZ
	kindJSON
	kindUUID
)

// textCollation compares strings by code unit, so keys and cursors that
// differ only in case or accents stay distinct whatever the database
// collation is.
const textCollation = "Latin1_General_100_BIN2"

// keyTextType is the column type of a string primary key column: an index
// cannot cover NVARCHAR(MAX).
const keyTextType = "NVARCHAR(255)"

// maxKeyText is the most UTF-16 code units a string primary key value may
// have.
const maxKeyText = 255

// sqlType is the column type a kind is created with.
func (k kind) sqlType() string {
	switch k {
	case kindBoolean:
		return "BIT"
	case kindInteger:
		return "BIGINT"
	case kindNumber:
		return "FLOAT(53)"
	case kindBigInteger:
		return "DECIMAL(38,0)"
	case kindDate:
		return "DATE"
	case kindTimestampTZ:
		return "DATETIMEOFFSET(7)"
	case kindTimestamp:
		return "DATETIME2(7)"
	case kindTime:
		return "TIME(7)"
	case kindUUID:
		return "UNIQUEIDENTIFIER"
	default:
		return "NVARCHAR(MAX)"
	}
}

// isText reports whether a column type holds strings, which are created
// with textCollation.
func isText(sqlType string) bool {
	return strings.HasPrefix(sqlType, "NVARCHAR")
}

// wellKnownTypes maps the definitions of Airbyte's WellKnownTypes.json that
// a property may reference with "$ref".
var wellKnownTypes = map[string]kind{
	"String":                   kindText,
	"BinaryData":               kindText,
	"Boolean":                  kindBoolean,
	"Integer":                  kindInteger,
	"Number":                   kindNumber,
	"Date":                     kindDate,
	"TimestampWithTimezone":    kindTimestampTZ,
	"TimestampWithoutTimezone": kindTimestamp,
	"TimeWithTimezone":         kindTimeTZ,
	"TimeWithoutTimezone":      kindTime,
}

// columnKind maps the JSON schema of one property to its column kind. A
// property with more than one non-null type, a union (anyOf, oneOf) or no
// type at all lands as JSON text.
func columnKind(prop map[string]any) kind {
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
		return kindBoolean
	case "integer":
		return kindInteger
	case "number":
		switch airbyteType {
		case "integer":
			return kindInteger
		case "big_integer":
			return kindBigInteger
		default:
			return kindNumber
		}
	case "string":
		switch {
		case airbyteType == "big_integer":
			return kindBigInteger
		case airbyteType == "integer":
			return kindInteger
		case airbyteType == "timestamp_without_timezone":
			return kindTimestamp
		case airbyteType == "timestamp_with_timezone":
			return kindTimestampTZ
		case airbyteType == "time_with_timezone":
			return kindTimeTZ
		case airbyteType == "time_without_timezone":
			return kindTime
		case format == "date":
			return kindDate
		case format == "date-time":
			return kindTimestampTZ
		case format == "time":
			return kindTime
		}
		return kindText
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

var (
	numberText = regexp.MustCompile(`^(-?)([0-9]*)(?:\.([0-9]*))?(?:[eE]([-+]?[0-9]+))?$`)
	dateText   = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})$`)
	timeText   = regexp.MustCompile(`^([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?$`)
	dateTime   = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})` +
		`(?:[Tt ]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?)?` +
		` ?([Zz]|[+-][0-9]{2}(?::?[0-9]{2})?)?$`)
)

const (
	// bigIntDigits is the precision of DECIMAL(38,0).
	bigIntDigits = 38
	// maxFracSeconds is the fractional-second precision of DATETIME2(7),
	// DATETIMEOFFSET(7) and TIME(7).
	maxFracSeconds = 7
)

// encodeValue renders one JSON value as the value bulk-copied into a column
// of kind k: bool, int64, float64, string, time.Time, or nil for a JSON null.
// A value SQL Server would round, truncate or reject is an error.
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
			return nil, errors.New("invalid JSON value")
		}
		return string(raw), nil
	case kindBoolean:
		switch strings.ToLower(text) {
		case "true", "1":
			return true, nil
		case "false", "0":
			return false, nil
		}
		return nil, fmt.Errorf("value %s is not a boolean", raw)
	case kindInteger:
		digits, err := plainInteger(text, 19)
		if err == nil {
			var n int64
			if n, err = strconv.ParseInt(digits, 10, 64); err == nil {
				return n, nil
			}
		}
		if errors.Is(err, errNotInteger) {
			return nil, fmt.Errorf("value %s is not an integer", raw)
		}
		return nil, fmt.Errorf("value %s is outside the BIGINT range", raw)
	case kindNumber:
		return encodeFloat(raw, text)
	case kindBigInteger:
		digits, err := plainInteger(text, bigIntDigits)
		switch {
		case errors.Is(err, errNotInteger):
			return nil, fmt.Errorf("value %s is not an integer", raw)
		case err != nil:
			return nil, fmt.Errorf("value %s has more than %d digits, which DECIMAL(38,0) cannot hold", raw, bigIntDigits)
		}
		return digits, nil
	case kindText, kindTimeTZ:
		return text, nil
	}
	if !isString {
		return nil, fmt.Errorf("value %s is not a string", raw)
	}
	switch k {
	case kindDate:
		return encodeDate(str, raw)
	case kindTimestampTZ, kindTimestamp:
		return encodeDateTime(k, str, raw)
	case kindTime:
		return encodeTime(str, raw)
	}
	return str, nil
}

// encodeFloat accepts a number only when the float64 nearest to it prints
// back as the same decimal value, so FLOAT holds it exactly as the source
// wrote it.
func encodeFloat(raw json.RawMessage, text string) (any, error) {
	m := numberText.FindStringSubmatch(text)
	if m == nil || (m[2] == "" && m[3] == "") {
		return nil, fmt.Errorf("value %s is not a number", raw)
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, fmt.Errorf("value %s is outside the FLOAT range", raw)
	}
	out := strconv.FormatFloat(f, 'g', -1, 64)
	want, ok1 := new(big.Rat).SetString(text)
	got, ok2 := new(big.Rat).SetString(out)
	if !ok1 || !ok2 || want.Cmp(got) != 0 {
		return nil, fmt.Errorf("value %s has more precision than FLOAT, which cannot hold it exactly (nearest is %s)", raw, out)
	}
	return f, nil
}

var (
	errNotInteger = errors.New("not an integer")
	errTooLong    = errors.New("too many digits")
)

// plainInteger renders a JSON number, or a number given as a string, that is
// an exact integer (1, 1.0, 1e3) as plain digits. It fails for a fraction or
// more than maxDigits digits.
func plainInteger(s string, maxDigits int) (string, error) {
	m := numberText.FindStringSubmatch(s)
	if m == nil || (m[2] == "" && m[3] == "") {
		return "", errNotInteger
	}
	sign, intPart, fracPart := m[1], m[2], m[3]
	all := intPart + fracPart
	// point is the position of the decimal point within all.
	point := len(intPart)
	if m[4] != "" {
		exp, err := strconv.Atoi(m[4])
		if err != nil || exp > 1000 || exp < -1000 {
			return "", errTooLong
		}
		point += exp
	}
	switch {
	case point < 0:
		all = strings.Repeat("0", -point) + all
		point = 0
	case point > len(all):
		all += strings.Repeat("0", point-len(all))
	}
	if strings.TrimRight(all[point:], "0") != "" {
		return "", errNotInteger
	}
	ip := strings.TrimLeft(all[:point], "0")
	if len(ip) > maxDigits {
		return "", errTooLong
	}
	if ip == "" {
		return "0", nil
	}
	return sign + ip, nil
}

func encodeDate(s string, raw json.RawMessage) (any, error) {
	m := dateText.FindStringSubmatch(s)
	if m == nil {
		return nil, fmt.Errorf("value %s is not a date (YYYY-MM-DD)", raw)
	}
	year, month, day := atoi(m[1]), atoi(m[2]), atoi(m[3])
	t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if year < 1 || t.Year() != year || int(t.Month()) != month || t.Day() != day {
		return nil, fmt.Errorf("value %s is not a valid date between 0001-01-01 and 9999-12-31", raw)
	}
	return t, nil
}

// encodeDateTime parses a date-time. A timestamp with a time zone is
// converted to UTC, one without a zone is taken as UTC; a timestamp without
// time zone must not carry one.
func encodeDateTime(k kind, s string, raw json.RawMessage) (any, error) {
	m := dateTime.FindStringSubmatch(s)
	if m == nil {
		if k == kindTimestamp && hasTimeZone(s) {
			return nil, fmt.Errorf("value %s carries a time zone, which a timestamp without time zone cannot hold", raw)
		}
		return nil, fmt.Errorf("value %s is not a date-time (YYYY-MM-DDTHH:MM:SS[.fffffff][Z|±HH:MM])", raw)
	}
	if m[8] != "" && k == kindTimestamp {
		return nil, fmt.Errorf("value %s carries a time zone, which a timestamp without time zone cannot hold", raw)
	}
	nanos, err := fraction(m[7], raw)
	if err != nil {
		return nil, err
	}
	year, month, day, hour, minute, sec := atoi(m[1]), atoi(m[2]), atoi(m[3]), atoi(m[4]), atoi(m[5]), atoi(m[6])
	loc := time.UTC
	if z := m[8]; z != "" && z != "Z" && z != "z" {
		digits := strings.ReplaceAll(z[1:], ":", "")
		h, mins := atoi(digits[:2]), 0
		if len(digits) == 4 {
			mins = atoi(digits[2:])
		}
		if h > 23 || mins > 59 {
			return nil, fmt.Errorf("value %s has an invalid time zone offset", raw)
		}
		offset := h*3600 + mins*60
		if z[0] == '-' {
			offset = -offset
		}
		loc = time.FixedZone("", offset)
	}
	t := time.Date(year, time.Month(month), day, hour, minute, sec, nanos, loc)
	if t.Year() != year || int(t.Month()) != month || t.Day() != day || t.Hour() != hour || t.Minute() != minute || t.Second() != sec {
		return nil, fmt.Errorf("value %s is not a valid date-time", raw)
	}
	t = t.UTC()
	if t.Year() < 1 || t.Year() > 9999 {
		return nil, fmt.Errorf("value %s is outside 0001-01-01 to 9999-12-31 in UTC", raw)
	}
	return t, nil
}

func encodeTime(s string, raw json.RawMessage) (any, error) {
	if hasTimeZone(s) {
		return nil, fmt.Errorf("value %s carries a time zone, which TIME(7) cannot hold", raw)
	}
	m := timeText.FindStringSubmatch(s)
	if m == nil {
		return nil, fmt.Errorf("value %s is not a time (HH:MM:SS[.fffffff])", raw)
	}
	nanos, err := fraction(m[4], raw)
	if err != nil {
		return nil, err
	}
	hour, minute, sec := atoi(m[1]), atoi(m[2]), atoi(m[3])
	if hour > 23 || minute > 59 || sec > 59 {
		return nil, fmt.Errorf("value %s is not a valid time of day", raw)
	}
	return time.Date(1, 1, 1, hour, minute, sec, nanos, time.UTC), nil
}

// fraction returns fractional seconds as nanoseconds. It fails when they
// have more non-zero digits than SQL Server keeps; it would round them.
func fraction(frac string, raw json.RawMessage) (int, error) {
	frac = strings.TrimRight(frac, "0")
	if len(frac) > maxFracSeconds {
		return 0, fmt.Errorf("value %s has more than %d fractional-second digits, which SQL Server would round", raw, maxFracSeconds)
	}
	return atoi((frac + "000000000")[:9]), nil
}

func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
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

// hasTimeZone reports whether a time, or the time part of a timestamp,
// names a zone: Z, a numeric offset or a zone name.
func hasTimeZone(s string) bool {
	timePart := s
	if i := dateEnd(s); i >= 0 {
		timePart = s[i+1:]
	} else if dateText.MatchString(s) {
		return false
	}
	for _, c := range strings.ToLower(timePart) {
		if c == '+' || c == '-' || (c >= 'a' && c <= 'z') {
			return true
		}
	}
	return false
}

// dateEnd returns the index of the separator between the date and the time
// of a timestamp, or -1 when s has no time part.
func dateEnd(s string) int {
	for i := 1; i < len(s); i++ {
		if (s[i] == 'T' || s[i] == 't' || s[i] == ' ') && s[i-1] >= '0' && s[i-1] <= '9' {
			return i
		}
	}
	return -1
}
