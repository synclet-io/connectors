package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// keyTextType is the column type of a string primary key column: an index
// cannot cover LONGTEXT, and three such columns fit InnoDB's 3,072-byte key.
const keyTextType = "VARCHAR(255)"

// maxKeyText is the most characters a string primary key value may have.
const maxKeyText = 255

// sqlType is the column type a kind is created with.
func (k kind) sqlType() string {
	switch k {
	case kindBoolean:
		return "BOOLEAN"
	case kindInteger:
		return "BIGINT"
	case kindNumber:
		return "DECIMAL(65,30)"
	case kindBigInteger:
		return "DECIMAL(65,0)"
	case kindDate:
		return "DATE"
	case kindTimestampTZ, kindTimestamp:
		return "DATETIME(6)"
	case kindTime:
		return "TIME(6)"
	case kindJSON:
		return "JSON"
	case kindUUID:
		return "CHAR(36)"
	default:
		return "LONGTEXT"
	}
}

// infoType is a column type as information_schema.COLUMNS.COLUMN_TYPE
// reports it.
func infoType(sqlType string) string {
	if sqlType == "BOOLEAN" {
		return "tinyint(1)"
	}
	return strings.ToLower(sqlType)
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
// type at all lands as JSON.
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
	dateText   = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	timeText   = regexp.MustCompile(`^[0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.([0-9]+))?)?$`)
	dateTime   = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})` +
		`(?:[Tt ]([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]+))?)?)?` +
		` ?([Zz]|[+-][0-9]{2}(?::?[0-9]{2})?)?$`)
)

// Digits DECIMAL(65,30) holds on either side of the point.
const (
	numberIntDigits  = 35
	numberFracDigits = 30
	bigIntDigits     = 65
	maxFracSeconds   = 6
)

// encodeValue renders one JSON value as the value bound for a column of kind
// k: a string, an int64 or nil for a JSON null. A value MySQL would round,
// truncate or reinterpret is an error; MySQL itself rejects dates it cannot
// hold.
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
		if err := exactJSONNumbers(raw); err != nil {
			return nil, err
		}
		return string(raw), nil
	case kindBoolean:
		switch strings.ToLower(text) {
		case "true", "1":
			return int64(1), nil
		case "false", "0":
			return int64(0), nil
		}
		return nil, fmt.Errorf("value %s is not a boolean", raw)
	case kindInteger:
		n, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return nil, fmt.Errorf("value %s is outside the BIGINT range", raw)
			}
			return nil, fmt.Errorf("value %s is not an integer", raw)
		}
		return n, nil
	case kindNumber:
		return plainDecimal(text, numberIntDigits, numberFracDigits, k)
	case kindBigInteger:
		return plainDecimal(text, bigIntDigits, 0, k)
	case kindText:
		return text, nil
	}
	if !isString {
		return nil, fmt.Errorf("value %s is not a string", raw)
	}
	switch k {
	case kindDate:
		if !dateText.MatchString(str) {
			return nil, fmt.Errorf("value %s is not a date (YYYY-MM-DD)", raw)
		}
		return str, nil
	case kindTimestampTZ, kindTimestamp:
		return encodeDateTime(k, str, raw)
	case kindTime:
		if hasTimeZone(str) {
			return nil, fmt.Errorf("value %s carries a time zone, which TIME(6) cannot hold", raw)
		}
		m := timeText.FindStringSubmatch(str)
		if m == nil {
			return nil, fmt.Errorf("value %s is not a time (HH:MM:SS[.ffffff])", raw)
		}
		if err := checkFraction(m[1], raw); err != nil {
			return nil, err
		}
		return str, nil
	}
	return str, nil
}

// encodeDateTime renders a date-time as "YYYY-MM-DD HH:MM:SS.ffffff". A
// timestamp with a time zone is converted to UTC, one without a zone is taken
// as UTC; a timestamp without time zone must not carry one.
func encodeDateTime(k kind, s string, raw json.RawMessage) (any, error) {
	m := dateTime.FindStringSubmatch(s)
	if m == nil {
		if k == kindTimestamp && hasTimeZone(s) {
			return nil, fmt.Errorf("value %s carries a time zone, which a timestamp without time zone cannot hold", raw)
		}
		return nil, fmt.Errorf("value %s is not a date-time (YYYY-MM-DDTHH:MM:SS[.ffffff][Z|±HH:MM])", raw)
	}
	if m[8] != "" && k == kindTimestamp {
		return nil, fmt.Errorf("value %s carries a time zone, which a timestamp without time zone cannot hold", raw)
	}
	if err := checkFraction(m[7], raw); err != nil {
		return nil, err
	}
	num := func(i int) int {
		n, _ := strconv.Atoi(m[i])
		return n
	}
	year, month, day, hour, minute, sec := num(1), num(2), num(3), num(4), num(5), num(6)
	frac := (m[7] + "000000")[:6]
	nanos, _ := strconv.Atoi(frac)
	loc := time.UTC
	if z := m[8]; z != "" && z != "Z" && z != "z" {
		digits := strings.ReplaceAll(z[1:], ":", "")
		h, _ := strconv.Atoi(digits[:2])
		mins := 0
		if len(digits) == 4 {
			mins, _ = strconv.Atoi(digits[2:])
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
	t := time.Date(year, time.Month(month), day, hour, minute, sec, nanos*1000, loc)
	if t.Year() != year || int(t.Month()) != month || t.Day() != day || t.Hour() != hour || t.Minute() != minute || t.Second() != sec {
		return nil, fmt.Errorf("value %s is not a valid date-time", raw)
	}
	return t.UTC().Format("2006-01-02 15:04:05.000000"), nil
}

// checkFraction fails when fractional seconds have more non-zero digits than
// MySQL keeps; it would round them.
func checkFraction(frac string, raw json.RawMessage) error {
	if len(strings.TrimRight(frac, "0")) > maxFracSeconds {
		return fmt.Errorf("value %s has more than %d fractional-second digits, which MySQL would round", raw, maxFracSeconds)
	}
	return nil
}

// plainDecimal renders a JSON number, or a number given as a string, in plain
// digits without an exponent. It fails when the value has more integer or
// fraction digits than the column holds, ignoring leading and trailing zeros.
func plainDecimal(s string, maxInt, maxFrac int, k kind) (string, error) {
	m := numberText.FindStringSubmatch(s)
	if m == nil || (m[2] == "" && m[3] == "") {
		return "", fmt.Errorf("value %q is not a number", s)
	}
	sign, intPart, fracPart := m[1], m[2], m[3]
	digits := strings.TrimLeft(intPart+fracPart, "0")
	if digits == "" {
		return "0", nil
	}
	// point is the position of the decimal point within intPart+fracPart.
	point := len(intPart)
	if m[4] != "" {
		exp, err := strconv.Atoi(m[4])
		if err != nil || exp > 1000 || exp < -1000 {
			return "", fmt.Errorf("value %q does not fit %s", s, k.sqlType())
		}
		point += exp
	}
	all := intPart + fracPart
	switch {
	case point <= 0:
		all = strings.Repeat("0", -point) + all
		point = 0
	case point > len(all):
		all += strings.Repeat("0", point-len(all))
	}
	ip := strings.TrimLeft(all[:point], "0")
	fp := strings.TrimRight(all[point:], "0")
	if len(ip) > maxInt {
		return "", fmt.Errorf("value %q has more than %d integer digits, which %s cannot hold", s, maxInt, k.sqlType())
	}
	if len(fp) > maxFrac {
		if maxFrac == 0 {
			return "", fmt.Errorf("value %q is not an integer, which %s needs", s, k.sqlType())
		}
		return "", fmt.Errorf("value %q has more than %d fraction digits, which %s would round", s, maxFrac, k.sqlType())
	}
	if ip == "" {
		ip = "0"
	}
	out := sign + ip
	if fp != "" {
		out += "." + fp
	}
	return out, nil
}

// exactJSONNumbers fails when a number inside a JSON value would change when
// MySQL stores it: MySQL keeps an integer that fits 64 bits and turns every
// other number into a double.
func exactJSONNumbers(raw json.RawMessage) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("invalid JSON: %w", err)
		}
		n, ok := tok.(json.Number)
		if !ok {
			continue
		}
		s := n.String()
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			continue
		}
		if _, err := strconv.ParseUint(s, 10, 64); err == nil {
			continue
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return fmt.Errorf("number %s in a JSON value is outside the range of a double", s)
		}
		exact, ok1 := new(big.Rat).SetString(s)
		stored, ok2 := new(big.Rat).SetString(strconv.FormatFloat(f, 'g', -1, 64))
		if !ok1 || !ok2 || exact.Cmp(stored) != 0 {
			return fmt.Errorf("number %s in a JSON value would be stored as the double %s", s, strconv.FormatFloat(f, 'g', -1, 64))
		}
	}
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
