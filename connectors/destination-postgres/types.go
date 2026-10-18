package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

// colType is a column type as Postgres' format_type prints it, so a type
// read back from the catalog compares equal to the one the connector wants.
type colType string

const (
	colBoolean     colType = "boolean"
	colBigint      colType = "bigint"
	colNumeric     colType = "numeric"
	colText        colType = "text"
	colDate        colType = "date"
	colTimestamptz colType = "timestamp with time zone"
	colTimestamp   colType = "timestamp without time zone"
	colTime        colType = "time without time zone"
	colTimetz      colType = "time with time zone"
	colJSONB       colType = "jsonb"
	colUUID        colType = "uuid"
)

// wellKnownTypes maps the definitions of Airbyte's WellKnownTypes.json that
// a property may reference with "$ref".
var wellKnownTypes = map[string]colType{
	"String":                   colText,
	"BinaryData":               colText,
	"Boolean":                  colBoolean,
	"Integer":                  colBigint,
	"Number":                   colNumeric,
	"Date":                     colDate,
	"TimestampWithTimezone":    colTimestamptz,
	"TimestampWithoutTimezone": colTimestamp,
	"TimeWithTimezone":         colTimetz,
	"TimeWithoutTimezone":      colTime,
}

// columnType maps the JSON schema of one property to its column type. A
// property with more than one non-null type, a union (anyOf, oneOf) or no
// type at all lands as jsonb.
func columnType(prop map[string]any) colType {
	if ref, ok := prop["$ref"].(string); ok {
		if _, def, found := strings.Cut(ref, "#/definitions/"); found {
			if t, ok := wellKnownTypes[def]; ok {
				return t
			}
		}
		return colJSONB
	}
	types := schemaTypes(prop["type"])
	if len(types) != 1 {
		return colJSONB
	}
	airbyteType, _ := prop["airbyte_type"].(string)
	format, _ := prop["format"].(string)
	switch types[0] {
	case "boolean":
		return colBoolean
	case "integer":
		return colBigint
	case "number":
		switch airbyteType {
		case "integer":
			return colBigint
		default:
			return colNumeric
		}
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
		return colText
	}
	return colJSONB
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

// encodeValue renders one JSON value as the text Postgres parses into a
// column of type t; null reports a JSON null. Numbers keep the exact digits
// of the JSON text. A value the column cannot hold is an error; Postgres
// itself rejects dates and times it cannot parse.
func encodeValue(t colType, raw json.RawMessage) (val string, null bool, err error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return "", true, nil
	}
	isString := raw[0] == '"'
	var str string
	if isString {
		if err := json.Unmarshal(raw, &str); err != nil {
			return "", false, fmt.Errorf("invalid JSON string: %w", err)
		}
		if badUnicode(raw) {
			return "", false, errors.New("value is not valid UTF-8 (invalid bytes or a lone UTF-16 surrogate)")
		}
	}
	switch t {
	case colJSONB:
		if bytes.Contains(raw, []byte(`\u0000`)) && containsNUL(raw) {
			return "", false, errors.New(`value contains \u0000, which jsonb cannot hold`)
		}
		return string(raw), false, nil
	case colBoolean:
		switch {
		case string(raw) == "true":
			return "t", false, nil
		case string(raw) == "false":
			return "f", false, nil
		case isString:
			return str, false, nil
		}
		return "", false, fmt.Errorf("value %s is not a boolean", raw)
	case colBigint:
		s := string(raw)
		if isString {
			s = str
		}
		if !numberText.MatchString(s) {
			return "", false, fmt.Errorf("value %s is not an integer", raw)
		}
		if _, err := strconv.ParseInt(s, 10, 64); err != nil {
			if errors.Is(err, strconv.ErrRange) {
				return "", false, fmt.Errorf("value %s is outside the bigint range", raw)
			}
			return "", false, fmt.Errorf("value %s is not an integer", raw)
		}
		return s, false, nil
	case colNumeric:
		s := string(raw)
		if isString {
			s = str
		}
		if !numberText.MatchString(s) {
			return "", false, fmt.Errorf("value %s is not a number", raw)
		}
		return s, false, nil
	case colText:
		if !isString {
			str = string(raw)
		}
		if strings.ContainsRune(str, 0) {
			return "", false, errors.New(`value contains \u0000, which text cannot hold`)
		}
		return str, false, nil
	default:
		if !isString {
			return "", false, fmt.Errorf("value %s is not a string", raw)
		}
		if strings.ContainsRune(str, 0) {
			return "", false, fmt.Errorf(`value contains \u0000, which %s cannot hold`, t)
		}
		if (t == colTimestamp || t == colTime) && hasTimeZone(t, str) {
			return "", false, fmt.Errorf("value %s carries a time zone, which %s cannot hold", raw, t)
		}
		return str, false, nil
	}
}

// badUnicode reports whether the JSON string raw holds bytes that are not
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

// hasTimeZone reports whether a date/time string for a column without a time
// zone names one: Z, a numeric offset or a zone name in its time part (AM,
// PM and BC are not zones). Postgres would drop it silently. The time part of
// a timestamp starts at the "T", "t" or space that follows the date's last
// digit; a lone date or a special value such as infinity has none.
func hasTimeZone(t colType, s string) bool {
	timePart := s
	if t == colTimestamp {
		i := dateEnd(timePart)
		if i < 0 {
			return false
		}
		timePart = timePart[i+1:]
	}
	timePart = strings.ToLower(timePart)
	timePart = strings.NewReplacer("am", "", "pm", "", "bc", "").Replace(timePart)
	for _, c := range timePart {
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

// containsNUL reports whether a JSON value holds a string with a NUL
// character, in a key or a value.
func containsNUL(raw json.RawMessage) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	for {
		tok, err := dec.Token()
		if err != nil {
			return false
		}
		if s, ok := tok.(string); ok && strings.ContainsRune(s, 0) {
			return true
		}
	}
}

// copyEscape appends s to b escaped for COPY's text format.
func copyEscape(b *bytes.Buffer, s string) {
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteByte(c)
		}
	}
}
