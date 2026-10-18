package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// kind is the JSON type a BSON value is written as.
type kind uint8

const (
	kindNull kind = 1 << iota
	kindBoolean
	kindInteger
	kindNumber
	kindString
	kindObject
	kindArray
)

// kindOf returns the JSON type appendValue writes v as.
func kindOf(v bson.RawValue) kind {
	switch v.Type {
	case bson.TypeNull, bson.TypeUndefined:
		return kindNull
	case bson.TypeBoolean:
		return kindBoolean
	case bson.TypeInt32, bson.TypeInt64:
		return kindInteger
	case bson.TypeDouble:
		if f := v.Double(); math.IsNaN(f) || math.IsInf(f, 0) {
			return kindNull
		}
		return kindNumber
	case bson.TypeDecimal128:
		if d := v.Decimal128(); d.IsNaN() || d.IsInf() != 0 {
			return kindNull
		}
		return kindNumber
	case bson.TypeEmbeddedDocument:
		return kindObject
	case bson.TypeArray:
		return kindArray
	}
	return kindString
}

// stringOf returns the text appendValue writes, as a JSON string, for a
// value of kindString.
func stringOf(v bson.RawValue) (string, error) {
	switch v.Type {
	case bson.TypeString:
		return v.StringValue(), nil
	case bson.TypeSymbol:
		return v.Symbol(), nil
	case bson.TypeObjectID:
		id := v.ObjectID()
		return hex.EncodeToString(id[:]), nil
	case bson.TypeBinary:
		_, data := v.Binary()
		return base64.StdEncoding.EncodeToString(data), nil
	case bson.TypeDateTime:
		if t, ok := dateTime(v.DateTime()); ok {
			return t, nil
		}
	}
	return extJSON(v)
}

// dateTime renders milliseconds since the epoch as an RFC 3339 UTC time,
// for years 0 to 9999, which RFC 3339 can express.
func dateTime(ms int64) (string, bool) {
	t := time.UnixMilli(ms).UTC()
	if t.Year() < 0 || t.Year() > 9999 {
		return "", false
	}
	return t.Format(time.RFC3339Nano), true
}

// extJSON renders v as relaxed MongoDB Extended JSON text.
func extJSON(v bson.RawValue) (string, error) {
	b, err := bson.MarshalExtJSON(bson.D{{Key: "v", Value: v}}, false, false)
	if err != nil {
		return "", err
	}
	return string(bytes.TrimSuffix(bytes.TrimPrefix(b, []byte(`{"v":`)), []byte("}"))), nil
}

// canonicalExtJSON renders v as canonical MongoDB Extended JSON, which
// keeps its BSON type.
func canonicalExtJSON(v bson.RawValue) (json.RawMessage, error) {
	b, err := bson.MarshalExtJSON(bson.D{{Key: "v", Value: v}}, true, false)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(bytes.TrimPrefix(b, []byte(`{"v":`)), []byte("}")), nil
}

// parseCanonicalExtJSON reads a value canonicalExtJSON wrote.
func parseCanonicalExtJSON(raw json.RawMessage) (bson.RawValue, error) {
	var doc bson.Raw
	if err := bson.UnmarshalExtJSON(append(append([]byte(`{"v":`), raw...), '}'), true, &doc); err != nil {
		return bson.RawValue{}, err
	}
	return doc.LookupErr("v")
}

// appendDocument appends doc as a JSON object.
func appendDocument(b []byte, doc bson.Raw) ([]byte, error) {
	elems, err := doc.Elements()
	if err != nil {
		return nil, err
	}
	b = append(b, '{')
	for i, e := range elems {
		if i > 0 {
			b = append(b, ',')
		}
		b = appendString(b, e.Key())
		b = append(b, ':')
		if b, err = appendValue(b, e.Value()); err != nil {
			return nil, fmt.Errorf("field %q: %w", e.Key(), err)
		}
	}
	return append(b, '}'), nil
}

// appendValue appends v as the JSON value of its kind.
func appendValue(b []byte, v bson.RawValue) ([]byte, error) {
	switch kindOf(v) {
	case kindNull:
		return append(b, "null"...), nil
	case kindBoolean:
		return strconv.AppendBool(b, v.Boolean()), nil
	case kindInteger:
		return strconv.AppendInt(b, v.AsInt64(), 10), nil
	case kindNumber:
		if v.Type == bson.TypeDecimal128 {
			s := v.Decimal128().String()
			if !json.Valid([]byte(s)) {
				return nil, fmt.Errorf("decimal %q is not a JSON number", s)
			}
			return append(b, s...), nil
		}
		return strconv.AppendFloat(b, v.Double(), 'g', -1, 64), nil
	case kindObject:
		return appendDocument(b, v.Document())
	case kindArray:
		values, err := v.Array().Values()
		if err != nil {
			return nil, err
		}
		b = append(b, '[')
		for i, item := range values {
			if i > 0 {
				b = append(b, ',')
			}
			if b, err = appendValue(b, item); err != nil {
				return nil, fmt.Errorf("item %d: %w", i, err)
			}
		}
		return append(b, ']'), nil
	}
	s, err := stringOf(v)
	if err != nil {
		return nil, err
	}
	return appendString(b, s), nil
}

const hexDigits = "0123456789abcdef"

// appendString appends s as a JSON string. Invalid UTF-8 becomes U+FFFD.
func appendString(b []byte, s string) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"', '\\':
				b = append(b, '\\', c)
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, s[start:i]...)
			b = append(b, `�`...)
			i++
			start = i
			continue
		}
		i += size
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}
