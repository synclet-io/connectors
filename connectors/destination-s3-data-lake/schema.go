package main

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/apache/iceberg-go"
)

const (
	colRawID       = "_airbyte_raw_id"
	colExtractedAt = "_airbyte_extracted_at"
)

// columnKind is how a declared property is typed in its table.
type columnKind int

const (
	kindJSON columnKind = iota
	kindString
	kindBoolean
	kindInteger
	kindNumber
	kindBigInteger
	kindDate
	kindTimestampTZ
	kindTimestampNTZ
	kindTimeTZ
	kindTime
	kindRawID
	kindExtractedAt
)

// column is one column a stream writes: a metadata column or a declared
// top-level property of its JSON schema.
type column struct {
	name string
	kind columnKind
	typ  iceberg.Type
}

// metaColumns are written to every table ahead of the catalog's columns.
func metaColumns() []column {
	return []column{
		{name: colRawID, kind: kindRawID, typ: iceberg.PrimitiveTypes.String},
		{name: colExtractedAt, kind: kindExtractedAt, typ: iceberg.PrimitiveTypes.TimestampTz},
	}
}

// icebergType is the Iceberg type of a column kind.
func icebergType(kind columnKind, numberType string, scale int) iceberg.Type {
	switch kind {
	case kindBoolean:
		return iceberg.PrimitiveTypes.Bool
	case kindInteger:
		return iceberg.PrimitiveTypes.Int64
	case kindNumber:
		if numberType == numberTypeDouble {
			return iceberg.PrimitiveTypes.Float64
		}
		return iceberg.DecimalTypeOf(maxDecimalPrecision, scale)
	case kindBigInteger:
		return iceberg.DecimalTypeOf(maxDecimalPrecision, 0)
	case kindDate:
		return iceberg.PrimitiveTypes.Date
	case kindTimestampTZ, kindExtractedAt:
		return iceberg.PrimitiveTypes.TimestampTz
	case kindTimestampNTZ:
		return iceberg.PrimitiveTypes.Timestamp
	case kindTime:
		return iceberg.PrimitiveTypes.Time
	default:
		return iceberg.PrimitiveTypes.String
	}
}

// streamColumns returns the metadata columns followed by the stream's
// top-level properties in schema order.
func streamColumns(schema json.RawMessage, cfg Config) ([]column, error) {
	cols := metaColumns()
	if len(bytes.TrimSpace(schema)) == 0 {
		return cols, nil
	}
	var top struct {
		Properties json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(schema, &top); err != nil {
		return nil, fmt.Errorf("json_schema: %w", err)
	}
	names, err := objectKeys(top.Properties)
	if err != nil {
		return nil, fmt.Errorf("json_schema properties: %w", err)
	}
	if len(names) == 0 {
		return cols, nil
	}
	var props map[string]json.RawMessage
	if err := json.Unmarshal(top.Properties, &props); err != nil {
		return nil, fmt.Errorf("json_schema properties: %w", err)
	}
	for _, n := range names {
		switch n {
		case "":
			return nil, fmt.Errorf("property name is empty")
		case colRawID, colExtractedAt:
			return nil, fmt.Errorf("property %q collides with a metadata column", n)
		}
		kind, err := propertyKind(props[n])
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", n, err)
		}
		cols = append(cols, column{name: n, kind: kind, typ: icebergType(kind, cfg.NumberType, *cfg.DecimalScale)})
	}
	return cols, nil
}

// objectKeys returns the keys of a JSON object in document order, each once.
func objectKeys(raw json.RawMessage) ([]string, error) {
	if len(bytes.TrimSpace(raw)) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("expected an object, got %v", tok)
	}
	var keys []string
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, _ := tok.(string)
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return nil, err
		}
		if !seen[key] {
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys, nil
}

type propertySchema struct {
	Type        json.RawMessage   `json:"type"`
	Format      string            `json:"format"`
	AirbyteType string            `json:"airbyte_type"`
	AnyOf       []json.RawMessage `json:"anyOf"`
	OneOf       []json.RawMessage `json:"oneOf"`
}

// propertyKind maps a property schema to its column kind: a union of
// several non-null types, anyOf, oneOf, an object, an array or no type is
// JSON.
func propertyKind(raw json.RawMessage) (columnKind, error) {
	var p propertySchema
	if err := json.Unmarshal(raw, &p); err != nil {
		return 0, err
	}
	if len(p.AnyOf) > 0 || len(p.OneOf) > 0 {
		return kindJSON, nil
	}
	types, err := nonNullTypes(p.Type)
	if err != nil {
		return 0, err
	}
	if len(types) != 1 {
		return kindJSON, nil
	}
	switch types[0] {
	case "boolean":
		return kindBoolean, nil
	case "integer":
		if p.AirbyteType == "big_integer" {
			return kindBigInteger, nil
		}
		return kindInteger, nil
	case "number":
		switch p.AirbyteType {
		case "integer":
			return kindInteger, nil
		case "big_integer":
			return kindBigInteger, nil
		}
		return kindNumber, nil
	case "string":
		if p.AirbyteType == "big_integer" {
			return kindBigInteger, nil
		}
		switch p.Format {
		case "date":
			return kindDate, nil
		case "date-time":
			if p.AirbyteType == "timestamp_without_timezone" {
				return kindTimestampNTZ, nil
			}
			return kindTimestampTZ, nil
		case "time":
			if p.AirbyteType == "time_with_timezone" {
				return kindTimeTZ, nil
			}
			return kindTime, nil
		}
		return kindString, nil
	default:
		return kindJSON, nil
	}
}

func nonNullTypes(raw json.RawMessage) ([]string, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, nil
	}
	var types []string
	if raw[0] == '"' {
		var t string
		if err := json.Unmarshal(raw, &t); err != nil {
			return nil, fmt.Errorf("type: %w", err)
		}
		types = []string{t}
	} else if err := json.Unmarshal(raw, &types); err != nil {
		return nil, fmt.Errorf("type: %w", err)
	}
	out := types[:0]
	for _, t := range types {
		if t != "null" {
			out = append(out, t)
		}
	}
	return out, nil
}

// newSchema is the Iceberg schema of a new table: every column optional,
// numbered from 1 in order.
func newSchema(cols []column) *iceberg.Schema {
	fields := make([]iceberg.NestedField, len(cols))
	for i, c := range cols {
		fields[i] = iceberg.NestedField{ID: i + 1, Name: c.name, Type: c.typ}
	}
	return iceberg.NewSchema(0, fields...)
}

// evolve compares the catalog's columns with a table's schema and returns
// the columns the table lacks. A column whose type differs from the table's
// fails; columns of the table the catalog lacks stay and are written null.
func evolve(existing *iceberg.Schema, cols []column) ([]column, error) {
	byName := map[string]iceberg.NestedField{}
	for _, f := range existing.Fields() {
		byName[f.Name] = f
	}
	var added []column
	for _, c := range cols {
		f, ok := byName[c.name]
		if !ok {
			added = append(added, c)
			continue
		}
		if !f.Type.Equals(c.typ) {
			return nil, fmt.Errorf("column %q is %s in the table but %s in the catalog; type changes are not supported and the table is unchanged", c.name, f.Type, c.typ)
		}
	}
	return added, nil
}
