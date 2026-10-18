package main

import (
	"bytes"
	"encoding/json"
	"fmt"
)

const (
	colRawID       = "_airbyte_raw_id"
	colExtractedAt = "_airbyte_extracted_at"
	colData        = "_airbyte_data"
	colAdditional  = "_airbyte_additional_properties"
)

// columnKind is how a declared property is typed in the output.
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
	kindTimeNTZ
)

// column is one declared top-level property of a stream's JSON schema.
type column struct {
	name string
	kind columnKind
}

// streamColumns returns the stream's top-level properties in schema order.
func streamColumns(schema json.RawMessage) ([]column, error) {
	if len(bytes.TrimSpace(schema)) == 0 {
		return nil, nil
	}
	var top struct {
		Properties json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(schema, &top); err != nil {
		return nil, err
	}
	names, err := objectKeys(top.Properties)
	if err != nil {
		return nil, fmt.Errorf("properties: %w", err)
	}
	if len(names) == 0 {
		return nil, nil
	}
	var props map[string]json.RawMessage
	if err := json.Unmarshal(top.Properties, &props); err != nil {
		return nil, fmt.Errorf("properties: %w", err)
	}
	cols := make([]column, 0, len(names))
	for _, n := range names {
		switch n {
		case colRawID, colExtractedAt, colData, colAdditional:
			return nil, fmt.Errorf("property %q collides with a metadata column", n)
		}
		kind, err := propertyKind(props[n])
		if err != nil {
			return nil, fmt.Errorf("property %q: %w", n, err)
		}
		cols = append(cols, column{name: n, kind: kind})
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
// several non-null types, anyOf, oneOf or no type is JSON.
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
		switch p.Format {
		case "date":
			return kindDate, nil
		case "date-time":
			if p.AirbyteType == "timestamp_without_timezone" {
				return kindTimestampNTZ, nil
			}
			return kindTimestampTZ, nil
		case "time":
			if p.AirbyteType == "time_without_timezone" {
				return kindTimeNTZ, nil
			}
			return kindTimeTZ, nil
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
			return nil, err
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
