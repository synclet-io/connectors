package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

var (
	decimalPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)
	integerPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
)

// schemaNode is a JSON schema as the configured catalog carries it.
type schemaNode struct {
	Type       synclet.PropTypes      `json:"type"`
	Properties map[string]*schemaNode `json:"properties"`
	Items      *schemaNode            `json:"items"`
}

func (n *schemaNode) is(t synclet.PropType) bool { return n != nil && slices.Contains(n.Type, t) }

// converter turns the objects of the API into records of a stream's
// configured schema.
type converter struct{ schema *schemaNode }

func newConverter(raw json.RawMessage) (converter, error) {
	var n schemaNode
	if err := json.Unmarshal(raw, &n); err != nil {
		return converter{}, err
	}
	return converter{schema: &n}, nil
}

// record keeps the top-level fields of obj the schema declares and
// converts each to its declared type.
func (c converter) record(obj map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	out := make(map[string]json.RawMessage, len(c.schema.Properties))
	for name, n := range c.schema.Properties {
		raw, ok := obj[name]
		if !ok {
			continue
		}
		v, err := convert(n, raw, name)
		if err != nil {
			return nil, err
		}
		out[name] = v
	}
	return out, nil
}

// convert returns raw as the type n declares. A Decimal or Money string
// under a number becomes a JSON number with the same digits, so amounts
// stay exact; under an integer, an UnsignedInt64 string becomes a JSON
// integer. Objects and arrays convert their declared members.
func convert(n *schemaNode, raw json.RawMessage, path string) (json.RawMessage, error) {
	raw = bytes.TrimSpace(raw)
	if n == nil || len(raw) == 0 || string(raw) == "null" {
		return raw, nil
	}
	switch raw[0] {
	case '"':
		if !n.is(synclet.Number) && !n.is(synclet.Integer) {
			return raw, nil
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		pattern, kind := decimalPattern, "a decimal number"
		if !n.is(synclet.Number) {
			pattern, kind = integerPattern, "an integer"
		}
		if !pattern.MatchString(s) {
			return nil, fmt.Errorf("%s: value %q is not %s", path, s, kind)
		}
		return json.RawMessage(s), nil
	case '{':
		if len(n.Properties) == 0 {
			return raw, nil
		}
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		for k, v := range obj {
			cv, err := convert(n.Properties[k], v, path+"."+k)
			if err != nil {
				return nil, err
			}
			obj[k] = cv
		}
		return json.Marshal(obj)
	case '[':
		if n.Items == nil {
			return raw, nil
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		for i, v := range arr {
			cv, err := convert(n.Items, v, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			arr[i] = cv
		}
		return json.Marshal(arr)
	}
	return raw, nil
}
