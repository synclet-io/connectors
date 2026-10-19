package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// record returns the schema fields of obj. A number sent as a string, as
// LinkedIn sends amounts, becomes a JSON number. A missing or null field is
// left out; a value whose type does not match its schema fails.
func record(fs []field, obj map[string]any) (map[string]any, error) {
	rec := make(map[string]any, len(fs))
	for _, f := range fs {
		v, ok := obj[f.name]
		if !ok || v == nil {
			continue
		}
		cv, err := convert(f.typ, v)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.name, err)
		}
		rec[f.name] = cv
	}
	return rec, nil
}

func convert(typ synclet.PropType, v any) (any, error) {
	switch typ {
	case synclet.String:
		switch t := v.(type) {
		case string:
			return t, nil
		case json.Number:
			return t.String(), nil
		}
	case synclet.Number, synclet.Integer:
		var s string
		switch t := v.(type) {
		case json.Number:
			s = t.String()
		case string:
			s = strings.TrimSpace(t)
		}
		if s != "" {
			var err error
			if typ == synclet.Integer {
				_, err = strconv.ParseInt(s, 10, 64)
			} else {
				_, err = strconv.ParseFloat(s, 64)
			}
			if err == nil {
				return json.Number(s), nil
			}
		}
	case boolean:
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case synclet.Object:
		if m, ok := v.(map[string]any); ok {
			return m, nil
		}
	case synclet.Array:
		if a, ok := v.([]any); ok {
			return a, nil
		}
	}
	return nil, fmt.Errorf("%T value %v is not %s", v, v, typ)
}
