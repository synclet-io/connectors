package main

import (
	"encoding/json"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// shape is what the sampled values at one place of a document had in
// common: the JSON types they are written as, the shapes of the fields of
// the objects and of the items of the arrays among them, and whether every
// value written as a string was an RFC 3339 date-time.
type shape struct {
	kinds   kind
	strings int
	dates   int
	props   map[string]*shape
	items   *shape
}

// addDocument merges a sampled document into s, the shape of a collection.
func (s *shape) addDocument(doc bson.Raw) error {
	elems, err := doc.Elements()
	if err != nil {
		return err
	}
	s.kinds |= kindObject
	for _, e := range elems {
		if err := s.prop(e.Key()).add(e.Value()); err != nil {
			return err
		}
	}
	return nil
}

func (s *shape) prop(name string) *shape {
	if s.props == nil {
		s.props = map[string]*shape{}
	}
	p := s.props[name]
	if p == nil {
		p = &shape{}
		s.props[name] = p
	}
	return p
}

func (s *shape) add(v bson.RawValue) error {
	k := kindOf(v)
	s.kinds |= k
	switch k {
	case kindString:
		str, err := stringOf(v)
		if err != nil {
			return err
		}
		s.strings++
		if isDateTime(str) {
			s.dates++
		}
	case kindObject:
		return s.addDocument(v.Document())
	case kindArray:
		values, err := v.Array().Values()
		if err != nil {
			return err
		}
		if s.items == nil {
			s.items = &shape{}
		}
		for _, item := range values {
			if err := s.items.add(item); err != nil {
				return err
			}
		}
	}
	return nil
}

func isDateTime(s string) bool {
	_, err := time.Parse(time.RFC3339Nano, s)
	return err == nil
}

// streamSchema returns the JSON schema of a collection of shape s: an
// object whose properties are the sampled fields, every one nullable, and
// which allows fields no sampled document had. _id is always a property.
func (s *shape) streamSchema() json.RawMessage {
	props := map[string]any{}
	for name, p := range s.props {
		props[name] = p.schema()
	}
	if _, ok := props["_id"]; !ok {
		props["_id"] = map[string]any{"type": []string{"null", "string"}}
	}
	b, _ := json.Marshal(map[string]any{"type": "object", "properties": props})
	return b
}

// schema returns the JSON schema of a nullable field of shape s. A field
// with integer and floating point values is a number; other mixes are a
// union of types. A field that was always null is a nullable string.
func (s *shape) schema() map[string]any {
	k := s.kinds | kindNull
	if k&kindNumber != 0 {
		k &^= kindInteger
	}
	if k == kindNull {
		k |= kindString
	}
	var types []string
	for _, t := range []struct {
		k    kind
		name string
	}{
		{kindNull, "null"}, {kindBoolean, "boolean"}, {kindInteger, "integer"}, {kindNumber, "number"},
		{kindString, "string"}, {kindObject, "object"}, {kindArray, "array"},
	} {
		if k&t.k != 0 {
			types = append(types, t.name)
		}
	}
	out := map[string]any{"type": types}
	if s.strings > 0 && s.dates == s.strings {
		out["format"] = "date-time"
		if k == kindNull|kindString {
			out["airbyte_type"] = "timestamp_with_timezone"
		}
	}
	if len(s.props) > 0 {
		props := make(map[string]any, len(s.props))
		for name, p := range s.props {
			props[name] = p.schema()
		}
		out["properties"] = props
	}
	if s.items != nil && s.items.kinds != 0 {
		out["items"] = s.items.schema()
	}
	return out
}
