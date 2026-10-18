package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// Metadata fields every record carries.
const (
	fieldFile     = "_synclet_source_file"
	fieldRow      = "_synclet_source_row"
	fieldModified = "_synclet_source_modified"
)

var metadataFields = []string{fieldFile, fieldRow, fieldModified}

// JSON schema types of a field, in the order a property lists them.
const (
	typeBoolean = "boolean"
	typeInteger = "integer"
	typeNumber  = "number"
	typeString  = "string"
	typeObject  = "object"
	typeArray   = "array"
	typeNull    = "null"
)

var typeOrder = []string{typeNull, typeBoolean, typeInteger, typeNumber, typeString, typeObject, typeArray}

var (
	intPattern    = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
	numberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)
)

// csvText is a CSV or TSV cell, which has no type until a field's schema
// gives it one.
type csvText string

// fieldSpec is what conversion needs of a property: the JSON types it
// allows. No types allows any value.
type fieldSpec struct {
	types []string
}

func (f fieldSpec) allows(t string) bool {
	return len(f.types) == 0 || slices.Contains(f.types, t)
}

// recordSchema is a stream's JSON schema as read: the properties with their
// raw JSON, and the types conversion needs.
type recordSchema struct {
	raw    map[string]json.RawMessage
	fields map[string]fieldSpec
}

// parseSchema reads an object JSON schema with properties. A property's
// "type" may be a string or an array of strings.
func parseSchema(b json.RawMessage) (*recordSchema, error) {
	var s struct {
		Type       any                        `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	if len(s.Properties) == 0 {
		return nil, errors.New("no properties")
	}
	rs := &recordSchema{raw: s.Properties, fields: map[string]fieldSpec{}}
	for name, p := range s.Properties {
		var prop struct {
			Type json.RawMessage `json:"type"`
		}
		if err := json.Unmarshal(p, &prop); err != nil {
			return nil, fmt.Errorf("property %q: %w", name, err)
		}
		var types []string
		if len(prop.Type) > 0 {
			var one string
			if err := json.Unmarshal(prop.Type, &one); err == nil {
				types = []string{one}
			} else if err := json.Unmarshal(prop.Type, &types); err != nil {
				return nil, fmt.Errorf("property %q: type must be a string or an array of strings", name)
			}
		}
		for _, t := range types {
			if !slices.Contains(typeOrder, t) {
				return nil, fmt.Errorf("property %q: unknown type %q", name, t)
			}
		}
		rs.fields[name] = fieldSpec{types: types}
	}
	return rs, nil
}

// withMetadata returns the stream's JSON schema: the given properties plus
// the metadata fields.
func withMetadata(props map[string]json.RawMessage) json.RawMessage {
	all := maps.Clone(props)
	all[fieldFile] = json.RawMessage(`{"type":"string"}`)
	all[fieldRow] = json.RawMessage(`{"type":"integer"}`)
	all[fieldModified] = json.RawMessage(`{"type":"string","format":"date-time","airbyte_type":"timestamp_with_timezone"}`)
	b, err := json.Marshal(struct {
		Type       string                     `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
	}{Type: "object", Properties: all})
	if err != nil {
		panic(err)
	}
	return b
}

// convertRow fits the values one file row gives to the stream's schema: a
// CSV cell takes the first type of its property it parses as, a typed value
// must be of a type its property allows, and a field the schema lacks is an
// error. Fields of the schema the row lacks are left out.
func convertRow(rs *recordSchema, in map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(in)+len(metadataFields))
	for _, name := range slices.Sorted(maps.Keys(in)) {
		if slices.Contains(metadataFields, name) {
			return nil, fmt.Errorf("field %q collides with a metadata field", name)
		}
		f, ok := rs.fields[name]
		if !ok {
			return nil, &unknownColumnError{name: name}
		}
		v, err := convertValue(f, in[name])
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", name, err)
		}
		out[name] = v
	}
	return out, nil
}

// unknownColumnError is a row field the stream schema does not have.
type unknownColumnError struct{ name string }

func (e *unknownColumnError) Error() string {
	return fmt.Sprintf("column %q is not in the stream schema", e.name)
}

func convertValue(f fieldSpec, v any) (any, error) {
	switch v := v.(type) {
	case nil:
		if f.allows(typeNull) {
			return nil, nil
		}
	case csvText:
		s := string(v)
		if b, ok := parseBool(s); ok && f.allows(typeBoolean) {
			return b, nil
		}
		if intPattern.MatchString(s) && f.allows(typeInteger) {
			return json.Number(s), nil
		}
		if numberPattern.MatchString(s) && f.allows(typeNumber) {
			return json.Number(s), nil
		}
		if f.allows(typeString) {
			return s, nil
		}
		return nil, fmt.Errorf("value %q is not of type %s", s, strings.Join(f.types, ", "))
	case bool:
		if f.allows(typeBoolean) {
			return v, nil
		}
	case json.Number:
		if f.allows(typeNumber) || (isIntLiteral(string(v)) && f.allows(typeInteger)) {
			return v, nil
		}
	case string:
		if f.allows(typeString) {
			return v, nil
		}
	case map[string]any:
		if f.allows(typeObject) {
			return v, nil
		}
	case []any:
		if f.allows(typeArray) {
			return v, nil
		}
	default:
		return nil, fmt.Errorf("unsupported value %T", v)
	}
	return nil, fmt.Errorf("%s value is not of type %s", jsonType(v), strings.Join(f.types, ", "))
}

func parseBool(s string) (bool, bool) {
	switch s {
	case "true", "True", "TRUE":
		return true, true
	case "false", "False", "FALSE":
		return false, true
	}
	return false, false
}

func isIntLiteral(s string) bool { return intPattern.MatchString(s) }

func jsonType(v any) string {
	switch v := v.(type) {
	case nil:
		return typeNull
	case bool:
		return typeBoolean
	case json.Number:
		if isIntLiteral(string(v)) {
			return typeInteger
		}
		return typeNumber
	case string:
		return typeString
	case map[string]any:
		return typeObject
	case []any:
		return typeArray
	}
	return fmt.Sprintf("%T", v)
}

// inferredField is what the sampled files show of one field.
type inferredField struct {
	types       map[string]bool
	format      string
	airbyteType string
	declared    bool // a file's schema gave format and airbyte_type
	conflict    bool // files disagree on format or airbyte_type
}

// inference collects the fields of sampled files. CSV and TSV columns widen
// along boolean or integer, then number, then string; JSONL and Parquet
// fields take the union of the types seen.
type inference struct {
	widen  bool
	fields map[string]*inferredField
}

func newInference(filetype string) *inference {
	return &inference{
		widen:  filetype == formatCSV || filetype == formatTSV,
		fields: map[string]*inferredField{},
	}
}

func (in *inference) field(name string) *inferredField {
	f, ok := in.fields[name]
	if !ok {
		f = &inferredField{types: map[string]bool{}}
		in.fields[name] = f
	}
	return f
}

// observe records the type of one value. A CSV cell counts by what it parses
// as; a quoted empty cell is a string.
func (in *inference) observe(name string, v any) {
	f := in.field(name)
	switch v := v.(type) {
	case nil:
		return
	case csvText:
		in.add(f, csvType(string(v)))
	default:
		in.add(f, jsonType(v))
	}
}

// declare records a field's type from a file's own schema.
func (in *inference) declare(name, typ, format, airbyteType string) {
	f := in.field(name)
	if typ != "" {
		in.add(f, typ)
	}
	in.setFormat(f, format, airbyteType)
}

func (in *inference) setFormat(f *inferredField, format, airbyteType string) {
	if !f.declared {
		f.declared = true
		f.format, f.airbyteType = format, airbyteType
		return
	}
	if f.format != format || f.airbyteType != airbyteType {
		f.conflict = true
	}
}

func csvType(s string) string {
	switch {
	case s == "":
		return typeString
	case func() bool { _, ok := parseBool(s); return ok }():
		return typeBoolean
	case intPattern.MatchString(s):
		return typeInteger
	case numberPattern.MatchString(s):
		return typeNumber
	}
	return typeString
}

func (in *inference) add(f *inferredField, t string) {
	if !in.widen {
		f.types[t] = true
		return
	}
	var cur string
	for k := range f.types {
		cur = k
	}
	f.types = map[string]bool{widen(cur, t): true}
}

// widen joins two CSV column types: integer and number give number, any
// other pair of different types gives string.
func widen(a, b string) string {
	switch {
	case a == "" || a == b:
		return b
	case (a == typeInteger && b == typeNumber) || (a == typeNumber && b == typeInteger):
		return typeNumber
	}
	return typeString
}

// merge adds another file's inference.
func (in *inference) merge(o *inference) {
	for name, of := range o.fields {
		f := in.field(name)
		for t := range of.types {
			in.add(f, t)
		}
		if of.conflict {
			f.conflict = true
		} else if of.declared {
			in.setFormat(f, of.format, of.airbyteType)
		}
	}
}

// properties renders the inferred fields as nullable JSON schema properties.
// A field no file gave a value is a string.
func (in *inference) properties() map[string]json.RawMessage {
	props := make(map[string]json.RawMessage, len(in.fields))
	for name, f := range in.fields {
		types := maps.Clone(f.types)
		if types[typeNumber] {
			delete(types, typeInteger)
		}
		if len(types) == 0 {
			types[typeString] = true
		}
		list := []string{typeNull}
		for _, t := range typeOrder {
			if types[t] {
				list = append(list, t)
			}
		}
		p := map[string]any{"type": list}
		if !f.conflict && f.format != "" && len(types) == 1 && types[typeString] {
			p["format"] = f.format
		}
		if !f.conflict && f.airbyteType != "" && len(types) == 1 {
			p["airbyte_type"] = f.airbyteType
		}
		b, err := json.Marshal(p)
		if err != nil {
			panic(err)
		}
		props[name] = b
	}
	return props
}
