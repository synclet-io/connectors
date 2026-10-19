package manifest

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// placeholderSchema stands in for a missing stream schema while a relaxed
// document passes the format schema; it never reaches the Manifest.
var placeholderSchema = map[string]any{"type": "object", "properties": map[string]any{"_": map[string]any{}}}

// LoadDocumentRelaxed loads an injected manifest for the builder's infer
// mode: a stream may have no schema, its Schema stays nil and the rules that
// need a schema skip it. Every other check of LoadDocument applies.
func LoadDocumentRelaxed(doc map[string]any) (*Manifest, error) {
	jv, err := jsonValue(doc, "")
	if err != nil {
		return nil, err
	}
	top := jv.(map[string]any)
	var missing []int
	if streams, ok := top["streams"].([]any); ok {
		for i, s := range streams {
			if st, ok := s.(map[string]any); ok {
				if _, has := st["schema"]; !has {
					st["schema"] = placeholderSchema
					missing = append(missing, i)
				}
			}
		}
	}
	return loadDocument(top, nil, missing)
}

// SchemaFieldErrors checks the fields of stream i that must be in its schema
// (primary key, entries key and value fields, add keys, cursor field)
// against schema instead of the stream's own, sorted by path.
func SchemaFieldErrors(m *Manifest, i int, schema json.RawMessage) []*Error {
	st := m.Streams[i]
	st.Schema = schema
	l := &linter{m: m}
	l.fields(fmt.Sprintf("streams[%d]", i), &st)
	slices.SortStableFunc(l.errs, func(a, b *Error) int { return strings.Compare(a.Path, b.Path) })
	return l.errs
}
