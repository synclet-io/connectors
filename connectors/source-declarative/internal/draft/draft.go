// Package draft drafts a stream's JSON Schema from sampled records, for a
// human to edit: the same records always give the same schema.
package draft

import (
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"time"
)

// node accumulates every value seen at one place of the records.
type node struct {
	null, boolean, integer, number, str bool

	strings, dates, dateTimes int

	objects int
	props   map[string]*node
	present map[string]int

	arrays bool
	items  *node
}

// Schema returns the draft schema of records: an object schema with a
// property for every field seen; a field's type is the union of the JSON
// types seen, with null when it was null or missing in some record; numbers
// are integer when every one seen is integral; strings get format date or
// date-time when every one seen parses as one; array items are merged the
// same way; every object allows additional properties and requires none.
func Schema(records []map[string]any) json.RawMessage {
	root := &node{}
	for _, r := range records {
		root.addObject(r)
	}
	s := root.objectSchema()
	s["type"] = "object"
	b, err := json.Marshal(s)
	if err != nil {
		panic(fmt.Sprintf("draft: marshal schema: %v", err))
	}
	return b
}

func (n *node) add(v any) {
	switch x := v.(type) {
	case nil:
		n.null = true
	case bool:
		n.boolean = true
	case json.Number:
		n.addNumber(x.String())
	case float64:
		n.addNumber(fmt.Sprint(x))
	case int, int64, int32, uint, uint64, uint32:
		n.integer = true
	case string:
		n.addString(x)
	case map[string]any:
		n.addObject(x)
	case []any:
		n.arrays = true
		if n.items == nil {
			n.items = &node{}
		}
		for _, e := range x {
			n.items.add(e)
		}
	default:
		n.addString(fmt.Sprint(x))
	}
}

func (n *node) addNumber(s string) {
	r, ok := new(big.Rat).SetString(s)
	if ok && r.IsInt() {
		n.integer = true
		return
	}
	n.number = true
}

func (n *node) addString(s string) {
	n.str = true
	n.strings++
	if _, err := time.Parse(time.DateOnly, s); err == nil {
		n.dates++
	}
	if _, err := time.Parse(time.RFC3339Nano, s); err == nil {
		n.dateTimes++
	}
}

func (n *node) addObject(m map[string]any) {
	n.objects++
	if n.props == nil {
		n.props, n.present = map[string]*node{}, map[string]int{}
	}
	for k, v := range m {
		p := n.props[k]
		if p == nil {
			p = &node{}
			n.props[k] = p
		}
		p.add(v)
		n.present[k]++
	}
}

func (n *node) objectSchema() map[string]any {
	props := map[string]any{}
	for k, p := range n.props {
		if n.present[k] < n.objects {
			p.null = true
		}
		props[k] = p.schema()
	}
	return map[string]any{"properties": props, "additionalProperties": true}
}

func (n *node) schema() map[string]any {
	s := map[string]any{}
	var types []string
	if n.null {
		types = append(types, "null")
	}
	if n.arrays {
		types = append(types, "array")
		items := map[string]any{}
		if n.items != nil && !n.items.empty() {
			items = n.items.schema()
		}
		s["items"] = items
	}
	if n.boolean {
		types = append(types, "boolean")
	}
	switch {
	case n.number:
		types = append(types, "number")
	case n.integer:
		types = append(types, "integer")
	}
	if n.objects > 0 {
		types = append(types, "object")
		for k, v := range n.objectSchema() {
			s[k] = v
		}
	}
	if n.str {
		types = append(types, "string")
		switch {
		case n.dates == n.strings:
			s["format"] = "date"
		case n.dateTimes == n.strings:
			s["format"] = "date-time"
		}
	}
	if n.null {
		slices.Sort(types[1:])
	} else {
		slices.Sort(types)
	}
	if len(types) == 1 {
		s["type"] = types[0]
	} else if len(types) > 1 {
		s["type"] = types
	}
	return s
}

func (n *node) empty() bool {
	return !n.null && !n.boolean && !n.integer && !n.number && !n.str && n.objects == 0 && !n.arrays
}
