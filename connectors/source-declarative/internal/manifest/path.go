package manifest

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Path is a response path: $ followed by .name, ['name'], [n] and [*].
type Path struct {
	Raw   string
	steps []step
}

type step struct {
	key      string
	index    int
	isIndex  bool
	wildcard bool
}

// UnmarshalJSON decodes a path; it is parsed by Load.
func (p *Path) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*p = Path{Raw: s}
	return nil
}

// ParsePath parses a response path.
func ParsePath(s string) (*Path, error) {
	p := &Path{Raw: s}
	return p, p.parse()
}

func (p *Path) parse() error {
	s := p.Raw
	if !strings.HasPrefix(s, "$") {
		return fmt.Errorf("path %q does not start with $", s)
	}
	p.steps = nil
	rest := s[1:]
	for rest != "" {
		switch {
		case strings.HasPrefix(rest, "['"):
			end := strings.Index(rest[2:], "']")
			if end < 0 {
				return fmt.Errorf("path %q: unterminated ['", s)
			}
			p.steps = append(p.steps, step{key: rest[2 : 2+end]})
			rest = rest[2+end+2:]
		case strings.HasPrefix(rest, "[*]"):
			p.steps = append(p.steps, step{wildcard: true})
			rest = rest[3:]
		case strings.HasPrefix(rest, "["):
			end := strings.Index(rest, "]")
			if end < 0 {
				return fmt.Errorf("path %q: unterminated [", s)
			}
			n, err := strconv.Atoi(rest[1:end])
			if err != nil || n < 0 {
				return fmt.Errorf("path %q: %q is not an index, [*] or ['name']", s, rest[:end+1])
			}
			p.steps = append(p.steps, step{index: n, isIndex: true})
			rest = rest[end+1:]
		case strings.HasPrefix(rest, "."):
			rest = rest[1:]
			end := strings.IndexAny(rest, ".[")
			if end < 0 {
				end = len(rest)
			}
			if end == 0 {
				return fmt.Errorf("path %q: empty name", s)
			}
			p.steps = append(p.steps, step{key: rest[:end]})
			rest = rest[end:]
		default:
			return fmt.Errorf("path %q: unexpected %q", s, rest)
		}
	}
	return nil
}

// Root reports whether the path is $ alone.
func (p *Path) Root() bool { return len(p.steps) == 0 }

// Get returns the value at the path in v, a document decoded with
// json.Number. found is false when a key or index is missing or a step meets
// a value of the wrong kind; a [*] step yields an array of the rest of the
// path over every element and is not found when any element lacks it.
func (p *Path) Get(v any) (value any, found bool) {
	return get(v, p.steps)
}

func get(v any, steps []step) (any, bool) {
	for i, st := range steps {
		switch {
		case st.wildcard:
			arr, ok := v.([]any)
			if !ok {
				return nil, false
			}
			out := make([]any, 0, len(arr))
			for _, e := range arr {
				ev, ok := get(e, steps[i+1:])
				if !ok {
					return nil, false
				}
				out = append(out, ev)
			}
			return out, true
		case st.isIndex:
			arr, ok := v.([]any)
			if !ok || st.index >= len(arr) {
				return nil, false
			}
			v = arr[st.index]
		default:
			obj, ok := v.(map[string]any)
			if !ok {
				return nil, false
			}
			if v, ok = obj[st.key]; !ok {
				return nil, false
			}
		}
	}
	return v, true
}

// GetField returns the value of a dotted field in a record.
func GetField(rec map[string]any, f Field) (any, bool) {
	var v any = rec
	for _, k := range f.Parts {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		if v, ok = obj[k]; !ok {
			return nil, false
		}
	}
	return v, true
}

// SetField sets a dotted field in a record, creating the objects on its
// way. It fails when a value on the way is not an object.
func SetField(rec map[string]any, f Field, v any) error {
	obj := rec
	for _, k := range f.Parts[:len(f.Parts)-1] {
		next, ok := obj[k]
		if !ok || next == nil {
			m := map[string]any{}
			obj[k] = m
			obj = m
			continue
		}
		if obj, ok = next.(map[string]any); !ok {
			return fmt.Errorf("field %s: %s is not an object", f.Raw, k)
		}
	}
	obj[f.Last()] = v
	return nil
}
