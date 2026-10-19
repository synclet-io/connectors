package manifest

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Template roots.
const (
	RootConfig    = "config"
	RootPartition = "partition"
	RootParent    = "parent"
	RootSince     = "since"
	RootUntil     = "until"
)

// Template is literal text with {{ root.key }} substitutions.
type Template struct {
	Raw   string
	parts []part
}

type part struct {
	lit  string
	expr *Expr
}

// Expr is one substitution.
type Expr struct {
	Root string
	Keys []string
	// Join is the separator of a join filter, nil without one.
	Join *string
}

func (e Expr) String() string {
	s := strings.Join(append([]string{e.Root}, e.Keys...), ".")
	if e.Join != nil {
		s += fmt.Sprintf(" | join(%q)", *e.Join)
	}
	return s
}

// UnmarshalJSON decodes a template; it is parsed by Load.
func (t *Template) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	*t = Template{Raw: s}
	return nil
}

// ParseTemplate parses a template.
func ParseTemplate(s string) (*Template, error) {
	t := &Template{Raw: s}
	return t, t.parse()
}

func (t *Template) parse() error {
	t.parts = nil
	rest := t.Raw
	for rest != "" {
		open := strings.Index(rest, "{{")
		if open < 0 {
			t.parts = append(t.parts, part{lit: rest})
			break
		}
		if open > 0 {
			t.parts = append(t.parts, part{lit: rest[:open]})
		}
		end := strings.Index(rest[open:], "}}")
		if end < 0 {
			return fmt.Errorf("template %q: {{ without }}", t.Raw)
		}
		e, err := parseExpr(rest[open+2 : open+end])
		if err != nil {
			return fmt.Errorf("template %q: %w", t.Raw, err)
		}
		t.parts = append(t.parts, part{expr: e})
		rest = rest[open+end+2:]
	}
	return nil
}

func parseExpr(s string) (*Expr, error) {
	ref, filter, hasFilter := strings.Cut(s, "|")
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, errors.New("empty substitution")
	}
	keys := strings.Split(ref, ".")
	for _, k := range keys {
		if k == "" || strings.ContainsAny(k, " \t{}()|\"'") {
			return nil, fmt.Errorf("%q is not a name such as config.api_key", ref)
		}
	}
	e := &Expr{Root: keys[0], Keys: keys[1:]}
	switch e.Root {
	case RootConfig, RootParent:
		if len(e.Keys) == 0 {
			return nil, fmt.Errorf("%s needs a name, such as %s.id", e.Root, e.Root)
		}
	case RootPartition:
		if len(e.Keys) != 1 || e.Keys[0] != "value" {
			return nil, errors.New("partition has only partition.value")
		}
	case RootSince, RootUntil:
		if len(e.Keys) != 0 {
			return nil, fmt.Errorf("%s takes no name", e.Root)
		}
	default:
		return nil, fmt.Errorf("unknown root %q: use config, partition, parent, since or until", e.Root)
	}
	if hasFilter {
		f := strings.TrimSpace(filter)
		arg, ok := strings.CutPrefix(f, "join(")
		if !ok || !strings.HasSuffix(arg, ")") {
			return nil, fmt.Errorf("unknown filter %q: join(\",\") is the only filter", f)
		}
		sep, err := unquote(strings.TrimSpace(strings.TrimSuffix(arg, ")")))
		if err != nil {
			return nil, fmt.Errorf("join: %w", err)
		}
		if e.Root != RootConfig {
			return nil, errors.New("join applies to a config array only")
		}
		e.Join = &sep
	}
	return e, nil
}

func unquote(s string) (string, error) {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1], nil
	}
	v, err := strconv.Unquote(s)
	if err != nil {
		return "", fmt.Errorf("separator %s is not a quoted string", s)
	}
	return v, nil
}

// Exprs returns the template's substitutions.
func (t *Template) Exprs() []Expr {
	var out []Expr
	for _, p := range t.parts {
		if p.expr != nil {
			out = append(out, *p.expr)
		}
	}
	return out
}

// Single returns the template's only substitution when the template is
// exactly one substitution and nothing else.
func (t *Template) Single() (Expr, bool) {
	if len(t.parts) != 1 || t.parts[0].expr == nil {
		return Expr{}, false
	}
	return *t.parts[0].expr, true
}

// Literal reports whether the template has no substitution.
func (t *Template) Literal() bool { return len(t.Exprs()) == 0 }

// Scope is what a template renders from. A nil Partition, Parent, Since or
// Until is not available.
type Scope struct {
	Config    map[string]any
	Partition *string
	Parent    map[string]any
	Since     *string
	Until     *string
}

// Render renders the template; escape is applied to every substituted value
// and to nothing else.
func (t *Template) Render(s Scope, escape func(string) string) (string, error) {
	var b strings.Builder
	for _, p := range t.parts {
		if p.expr == nil {
			b.WriteString(p.lit)
			continue
		}
		v, err := s.value(*p.expr)
		if err != nil {
			return "", err
		}
		if escape != nil {
			v = escape(v)
		}
		b.WriteString(v)
	}
	return b.String(), nil
}

func (s Scope) value(e Expr) (string, error) {
	switch e.Root {
	case RootConfig:
		v, ok := lookup(s.Config, e.Keys)
		if !ok || v == nil {
			return "", nil
		}
		if e.Join != nil {
			arr, ok := v.([]any)
			if !ok {
				return "", fmt.Errorf("{{ %s }}: config.%s is not an array", e, strings.Join(e.Keys, "."))
			}
			items := make([]string, len(arr))
			for i, x := range arr {
				str, err := scalar(x)
				if err != nil {
					return "", fmt.Errorf("{{ %s }}: element %d: %w", e, i, err)
				}
				items[i] = str
			}
			return strings.Join(items, *e.Join), nil
		}
		str, err := scalar(v)
		if err != nil {
			return "", fmt.Errorf("{{ %s }}: %w", e, err)
		}
		return str, nil
	case RootPartition:
		if s.Partition == nil {
			return "", fmt.Errorf("{{ %s }}: no partition here", e)
		}
		return *s.Partition, nil
	case RootParent:
		if s.Parent == nil {
			return "", fmt.Errorf("{{ %s }}: no parent record here", e)
		}
		v, ok := lookup(s.Parent, e.Keys)
		if !ok || v == nil {
			return "", fmt.Errorf("{{ %s }}: the parent record has no %s", e, strings.Join(e.Keys, "."))
		}
		str, err := scalar(v)
		if err != nil {
			return "", fmt.Errorf("{{ %s }}: %w", e, err)
		}
		return str, nil
	case RootSince:
		if s.Since == nil {
			return "", fmt.Errorf("{{ since }}: no incremental bound here")
		}
		return *s.Since, nil
	case RootUntil:
		if s.Until == nil {
			return "", fmt.Errorf("{{ until }}: no incremental bound here")
		}
		return *s.Until, nil
	}
	return "", fmt.Errorf("unknown root %q", e.Root)
}

func lookup(m map[string]any, keys []string) (any, bool) {
	var v any = m
	for _, k := range keys {
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

// scalar renders a JSON scalar as text.
func scalar(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case json.Number:
		return x.String(), nil
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), nil
	case bool:
		return strconv.FormatBool(x), nil
	case []any:
		return "", errors.New("value is an array; join(\",\") turns a config array into one value")
	}
	return "", fmt.Errorf("value %v is not a string, number or boolean", v)
}

// ConfigList returns the strings of a config array property, nil when it
// is missing or null. A value listed twice is an error.
func ConfigList(config map[string]any, name string) ([]string, error) {
	v, ok := config[name]
	if !ok || v == nil {
		return nil, nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("config.%s is not an array", name)
	}
	out := make([]string, len(arr))
	seen := make(map[string]bool, len(arr))
	for i, x := range arr {
		s, err := scalar(x)
		if err != nil {
			return nil, fmt.Errorf("config.%s[%d]: %w", name, i, err)
		}
		if seen[s] {
			return nil, fmt.Errorf("config.%s lists %q twice", name, s)
		}
		seen[s] = true
		out[i] = s
	}
	return out, nil
}
