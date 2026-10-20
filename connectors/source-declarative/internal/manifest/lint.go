package manifest

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Cursor formats besides a Go time layout.
const (
	FormatUnixS   = "unix_s"
	FormatUnixMS  = "unix_ms"
	FormatRFC3339 = "rfc3339"
	FormatDate    = "date"
)

// ValidTimeFormat reports whether f is a cursor format: a named one or a Go
// layout with a year.
func ValidTimeFormat(f string) bool {
	switch f {
	case FormatUnixS, FormatUnixMS, FormatRFC3339, FormatDate:
		return true
	}
	if !strings.Contains(f, "2006") {
		return false
	}
	ref := time.Date(2026, 10, 18, 12, 34, 56, 789000000, time.UTC)
	_, err := time.Parse(f, ref.Format(f))
	return err == nil
}

// linter collects the semantic problems of a decoded manifest.
type linter struct {
	m    *Manifest
	errs []*Error
}

func (l *linter) add(path, format string, args ...any) {
	l.errs = append(l.errs, errorf(path, format, args...))
}

// roots is the set of template roots allowed in a place.
type roots map[string]bool

func lint(m *Manifest) []*Error {
	l := &linter{m: m}
	l.spec()
	configOnly := roots{RootConfig: true}
	l.template("base_url", &m.BaseURL, configOnly, false)
	if !strings.HasSuffix(m.BaseURL.Raw, "/") {
		l.add("base_url", "must end with /")
	}
	l.auth(configOnly)
	l.templates("headers", m.Headers, configOnly)
	l.params("params", m.Params, configOnly)
	if m.Timeout != nil && *m.Timeout <= 0 {
		l.add("timeout", "must be positive")
	}
	l.path("errors.message", m.Errors.Message)
	l.rules("errors.when", m.Errors.When)
	for i := range m.Retry.RateLimit.Wait {
		if err := m.Retry.RateLimit.Wait[i].parse(); err != nil {
			l.add(fmt.Sprintf("retry.rate_limit.wait[%d]", i), "%v", err)
		}
	}
	names := map[string]int{}
	for i, st := range m.Streams {
		if j, ok := names[st.Name]; ok {
			l.add(fmt.Sprintf("streams[%d].name", i), "stream %q is also streams[%d]", st.Name, j)
			continue
		}
		names[st.Name] = i
	}
	if _, ok := names[m.Check.Stream]; !ok {
		l.add("check.stream", "no stream named %q", m.Check.Stream)
	}
	for i := range m.Streams {
		l.stream(i, names)
	}
	return l.errs
}

func (l *linter) spec() {
	props := l.m.Spec.Properties
	for name, p := range props {
		at := joinKey("spec.properties", name)
		if strings.HasPrefix(name, "__") {
			l.add(at, "names starting with __ are reserved for the engine")
		}
		if p.Items != nil && p.Type != "array" {
			l.add(at+".items", "only an array property has items")
		}
		if p.Type == "array" && p.Items == nil {
			l.add(at+".items", "an array property needs items: {type: string}")
		}
		if p.Format != "" && p.Type != "string" {
			l.add(at+".format", "only a string property has a format")
		}
		if p.Pattern != "" {
			if p.Type != "string" {
				l.add(at+".pattern", "only a string property has a pattern")
			}
			if _, err := regexp.Compile(p.Pattern); err != nil {
				l.add(at+".pattern", "%v", err)
			}
		}
	}
	for i, r := range l.m.Spec.Required {
		if _, ok := props[r]; !ok {
			l.add(fmt.Sprintf("spec.required[%d]", i), "no property named %q", r)
		}
	}
}

func (l *linter) auth(configOnly roots) {
	a := &l.m.Auth
	for key, t := range map[string]*Template{
		"value": &a.Value, "token": &a.Token, "username": &a.Username, "password": &a.Password,
		"token_url": &a.TokenURL, "client_id": &a.ClientID, "client_secret": &a.ClientSecret, "refresh_token": &a.RefreshToken,
	} {
		l.template("auth."+key, t, configOnly, false)
	}
	l.path("auth.access_token_path", a.AccessTokenPath)
	l.path("auth.expires_in_path", a.ExpiresInPath)
}

func (l *linter) rules(at string, rules []BodyRule) {
	for i := range rules {
		rat := fmt.Sprintf("%s[%d].path", at, i)
		p := rules[i].Path
		l.path(rat, p)
		if p.Wildcard() {
			l.add(rat, "a path with [*] selects an array; a rule compares one value")
		}
		if rules[i].Action == "" {
			rules[i].Action = ActionFail
		}
	}
}

func (l *linter) path(at string, p *Path) {
	if p == nil {
		return
	}
	if err := p.parse(); err != nil {
		l.add(at, "%v", err)
	}
}

func (l *linter) templates(at string, ts map[string]Template, allowed roots) {
	for k, t := range ts {
		l.template(joinKey(at, k), &t, allowed, false)
		ts[k] = t
	}
}

func (l *linter) params(at string, ps map[string]Param, allowed roots) {
	for k, p := range ps {
		for i := range p.Values {
			pat := joinKey(at, k)
			if p.List {
				pat += fmt.Sprintf("[%d]", i)
			}
			l.template(pat, &p.Values[i], allowed, false)
		}
		ps[k] = p
	}
}

// template parses t and checks its substitutions against the allowed roots
// and the spec. A config array is allowed without join only when list is
// true (partition.values).
func (l *linter) template(at string, t *Template, allowed roots, list bool) {
	if err := t.parse(); err != nil {
		l.add(at, "%v", err)
		return
	}
	for _, e := range t.Exprs() {
		if !allowed[e.Root] {
			l.add(at, "{{ %s }} is not allowed here (%s)", e, rootHint(e.Root, allowed))
			continue
		}
		if e.Root != RootConfig {
			continue
		}
		if len(e.Keys) != 1 {
			l.add(at, "{{ %s }}: config takes one property name", e)
			continue
		}
		p, ok := l.m.Spec.Properties[e.Keys[0]]
		if !ok {
			l.add(at, "{{ %s }}: spec.properties has no %q", e, e.Keys[0])
			continue
		}
		switch {
		case e.Join != nil && p.Type != "array":
			l.add(at, "{{ %s }}: join needs an array property, %s is %s", e, e.Keys[0], p.Type)
		case e.Join == nil && p.Type == "array" && !list:
			l.add(at, "{{ %s }}: %s is an array; use {{ %s | join(\",\") }}", e, e.Keys[0], e)
		}
	}
}

func rootHint(root string, allowed roots) string {
	if len(allowed) == 1 {
		return "base_url, auth, start and the top-level headers and params take config only"
	}
	switch root {
	case RootPartition:
		return "partition.value needs partition.values"
	case RootParent:
		return "parent needs partition.parent"
	case RootSince, RootUntil:
		return root + " needs incremental"
	}
	return "unknown root"
}

func (l *linter) stream(i int, names map[string]int) {
	st := &l.m.Streams[i]
	at := fmt.Sprintf("streams[%d]", i)
	var parent *Stream
	if p := st.Partition; p != nil {
		switch {
		case p.Parent != "":
			j, ok := names[p.Parent]
			switch {
			case !ok:
				l.add(at+".partition.parent", "no stream named %q", p.Parent)
			case p.Parent == st.Name:
				l.add(at+".partition.parent", "a stream cannot be its own parent")
			case l.m.Streams[j].Partition != nil:
				l.add(at+".partition.parent", "%q is partitioned itself; partitions are one level only", p.Parent)
			default:
				parent = &l.m.Streams[j]
			}
		case p.Values != nil:
			l.values(at+".partition.values", p.Values)
		}
	}

	allowed := roots{RootConfig: true}
	if st.Partition != nil && st.Partition.Values != nil {
		allowed[RootPartition] = true
	}
	if st.Partition != nil && st.Partition.Parent != "" {
		allowed[RootParent] = true
	}
	if st.Incremental != nil {
		allowed[RootSince], allowed[RootUntil] = true, true
	}
	l.rules(at+".errors.when", st.Errors.When)
	l.template(at+".path", &st.Path, allowed, false)
	l.params(at+".params", st.Params, allowed)
	l.templates(at+".headers", st.Headers, allowed)

	l.fields(at, st)
	l.records(at+".records", st, allowed)
	l.pagination(at+".pagination", st)
	l.incremental(at+".incremental", st, parent)
}

func (l *linter) values(at string, v *Values) {
	if v.Template == nil {
		return
	}
	l.template(at, v.Template, roots{RootConfig: true}, true)
	e, ok := v.Template.Single()
	if !ok || e.Join != nil {
		l.add(at, "must be one {{ config.<array property> }} or a list of strings")
		return
	}
	if p, ok := l.m.Spec.Properties[e.Keys[0]]; ok && p.Type != "array" {
		l.add(at, "config.%s is %s, not an array", e.Keys[0], p.Type)
	}
}

func (l *linter) records(at string, st *Stream, allowed roots) {
	r := &st.Records
	l.path(at+".path", r.Path)
	switch r.Mode {
	case ModeEntries:
		if r.KeyField == nil {
			l.add(at+".key_field", "mode entries needs key_field")
		}
	default:
		if r.KeyField != nil {
			l.add(at+".key_field", "only mode entries has key_field")
		}
		if r.ValueField != nil {
			l.add(at+".value_field", "only mode entries has value_field")
		}
	}
	for name, src := range r.Add {
		fat := joinKey(at+".add", name)
		switch {
		case src.Path != nil:
			l.path(fat, src.Path)
		case src.Template != nil:
			l.template(fat, src.Template, allowed, false)
		}
		r.Add[name] = src
	}
}

func (l *linter) pagination(at string, st *Stream) {
	p := st.Pagination
	if p == nil {
		return
	}
	if (p.Size == 0) != (p.SizeParam == "") {
		l.add(at, "size and size_param go together")
	}
	if p.ShortPage && p.Size == 0 {
		l.add(at+".short_page", "needs size")
	}
	l.path(at+".url_from", p.URLFrom)
	l.path(at+".has_more", p.HasMore)
	l.path(at+".is_last", p.IsLast)
	l.path(at+".total", p.Total)
	if p.Type != PageCursor {
		return
	}
	switch {
	case strings.HasPrefix(p.CursorFrom, "$"):
		path, err := ParsePath(p.CursorFrom)
		if err != nil {
			l.add(at+".cursor_from", "%v", err)
		}
		p.Cursor = CursorSource{Kind: CursorBody, Path: path}
	case strings.HasPrefix(p.CursorFrom, "header:"):
		p.Cursor = CursorSource{Kind: CursorHeader, Header: strings.TrimPrefix(p.CursorFrom, "header:")}
	case strings.HasPrefix(p.CursorFrom, "last:"):
		name := strings.TrimPrefix(p.CursorFrom, "last:")
		f := Field{Raw: name, Parts: strings.Split(name, ".")}
		l.field(at+".cursor_from", st, f)
		p.Cursor = CursorSource{Kind: CursorLast, Field: f}
	}
}

func (l *linter) incremental(at string, st *Stream, parent *Stream) {
	inc := st.Incremental
	if inc == nil {
		return
	}
	if !ValidTimeFormat(inc.CursorFormat) {
		l.add(at+".cursor_format", "%q is not unix_s, unix_ms, rfc3339, date or a Go time layout", inc.CursorFormat)
	}
	if inc.SinceFormat != "" && !ValidTimeFormat(inc.SinceFormat) {
		l.add(at+".since_format", "%q is not unix_s, unix_ms, rfc3339, date or a Go time layout", inc.SinceFormat)
	}
	if st.Partition != nil && st.Partition.Parent != "" {
		if parent != nil && parent.Incremental == nil {
			l.add(at, "a child stream is incremental only when its parent %q is", parent.Name)
		}
		for key, set := range map[string]bool{"start": inc.Start != nil, "since_format": inc.SinceFormat != "", "lookback": inc.Lookback != nil, "window": inc.Window != nil} {
			if set {
				l.add(at+"."+key, "a child stream takes cursor_field and cursor_format only; %s is its parent's", key)
			}
		}
		return
	}
	if inc.Start == nil {
		l.add(at+".start", "required key is missing")
	} else {
		l.template(at+".start", inc.Start, roots{RootConfig: true}, false)
	}
	if inc.Lookback != nil && *inc.Lookback <= 0 {
		l.add(at+".lookback", "must be positive")
	}
	if inc.Window != nil && inc.Window.Step <= 0 {
		l.add(at+".window.step", "must be positive")
	}
}

// fields checks every field of st that must be in its schema; a stream
// without a schema (infer mode) skips them.
func (l *linter) fields(at string, st *Stream) {
	if st.Schema == nil {
		return
	}
	for k, f := range st.PrimaryKey {
		l.field(at+fmt.Sprintf(".primary_key[%d]", k), st, f)
	}
	r := &st.Records
	if r.Mode == ModeEntries {
		if r.KeyField != nil {
			l.field(at+".records.key_field", st, *r.KeyField)
		}
		if r.ValueField != nil {
			l.field(at+".records.value_field", st, *r.ValueField)
		}
	}
	for _, name := range sortedNames(r.Add) {
		l.field(joinKey(at+".records.add", name), st, Field{Raw: name, Parts: strings.Split(name, ".")})
	}
	if st.Incremental != nil {
		l.field(at+".incremental.cursor_field", st, st.Incremental.CursorField)
	}
}

func sortedNames[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// field checks that a dotted field is declared in the stream's schema.
func (l *linter) field(at string, st *Stream, f Field) {
	if !json.Valid(st.Schema) || !strings.HasPrefix(strings.TrimSpace(string(st.Schema)), "{") {
		return
	}
	var node map[string]any
	if err := json.Unmarshal(st.Schema, &node); err != nil {
		return
	}
	for _, part := range f.Parts {
		props, _ := node["properties"].(map[string]any)
		next, ok := props[part].(map[string]any)
		if !ok {
			l.add(at, "field %q is not in the stream's schema", f.Raw)
			return
		}
		node = next
	}
}
