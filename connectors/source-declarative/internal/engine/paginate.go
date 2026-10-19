package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"

	"source-declarative/internal/manifest"
)

const defaultMaxPages = 100000

// errSkipped reports that on_404: skip ended a stream or partition.
var errSkipped = errors.New("skipped on 404")

// params renders the top-level and stream query parameters, the stream's
// winning; a value that renders empty is left out.
func (r *run) params(st *manifest.Stream, scope manifest.Scope) (url.Values, error) {
	merged := map[string]manifest.Param{}
	for k, p := range r.m.Params {
		merged[k] = p
	}
	for k, p := range st.Params {
		merged[k] = p
	}
	q := url.Values{}
	for _, k := range sortedKeys(merged) {
		for _, t := range merged[k].Values {
			v, err := t.Render(scope, nil)
			if err != nil {
				return nil, fmt.Errorf("params.%s: %w", k, err)
			}
			if v != "" {
				q.Add(k, v)
			}
		}
	}
	return q, nil
}

func (r *run) streamURL(st *manifest.Stream, scope manifest.Scope) (*url.URL, error) {
	p, err := st.Path.RenderPath(scope)
	if err != nil {
		return nil, fmt.Errorf("stream %s: path: %w", st.Name, err)
	}
	if p == "" {
		return nil, fmt.Errorf("stream %s: path renders empty", st.Name)
	}
	u, err := url.Parse(r.base.String() + p)
	if err != nil {
		return nil, fmt.Errorf("stream %s: path %q: %w", st.Name, p, err)
	}
	return u, nil
}

// page is one response of a stream with its records.
type page struct {
	resp    *response
	records []map[string]any
}

// pages requests a stream's pages for one scope (partition, parent,
// window) and calls fn with each page's records. With check it sends only
// the first request, with page size 1 where the paginator has a size.
func (r *run) pages(ctx context.Context, st *manifest.Stream, scope manifest.Scope, check bool, fn func(page) error) error {
	if r.baseErr != nil {
		return r.baseErr
	}
	p := st.Pagination
	static, err := r.params(st, scope)
	if err != nil {
		return synclet.ConfigError(err)
	}
	first, err := r.streamURL(st, scope)
	if err != nil {
		return synclet.ConfigError(err)
	}
	maxPages := defaultMaxPages
	if p != nil && p.MaxPages > 0 {
		maxPages = p.MaxPages
	}
	used := map[string]bool{}
	var next *url.URL
	var cursorValue string
	var prevBody []byte
	var prevKeys map[string]bool
	received := 0
	for n := 1; ; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		u := next
		if u == nil {
			u = withQuery(first, static, r.pageParams(p, n, received, cursorValue, check))
		}
		if r.o.OnRequest != nil {
			r.o.OnRequest(st.Name, n)
		}
		resp, err := r.get(ctx, st, u, scope)
		if err != nil {
			var he *httpError
			if errors.As(err, &he) && he.status == 404 && st.On404 == manifest.On404Skip {
				r.logger(ctx).Warn(fmt.Sprintf("stream %s: %s answered 404, skipping it (on_404: skip): %s", st.Name, he.desc, he.detail))
				return errSkipped
			}
			return err
		}
		if check {
			return nil
		}
		recs, err := extract(st, resp, scope)
		if err != nil {
			return err
		}
		keys := primaryKeys(st, recs)
		if n > 1 {
			if bytes.Equal(resp.body, prevBody) {
				return fmt.Errorf("stream %s: page %d repeated page %d (%s)", st.Name, n, n-1, resp.desc)
			}
			if len(keys) > 0 && prevKeys != nil && allSeen(keys, prevKeys) {
				return fmt.Errorf("stream %s: page %d repeats the records of page %d (%s)", st.Name, n, n-1, resp.desc)
			}
		}
		if err := fn(page{resp: resp, records: recs}); err != nil {
			return err
		}
		received += len(recs)
		if p == nil {
			return nil
		}
		stop, err := stopped(p, resp, received)
		if err != nil {
			return fmt.Errorf("stream %s: %w (%s)", st.Name, err, resp.desc)
		}
		if stop || len(recs) == 0 || (p.ShortPage && len(recs) < p.Size) {
			return nil
		}
		switch p.Type {
		case manifest.PageCursor, manifest.PageNext, manifest.PageLink:
			v, err := nextValue(p, resp, recs)
			if err != nil {
				return fmt.Errorf("stream %s: %w (%s)", st.Name, err, resp.desc)
			}
			if v == "" {
				if p.HasMore != nil || p.IsLast != nil {
					return fmt.Errorf("stream %s: the page says more exist but has no next value (%s)", st.Name, resp.desc)
				}
				return nil
			}
			if p.Type != manifest.PageCursor {
				nu, err := r.nextURL(v, static)
				if err != nil {
					return fmt.Errorf("stream %s: %w (%s)", st.Name, err, resp.desc)
				}
				v = r.describe(http.MethodGet, nu)
				next = nu
			}
			if used[v] {
				return fmt.Errorf("stream %s: pagination repeated cursor %q (%s)", st.Name, v, resp.desc)
			}
			used[v] = true
			cursorValue = v
		}
		if n >= maxPages {
			return fmt.Errorf("stream %s: max_pages %d reached (%s)", st.Name, maxPages, resp.desc)
		}
		prevBody, prevKeys = resp.body, keys
	}
}

// pageParams are the paginator's query parameters of request n.
func (r *run) pageParams(p *manifest.Pagination, n, received int, cursorValue string, check bool) url.Values {
	q := url.Values{}
	if p == nil {
		return q
	}
	if p.SizeParam != "" {
		switch p.Type {
		case manifest.PageNumber, manifest.PageOffset, manifest.PageCursor:
			q.Set(p.SizeParam, strconv.Itoa(sizeOf(p, check)))
		default:
			if n == 1 {
				q.Set(p.SizeParam, strconv.Itoa(sizeOf(p, check)))
			}
		}
	}
	switch p.Type {
	case manifest.PageNumber:
		first := 1
		if p.First != nil {
			first = *p.First
		}
		name := p.PageParam
		if name == "" {
			name = "page"
		}
		q.Set(name, strconv.Itoa(first+n-1))
	case manifest.PageOffset:
		name := p.OffsetParam
		if name == "" {
			name = "offset"
		}
		q.Set(name, strconv.Itoa(received))
	case manifest.PageCursor:
		if n > 1 {
			q.Set(p.CursorParam, cursorValue)
		}
	}
	return q
}

func sizeOf(p *manifest.Pagination, check bool) int {
	if check {
		return 1
	}
	return p.Size
}

func withQuery(u *url.URL, qs ...url.Values) *url.URL {
	cp := *u
	q := cp.Query()
	for _, extra := range qs {
		for k, vs := range extra {
			q[k] = vs
		}
	}
	cp.RawQuery = q.Encode()
	return &cp
}

// nextURL resolves a next page URL against the base URL and adds the
// static parameters it lacks. The URL must keep the base URL's scheme and
// host, so the credential never goes to another server.
func (r *run) nextURL(v string, static url.Values) (*url.URL, error) {
	ref, err := url.Parse(v)
	if err != nil {
		return nil, errors.New("next page URL does not parse")
	}
	u := r.base.ResolveReference(ref)
	if u.Scheme != r.base.Scheme || u.Host != r.base.Host {
		return nil, fmt.Errorf("next page URL %s is not on the base URL's scheme and host", u.Scheme+"://"+u.Host)
	}
	q := u.Query()
	for k, vs := range static {
		if !q.Has(k) {
			q[k] = vs
		}
	}
	u.RawQuery = q.Encode()
	return u, nil
}

// stopped applies has_more, is_last and total.
func stopped(p *manifest.Pagination, resp *response, received int) (bool, error) {
	if p.HasMore != nil {
		b, err := boolAt(resp.doc, p.HasMore, "has_more")
		if err != nil {
			return false, err
		}
		if !b {
			return true, nil
		}
	}
	if p.IsLast != nil {
		b, err := boolAt(resp.doc, p.IsLast, "is_last")
		if err != nil {
			return false, err
		}
		if b {
			return true, nil
		}
	}
	if p.Total != nil {
		v, ok := p.Total.Get(resp.doc)
		if !ok {
			return false, fmt.Errorf("total %s is not in the response", p.Total.Raw)
		}
		n, ok := v.(json.Number)
		total, okRat := new(big.Rat).SetString(string(n))
		if !ok || !okRat {
			return false, fmt.Errorf("total %s is %v, not a number", p.Total.Raw, v)
		}
		if new(big.Rat).SetInt64(int64(received)).Cmp(total) >= 0 {
			return true, nil
		}
	}
	return false, nil
}

func boolAt(doc any, p *manifest.Path, key string) (bool, error) {
	v, ok := p.Get(doc)
	if !ok {
		return false, fmt.Errorf("%s %s is not in the response", key, p.Raw)
	}
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("%s %s is %v, not a boolean", key, p.Raw, v)
	}
	return b, nil
}

var linkNext = regexp.MustCompile(`<([^>]*)>\s*((?:;\s*[^;,]*)*)`)

// nextValue is the cursor, next URL or Link of a page; empty means none.
func nextValue(p *manifest.Pagination, resp *response, recs []map[string]any) (string, error) {
	switch p.Type {
	case manifest.PageLink:
		for _, h := range resp.header.Values("Link") {
			for _, m := range linkNext.FindAllStringSubmatch(h, -1) {
				for _, param := range strings.Split(m[2], ";") {
					k, v, _ := strings.Cut(strings.TrimSpace(param), "=")
					if strings.EqualFold(strings.TrimSpace(k), "rel") && slicesContainsFold(strings.Fields(strings.Trim(v, `"`)), "next") {
						return m[1], nil
					}
				}
			}
		}
		return "", nil
	case manifest.PageNext:
		v, _ := p.URLFrom.Get(resp.doc)
		return textValue(v, "url_from "+p.URLFrom.Raw)
	}
	switch c := p.Cursor; c.Kind {
	case manifest.CursorHeader:
		return resp.header.Get(c.Header), nil
	case manifest.CursorLast:
		v, _ := manifest.GetField(recs[len(recs)-1], c.Field)
		return textValue(v, "cursor field "+c.Field.Raw+" of the last record")
	default:
		v, _ := c.Path.Get(resp.doc)
		return textValue(v, "cursor "+c.Path.Raw)
	}
}

func slicesContainsFold(xs []string, s string) bool {
	for _, x := range xs {
		if strings.EqualFold(x, s) {
			return true
		}
	}
	return false
}

func textValue(v any, what string) (string, error) {
	switch x := v.(type) {
	case nil:
		return "", nil
	case string:
		return x, nil
	case json.Number:
		return x.String(), nil
	case bool:
		return strconv.FormatBool(x), nil
	}
	return "", fmt.Errorf("%s is %v, not a string or number", what, v)
}

// primaryKeys returns the encoded primary key of every record.
func primaryKeys(st *manifest.Stream, recs []map[string]any) map[string]bool {
	if len(st.PrimaryKey) == 0 || len(recs) == 0 {
		return nil
	}
	keys := map[string]bool{}
	for _, rec := range recs {
		parts := make([]any, len(st.PrimaryKey))
		for i, f := range st.PrimaryKey {
			parts[i], _ = manifest.GetField(rec, f)
		}
		b, _ := json.Marshal(parts)
		keys[string(b)] = true
	}
	return keys
}

func allSeen(keys, prev map[string]bool) bool {
	for k := range keys {
		if !prev[k] {
			return false
		}
	}
	return true
}

// extract selects the records of a response and adds records.add.
func extract(st *manifest.Stream, resp *response, scope manifest.Scope) ([]map[string]any, error) {
	rs := st.Records
	path := rs.Path
	if path == nil {
		path, _ = manifest.ParsePath("$")
	}
	v, ok := path.Get(resp.doc)
	if !ok {
		return nil, fmt.Errorf("stream %s: records path %s is not in the response (%s)", st.Name, path.Raw, resp.desc)
	}
	var recs []map[string]any
	switch rs.Mode {
	case manifest.ModeObject:
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("stream %s: the record at %s is %s, not an object (%s)", st.Name, path.Raw, kindOf(v), resp.desc)
		}
		recs = []map[string]any{obj}
	case manifest.ModeEntries:
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("stream %s: the entries at %s are %s, not an object (%s)", st.Name, path.Raw, kindOf(v), resp.desc)
		}
		for _, k := range sortedKeys(obj) {
			rec := map[string]any{}
			if fields, ok := obj[k].(map[string]any); ok {
				for fk, fv := range fields {
					rec[fk] = fv
				}
			} else {
				if rs.ValueField == nil {
					return nil, fmt.Errorf("stream %s: entry %q is %s, not an object, and records.value_field is not set (%s)", st.Name, k, kindOf(obj[k]), resp.desc)
				}
				if err := manifest.SetField(rec, *rs.ValueField, obj[k]); err != nil {
					return nil, err
				}
			}
			if err := manifest.SetField(rec, *rs.KeyField, k); err != nil {
				return nil, err
			}
			recs = append(recs, rec)
		}
	default:
		arr, ok := v.([]any)
		if !ok {
			return nil, fmt.Errorf("stream %s: records at %s are not an array (%s); got %s", st.Name, path.Raw, resp.desc, kindOf(v))
		}
		recs = make([]map[string]any, len(arr))
		for i, e := range arr {
			obj, ok := e.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("stream %s: record %d is %s, not an object (%s)", st.Name, i, kindOf(e), resp.desc)
			}
			recs[i] = obj
		}
	}
	if len(rs.Add) == 0 {
		return recs, nil
	}
	added := map[string]any{}
	for _, name := range sortedKeys(rs.Add) {
		src := rs.Add[name]
		switch {
		case src.Path != nil:
			v, ok := src.Path.Get(resp.doc)
			if !ok {
				return nil, fmt.Errorf("stream %s: records.add.%s: %s is not in the response (%s)", st.Name, name, src.Path.Raw, resp.desc)
			}
			added[name] = v
		case src.Template != nil:
			v, err := src.Template.Render(scope, nil)
			if err != nil {
				return nil, fmt.Errorf("stream %s: records.add.%s: %w", st.Name, name, err)
			}
			added[name] = v
		default:
			added[name] = src.Const
		}
	}
	for _, rec := range recs {
		for _, name := range sortedKeys(added) {
			f := manifest.Field{Raw: name, Parts: strings.Split(name, ".")}
			if cur, ok := manifest.GetField(rec, f); ok && cur != nil {
				continue
			}
			if err := manifest.SetField(rec, f, added[name]); err != nil {
				return nil, fmt.Errorf("stream %s: records.add: %w (%s)", st.Name, err, resp.desc)
			}
		}
	}
	return recs, nil
}

func kindOf(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case map[string]any:
		return "an object"
	case []any:
		return "an array"
	case string:
		return "a string"
	case json.Number:
		return "a number"
	case bool:
		return "a boolean"
	}
	return fmt.Sprintf("%T", v)
}
