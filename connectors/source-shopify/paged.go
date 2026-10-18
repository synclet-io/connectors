package main

import (
	"context"
	"encoding/json"
	"fmt"
)

// pageSize is the first argument of paged queries, which keeps a page of
// the paged streams well under the cost limit of one query.
const pageSize = 100

// pagedDoc is the query of a paged stream, with a $query variable when
// filtered.
func (s stream) pagedDoc(filtered bool) string {
	if s.single {
		return fmt.Sprintf("query %s { %s { %s } }", s.op, s.root, selection(s.fields))
	}
	vars, args := "$first: Int!, $after: String", "first: $first, after: $after, sortKey: ID"
	if s.args != "" {
		args += ", " + s.args
	}
	if filtered {
		vars += ", $query: String"
		args += ", query: $query"
	}
	return fmt.Sprintf("query %s(%s) { %s(%s) { pageInfo { hasNextPage endCursor } nodes { %s } } }",
		s.op, vars, s.root, args, selection(s.fields))
}

// paged emits the records of a paged stream updated in w, or all of them
// when w is nil, following endCursor. Pages are in id order, which an
// update does not change.
func (r streamRead) paged(ctx context.Context, w *window) error {
	doc := r.st.pagedDoc(w != nil)
	if r.st.single {
		var res map[string]map[string]json.RawMessage
		if err := r.c.query(ctx, r.st.op, doc, nil, &res); err != nil {
			return err
		}
		obj, ok := res[r.st.root]
		if !ok || obj == nil {
			return fmt.Errorf("%s: response without %s", r.st.op, r.st.root)
		}
		return r.emit(obj)
	}
	vars := map[string]any{"first": pageSize, "after": nil}
	if w != nil {
		vars["query"] = w.filter()
	}
	for {
		var res map[string]*struct {
			PageInfo struct {
				HasNextPage bool    `json:"hasNextPage"`
				EndCursor   *string `json:"endCursor"`
			} `json:"pageInfo"`
			Nodes []map[string]json.RawMessage `json:"nodes"`
		}
		if err := r.c.query(ctx, r.st.op, doc, vars, &res); err != nil {
			return err
		}
		page := res[r.st.root]
		if page == nil {
			return fmt.Errorf("%s: response without %s", r.st.op, r.st.root)
		}
		for _, n := range page.Nodes {
			if err := r.emit(n); err != nil {
				return err
			}
		}
		if !page.PageInfo.HasNextPage {
			return nil
		}
		if page.PageInfo.EndCursor == nil || *page.PageInfo.EndCursor == "" {
			return fmt.Errorf("%s: hasNextPage without an endCursor", r.st.op)
		}
		vars["after"] = *page.PageInfo.EndCursor
	}
}
