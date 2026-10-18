package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
)

// pageSize is the page[size] of list requests, the most Zendesk allows.
const pageSize = "100"

// paged requests a list page by page, following meta.after_cursor while
// meta.has_more.
func (r streamRead) paged(ctx context.Context) error {
	query := url.Values{"page[size]": {pageSize}}
	for {
		var body map[string]json.RawMessage
		if err := r.c.get(ctx, r.def.path, query, &body); err != nil {
			return err
		}
		if err := r.emit(body); err != nil {
			return err
		}
		var meta struct {
			HasMore     bool   `json:"has_more"`
			AfterCursor string `json:"after_cursor"`
		}
		if err := field(body, "meta", &meta); err != nil {
			return fmt.Errorf("GET %s: %w", r.def.path, err)
		}
		if !meta.HasMore || meta.AfterCursor == "" {
			return nil
		}
		query.Set("page[after]", meta.AfterCursor)
	}
}
