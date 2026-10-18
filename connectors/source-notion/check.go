package main

import (
	"context"
	"errors"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// errNothingShared is the check failure of an integration that sees no page
// and no database.
var errNothingShared = errors.New("nothing is shared with the integration: add it to pages or databases under ⋯ → Connections in Notion")

// Check requests the integration's bot user, which fails on an invalid
// token, then one search result, which is missing when nothing is shared
// with the integration.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var me struct {
		ID string `json:"id"`
	}
	if err := c.get(ctx, "users/me", nil, &me); err != nil {
		return err
	}
	var p listPage
	if err := c.post(ctx, "search", map[string]any{"page_size": 1}, &p); err != nil {
		return err
	}
	if len(p.Results) == 0 {
		return synclet.ConfigError(errNothingShared)
	}
	return nil
}
