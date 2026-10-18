package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check requests the latest rates with the configured base and symbols, so
// an invalid app_id or a plan without those features fails here.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	var r ratesResponse
	return s.apiClient(cfg).get(ctx, "latest.json", cfg.query(), &r)
}
