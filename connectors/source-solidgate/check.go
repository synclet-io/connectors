package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check requests one card order of the last hour for every account, so a
// wrong key pair, a blocked host or an exhausted rate limit fails here with
// the account and the cause.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	now := s.clock().UTC()
	req := reportRequest{
		DateFrom: now.Add(-time.Hour).Format(apiTimeLayout),
		DateTo:   now.Format(apiTimeLayout),
		Filter:   "updated_at",
		Limit:    1,
	}
	for _, a := range cfg.Accounts {
		c := s.apiClient(a)
		var body map[string]json.RawMessage
		if err := c.postJSON(ctx, c.reportsURL, "card-orders", req, &body); err != nil {
			return synclet.ConfigError(fmt.Errorf("account %q: %w", a.Name, err))
		}
	}
	return nil
}
