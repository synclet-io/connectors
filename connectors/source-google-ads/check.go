package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check exchanges the refresh token for an access token and reads one row
// of every configured customer, naming the first cause that fails: the
// refresh token, the OAuth client, the developer token or a customer the
// credentials cannot reach.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	if _, err := c.auth.get(ctx); err != nil {
		return err
	}
	for _, cid := range cfg.customers() {
		err := c.search(ctx, cid, cfg.login(), "SELECT customer.id FROM customer LIMIT 1", func(map[string]any) error { return nil })
		if err != nil {
			return err
		}
	}
	return nil
}
