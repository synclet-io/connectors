package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check exchanges the refresh token for an access token of the configured
// org and lists its objects, which needs API access for the user.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c, err := s.apiClient(cfg)
	if err != nil {
		return err
	}
	c.warnRotation()
	_, err = c.sobjects(ctx)
	return err
}
