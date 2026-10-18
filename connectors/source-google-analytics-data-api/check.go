package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check gets an access token and reads the metadata of every configured
// property, naming the first cause that fails: the service account key or
// refresh token, a disabled Data API, a property the credentials cannot
// read, or a field of a report a property does not offer.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c, err := s.apiClient(ctx, cfg)
	if err != nil {
		return err
	}
	if _, err := c.auth.get(); err != nil {
		return err
	}
	_, err = streams(ctx, c, cfg)
	return err
}
