package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// unverifiedPermission is the permission level of a site the user added but
// has not verified.
const unverifiedPermission = "siteUnverifiedUser"

// Check gets an access token and reads every configured site, naming the
// first cause that fails: the service account key or refresh token, a
// disabled Search Console API, or a site that is not verified for the
// account.
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
	for _, u := range cfg.SiteURLs {
		st, err := c.site(ctx, u)
		if err != nil {
			return err
		}
		if st.PermissionLevel == unverifiedPermission {
			return c.notVerified(u, nil)
		}
	}
	return nil
}
