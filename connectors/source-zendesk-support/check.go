package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Check requests the authenticated user. A 404 or an unknown host means the
// subdomain names no Zendesk account; incremental exports of tickets and
// users need an admin, so any other role fails.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	var me struct {
		User struct {
			Role string `json:"role"`
		} `json:"user"`
	}
	err := s.apiClient(cfg).get(ctx, "users/me.json", nil, &me)
	var he *httpError
	if errors.As(err, &he) && he.status == http.StatusNotFound {
		return synclet.ConfigError(fmt.Errorf("wrong subdomain %q: %s", cfg.Subdomain, he))
	}
	if err != nil {
		return err
	}
	if me.User.Role != "admin" {
		return synclet.ConfigError(fmt.Errorf("missing admin role for user export (role %q)", me.User.Role))
	}
	return nil
}
