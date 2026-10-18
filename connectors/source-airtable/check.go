package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// requiredScopes are the token scopes discover and read need.
var requiredScopes = []string{"data.records:read", "schema.bases:read"}

// Check requests the token's user and scopes, then the tables of every
// configured base, or the list of bases when none is configured.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var me struct {
		Scopes *[]string `json:"scopes"`
	}
	if err := c.get(ctx, "", "meta/whoami", nil, &me); err != nil {
		return err
	}
	if me.Scopes != nil {
		var missing []string
		for _, sc := range requiredScopes {
			if !slices.Contains(*me.Scopes, sc) {
				missing = append(missing, sc)
			}
		}
		if len(missing) > 0 {
			return synclet.ConfigError(fmt.Errorf("token lacks scope(s) %s", strings.Join(missing, ", ")))
		}
	}
	m := newMetadata(c)
	if len(cfg.BaseIDs) == 0 {
		bases, err := m.listBases(ctx)
		if statusOf(err) == http.StatusForbidden {
			return synclet.ConfigError(fmt.Errorf("the token cannot list bases, most likely it lacks the scope schema.bases:read: %w", err))
		}
		if err != nil {
			return err
		}
		if len(bases) == 0 {
			return synclet.ConfigError(errors.New("no base is shared with the token"))
		}
		return nil
	}
	for _, id := range cfg.BaseIDs {
		if _, err := m.baseTables(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
