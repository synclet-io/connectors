package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const checkDoc = `query CheckShop { shop { name } currentAppInstallation { accessScopes { handle } } }`

// allOrders is the scope without which Shopify returns only the orders of
// the last 60 days.
const allOrders = "read_all_orders"

// Check fetches a token when the config has client credentials, reads the
// shop and the app's access scopes, and fails naming every scope a stream
// needs that the app lacks, with the streams that need it.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var res struct {
		Shop struct {
			Name string `json:"name"`
		} `json:"shop"`
		CurrentAppInstallation struct {
			AccessScopes []struct {
				Handle string `json:"handle"`
			} `json:"accessScopes"`
		} `json:"currentAppInstallation"`
	}
	if err := c.query(ctx, "CheckShop", checkDoc, nil, &res); err != nil {
		return err
	}
	granted := map[string]bool{}
	for _, sc := range res.CurrentAppInstallation.AccessScopes {
		granted[sc.Handle] = true
	}
	has := func(scope string) bool {
		return granted[scope] || granted["write_"+strings.TrimPrefix(scope, "read_")]
	}
	var missing []string
	for _, scope := range scopes() {
		if !has(scope) {
			missing = append(missing, fmt.Sprintf("missing access scope %s (%s)", scope, strings.Join(streamsNeeding(scope), ", ")))
		}
	}
	if len(missing) > 0 {
		return synclet.ConfigError(errors.New(strings.Join(missing, "; ")))
	}
	if !has(allOrders) {
		synclet.Logger(ctx).Warn("the app lacks read_all_orders: orders, refunds, fulfillments and transactions hold only the orders of the last 60 days", "shop", c.domain())
	}
	return nil
}

// scopes are the scopes of every stream, in the order streams first need
// them.
func scopes() []string {
	var out []string
	for _, st := range streams {
		for _, sc := range st.scopes {
			if !slices.Contains(out, sc) {
				out = append(out, sc)
			}
		}
	}
	return out
}

func streamsNeeding(scope string) []string {
	var out []string
	for _, st := range streams {
		if slices.Contains(st.scopes, scope) {
			out = append(out, st.name)
		}
	}
	return out
}
