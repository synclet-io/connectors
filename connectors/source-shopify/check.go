package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	checkDoc  = `query CheckShop { shop { name createdAt } currentAppInstallation { accessScopes { handle } } }`
	scopesDoc = `query AccessScopes { currentAppInstallation { accessScopes { handle } } }`
)

// scopeSet is the access scopes granted to the app.
type scopeSet map[string]bool

// has reports whether scope is granted; a write_ scope grants its read_
// scope.
func (s scopeSet) has(scope string) bool {
	return s[scope] || s["write_"+strings.TrimPrefix(scope, "read_")]
}

type scopesResult struct {
	AccessScopes []struct {
		Handle string `json:"handle"`
	} `json:"accessScopes"`
}

func (r scopesResult) set() scopeSet {
	s := scopeSet{}
	for _, sc := range r.AccessScopes {
		s[sc.Handle] = true
	}
	return s
}

func accessScopes(ctx context.Context, c *apiClient) (scopeSet, error) {
	var res struct {
		CurrentAppInstallation scopesResult `json:"currentAppInstallation"`
	}
	if err := c.query(ctx, "AccessScopes", scopesDoc, nil, &res); err != nil {
		return nil, err
	}
	return res.CurrentAppInstallation.set(), nil
}

func allOrdersError(from time.Time) error {
	return fmt.Errorf("the app lacks read_all_orders, so Shopify returns only the orders of the last 60 days, but the read starts at %s; grant read_all_orders or set start_date within the last 60 days",
		from.Format(time.RFC3339))
}

// allOrders is the scope without which Shopify returns only the orders of
// the last 60 days.
const allOrders = "read_all_orders"

// Check fetches a token when the config has client credentials, reads the
// shop and the app's access scopes, and fails naming every scope a stream
// needs that the app lacks, with the streams that need it. Without
// read_all_orders it fails when the start, start_date or the shop's
// creation, is more than 60 days back, and warns otherwise.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var res struct {
		Shop struct {
			Name      string    `json:"name"`
			CreatedAt time.Time `json:"createdAt"`
		} `json:"shop"`
		CurrentAppInstallation scopesResult `json:"currentAppInstallation"`
	}
	if err := c.query(ctx, "CheckShop", checkDoc, nil, &res); err != nil {
		return err
	}
	granted := res.CurrentAppInstallation.set()
	var missing []string
	for _, scope := range scopes() {
		if !granted.has(scope) {
			missing = append(missing, fmt.Sprintf("missing access scope %s (%s)", scope, strings.Join(streamsNeeding(scope), ", ")))
		}
	}
	if len(missing) > 0 {
		return synclet.ConfigError(errors.New(strings.Join(missing, "; ")))
	}
	if granted.has(allOrders) {
		return nil
	}
	start, ok, _ := cfg.start()
	if !ok {
		start = res.Shop.CreatedAt
	}
	if start.Before(c.now().Add(-ordersHistory)) {
		return synclet.ConfigError(allOrdersError(start))
	}
	synclet.Logger(ctx).Warn("the app lacks read_all_orders: orders, refunds, fulfillments and transactions hold only the orders of the last 60 days", "shop", c.domain())
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
