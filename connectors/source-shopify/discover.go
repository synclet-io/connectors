package main

import (
	"context"
	"errors"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const shopPlanDoc = `query ShopPlan { shop { plan { shopifyPlus } } }`

// Discover returns every stream. The B2B streams are left out, with a
// warning, on stores that are not on Shopify Plus, which have no B2B.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var res struct {
		Shop struct {
			Plan *struct {
				ShopifyPlus *bool `json:"shopifyPlus"`
			} `json:"plan"`
		} `json:"shop"`
	}
	if err := c.query(ctx, "ShopPlan", shopPlanDoc, nil, &res); err != nil {
		return nil, err
	}
	if res.Shop.Plan == nil || res.Shop.Plan.ShopifyPlus == nil {
		return nil, errors.New("ShopPlan: response without shop.plan.shopifyPlus")
	}
	b2b := *res.Shop.Plan.ShopifyPlus
	if !b2b {
		synclet.Logger(ctx).Warn("companies and company_locations left out: B2B needs a Shopify Plus store", "shop", c.domain())
	}
	catalog := &synclet.Catalog{}
	for _, st := range streams {
		if st.b2b() && !b2b {
			continue
		}
		catalog.Streams = append(catalog.Streams, st.catalogStream())
	}
	return catalog, nil
}

func (s stream) catalogStream() synclet.Stream {
	out := synclet.Stream{
		Name:                    s.name,
		JSONSchema:              synclet.ObjectSchema(s.schema()),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{"id"}},
	}
	if cur := s.cursor(); cur != "" {
		out.SupportedSyncModes = append(out.SupportedSyncModes, synclet.SyncModeIncremental)
		out.SourceDefinedCursor = true
		out.DefaultCursorField = []string{cur}
	}
	return out
}
