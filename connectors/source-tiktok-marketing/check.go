package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// advertiserBatch is how many advertiser ids one advertiser/info/ request takes.
const advertiserBatch = 100

// Check verifies the configuration, the access token and that every
// configured advertiser is authorised for it, naming the first cause that
// fails.
func (s Source) Check(ctx context.Context, cfg Config) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	for batch := range slices.Chunk(cfg.advertisers(), advertiserBatch) {
		if err := checkAdvertisers(ctx, c, batch); err != nil {
			return err
		}
	}
	return nil
}

// checkAdvertisers requests advertiser/info/ for ids. When the API refuses a
// batch of several ids with code 40001, which does not say which id, each id
// is checked on its own.
func checkAdvertisers(ctx context.Context, c *apiClient, ids []string) error {
	found := map[string]bool{}
	err := c.advertiserInfo(ctx, ids, []string{"advertiser_id", "name"}, func(obj map[string]any) error {
		if id, ok := stringValue(obj["advertiser_id"]); ok {
			found[id] = true
		}
		return nil
	})
	var ae *apiError
	if errors.As(err, &ae) {
		switch {
		case authCodes[ae.code]:
			return synclet.ConfigError(fmt.Errorf("access token is invalid or expired (code %d)", ae.code))
		case ae.code == codeNoPermission && len(ids) > 1:
			for _, id := range ids {
				if err := checkAdvertisers(ctx, c, []string{id}); err != nil {
					return err
				}
			}
			return err
		case ae.code == codeNoPermission:
			return synclet.ConfigError(fmt.Errorf("advertiser %s is not authorised for this access token (%w)", ids[0], ae))
		}
	}
	if err != nil {
		return err
	}
	for _, id := range ids {
		if !found[id] {
			return synclet.ConfigError(fmt.Errorf("advertiser %s is not authorised for this access token", id))
		}
	}
	return nil
}

// advertiserInfo calls fn with the advertiser/info/ object of every id.
func (c *apiClient) advertiserInfo(ctx context.Context, ids, fieldNames []string, fn func(map[string]any) error) error {
	q := url.Values{"advertiser_ids": {jsonList(ids...)}, "fields": {jsonList(fieldNames...)}}
	var data struct {
		List []map[string]any `json:"list"`
	}
	if err := c.get(ctx, "advertiser/info/", q, &data); err != nil {
		return err
	}
	for _, obj := range data.List {
		if err := fn(obj); err != nil {
			return err
		}
	}
	return nil
}
