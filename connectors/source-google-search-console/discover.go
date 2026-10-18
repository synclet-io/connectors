package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// catalog returns the streams of cfg: sites, sitemaps, the built-in reports
// and one stream per custom report.
func catalog(cfg Config) (*synclet.Catalog, error) {
	defs, err := reportDefs(cfg)
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{Streams: []synclet.Stream{sitesStream(), sitemapsStream()}}
	for _, d := range defs {
		cat.Streams = append(cat.Streams, d.stream())
	}
	return cat, nil
}

// Discover returns the streams of the configuration without calling the
// API: their schemas do not depend on the sites.
func (s Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	return catalog(cfg)
}
