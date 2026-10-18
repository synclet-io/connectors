package main

import (
	"context"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// streamSet is every report of a configuration with the schema type of each
// metric they use.
type streamSet struct {
	defs  []reportDef
	kinds map[string]synclet.PropType
}

func (s streamSet) byName(name string) (reportDef, bool) {
	for _, d := range s.defs {
		if d.name == name {
			return d, true
		}
	}
	return reportDef{}, false
}

// streams returns the reports of cfg with metric types from the metadata of
// every configured property. A field a property does not offer, or a metric
// whose type differs between properties, is a configuration error.
func streams(ctx context.Context, c *apiClient, cfg Config) (streamSet, error) {
	defs, err := reportDefs(cfg)
	if err != nil {
		return streamSet{}, synclet.ConfigError(err)
	}
	set := streamSet{defs: defs, kinds: map[string]synclet.PropType{}}
	from := map[string]string{}
	for _, prop := range cfg.properties() {
		m, err := c.metadata(ctx, prop)
		if err != nil {
			return streamSet{}, fmt.Errorf("metadata of property %s: %w", prop, err)
		}
		dims := map[string]bool{}
		for _, d := range m.Dimensions {
			dims[d.APIName] = true
		}
		mets := map[string]string{}
		for _, x := range m.Metrics {
			mets[x.APIName] = x.Type
		}
		for _, d := range defs {
			for _, n := range d.dimensions {
				if !dims[n] {
					return streamSet{}, synclet.ConfigError(fmt.Errorf("%s %s: property %s has no dimension %s", d.kind(), d.name, prop, n))
				}
			}
			for _, n := range d.metrics {
				t, ok := mets[n]
				if !ok {
					return streamSet{}, synclet.ConfigError(fmt.Errorf("%s %s: property %s has no metric %s", d.kind(), d.name, prop, n))
				}
				k := metricKind(t)
				if prev, ok := set.kinds[n]; ok && prev != k {
					return streamSet{}, synclet.ConfigError(fmt.Errorf("%s %s: metric %s is %s in property %s and %s in property %s", d.kind(), d.name, n, prev, from[n], k, prop))
				}
				set.kinds[n], from[n] = k, prop
			}
		}
	}
	return set, nil
}

// Discover returns the built-in reports and one stream per custom report.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	c, err := s.apiClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	set, err := streams(ctx, c, cfg)
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{}
	for _, d := range set.defs {
		cat.Streams = append(cat.Streams, d.stream(set.kinds, cfg.dataQuality()))
	}
	return cat, nil
}
