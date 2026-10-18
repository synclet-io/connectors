package main

import (
	"context"
	"fmt"
	"maps"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// streamSet is every stream of a configuration with the types of all their
// fields.
type streamSet struct {
	defs  []streamDef
	types map[string]fieldType
}

func (s streamSet) byName(name string) (streamDef, bool) {
	for _, d := range s.defs {
		if d.name == name {
			return d, true
		}
	}
	return streamDef{}, false
}

// streams returns the built-in streams and one per custom query. Fields of
// custom queries that knownFields lacks get their types from one
// GoogleAdsFields request; a field it does not know either is a
// configuration error.
func streams(ctx context.Context, c *apiClient, cfg Config) (streamSet, error) {
	custom, err := customDefs(cfg.CustomQueries)
	if err != nil {
		return streamSet{}, synclet.ConfigError(err)
	}
	set := streamSet{defs: append(append([]streamDef{}, builtinDefs...), custom...), types: maps.Clone(knownFields)}
	var unknown []string
	for _, d := range custom {
		for _, f := range d.query.fields {
			if _, ok := set.types[f]; !ok && !slices.Contains(unknown, f) {
				unknown = append(unknown, f)
			}
		}
	}
	if len(unknown) == 0 {
		return set, nil
	}
	found, err := c.fieldTypes(ctx, unknown)
	if err != nil {
		return streamSet{}, err
	}
	maps.Copy(set.types, found)
	for _, d := range custom {
		for _, f := range d.query.fields {
			if _, ok := set.types[f]; !ok {
				return streamSet{}, synclet.ConfigError(fmt.Errorf("custom query %s: unknown field %s", d.name, f))
			}
		}
	}
	return set, nil
}

// Discover returns the built-in streams and one stream per custom query.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	set, err := streams(ctx, s.apiClient(cfg), cfg)
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{}
	for _, d := range set.defs {
		cat.Streams = append(cat.Streams, d.stream(set.types))
	}
	return cat, nil
}
