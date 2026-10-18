package main

import (
	"context"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Read emits every configured stream in catalog order, reading advertisers
// one after another.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		d, ok := streamByName(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		var err error
		switch d.kind {
		case kindAdvertisers:
			err = readAdvertisers(ctx, c, cfg, d, desc, em)
		case kindEntity:
			err = readEntity(ctx, c, cfg, d, desc, em)
		case kindReport:
			err = s.readReport(ctx, c, cfg, d, cs, state, em)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
