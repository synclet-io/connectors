package main

import (
	"context"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Read emits every configured stream in catalog order. customer_client reads
// the configured customers, listing the accounts below a manager; every
// other stream reads each account resolveAccounts returns.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	set, err := streams(ctx, c, cfg)
	if err != nil {
		return err
	}
	var accts []account
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		d, ok := set.byName(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		targets := accts
		if d.name == streamCustomerClient && !d.custom {
			targets = nil
			for _, cid := range cfg.customers() {
				targets = append(targets, account{id: cid, login: cfg.login()})
			}
		} else if accts == nil {
			if accts, err = resolveAccounts(ctx, c, cfg); err != nil {
				return err
			}
			targets = accts
		}
		fs := d.fields(set.types)
		if d.incremental() {
			err = s.readDaily(ctx, c, cfg, d, fs, cs, targets, state, em)
		} else {
			err = readFull(ctx, c, d, fs, desc, targets, em)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func readFull(ctx context.Context, c *apiClient, d streamDef, fs []field, desc synclet.StreamDescriptor, accts []account, em *synclet.Emitter) error {
	for _, a := range accts {
		err := c.search(ctx, a.id, a.login, d.query.text, func(row map[string]any) error {
			return em.Record(desc, record(fs, row))
		})
		if err != nil {
			return fmt.Errorf("%s: customer %s: %w", d.name, a.id, err)
		}
	}
	return nil
}
