package main

import (
	"context"
	"errors"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// accountSet resolves the ad accounts of a read once: the configured ids, or
// every account the account search returns.
type accountSet struct {
	c       *apiClient
	ids     []string
	objects []map[string]any
	done    bool
}

func (a *accountSet) resolve(ctx context.Context) error {
	if a.done {
		return nil
	}
	if len(a.ids) == 0 {
		err := a.c.list(ctx, "adAccounts", query{{key: "q", value: "search"}}, func(obj map[string]any) error {
			id, err := convert(synclet.Integer, obj["id"])
			if err != nil {
				return fmt.Errorf("account without a numeric id: %w", err)
			}
			a.ids = append(a.ids, fmt.Sprint(id))
			a.objects = append(a.objects, obj)
			return nil
		})
		if err != nil {
			return fmt.Errorf("search ad accounts: %w", err)
		}
		if len(a.ids) == 0 {
			return synclet.ConfigError(errors.New("the user who authorized the app has no ad account"))
		}
	}
	a.done = true
	return nil
}

// Read emits every configured stream in catalog order, reading accounts one
// after another.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	accounts := &accountSet{c: c, ids: cfg.accounts()}
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		d, ok := streamByName(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		if err := accounts.resolve(ctx); err != nil {
			return err
		}
		var err error
		switch d.kind {
		case kindAccounts:
			err = readAccounts(ctx, c, accounts, d, desc, em)
		case kindEntity:
			err = readEntity(ctx, c, accounts.ids, d, desc, em)
		case kindAnalytics:
			err = s.readAnalytics(ctx, c, cfg, accounts.ids, d, cs, state, em)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func emit(em *synclet.Emitter, desc synclet.StreamDescriptor, d streamDef, obj map[string]any) error {
	rec, err := record(d.fields, obj)
	if err != nil {
		return err
	}
	return em.Record(desc, rec)
}

// readAccounts emits the searched accounts, or reads each configured one.
func readAccounts(ctx context.Context, c *apiClient, accounts *accountSet, d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	if accounts.objects != nil {
		for _, obj := range accounts.objects {
			if err := emit(em, desc, d, obj); err != nil {
				return fmt.Errorf("%s: %w", d.name, err)
			}
		}
		return nil
	}
	for _, id := range accounts.ids {
		var obj map[string]any
		if err := c.get(ctx, "adAccounts/"+id, nil, &obj); err != nil {
			return fmt.Errorf("%s: account %s: %w", d.name, id, err)
		}
		if err := emit(em, desc, d, obj); err != nil {
			return fmt.Errorf("%s: account %s: %w", d.name, id, err)
		}
	}
	return nil
}

func readEntity(ctx context.Context, c *apiClient, ids []string, d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	for _, id := range ids {
		q := query{{key: "q", value: d.finder}}
		if d.search != "" {
			q = append(q, param{key: "search", value: d.search, raw: true})
		}
		err := c.list(ctx, "adAccounts/"+id+"/"+d.resource, q, func(obj map[string]any) error {
			return emit(em, desc, d, obj)
		})
		if err != nil {
			return fmt.Errorf("%s: account %s: %w", d.name, id, err)
		}
	}
	return nil
}
