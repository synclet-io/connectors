package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// productsPageSize is the page size of the products list.
const productsPageSize = 100

func (s Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Discover returns the nine streams; the API has the same reports for every
// merchant account, so nothing is requested.
func (Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	cat := &synclet.Catalog{}
	for _, d := range streamDefs {
		cat.Streams = append(cat.Streams, d.stream())
	}
	return cat, nil
}

// Read emits every configured stream in catalog order, account by account.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		d, ok := streamDefByName(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		if d.cursor == "" {
			if err := s.readProducts(ctx, cfg, d, desc, em); err != nil {
				return err
			}
			continue
		}
		st, err := loadCursorState(desc, state)
		if err != nil {
			return err
		}
		read := reportWindow(d, desc, em)
		if d.report == nil {
			read = s.financialWindow(d, desc, em)
		}
		if err := s.readWindows(ctx, cfg, d, cs, st, em, read); err != nil {
			return err
		}
	}
	return nil
}

// readProducts lists the products of every account page by page until a
// page comes back empty; the offset advances by the products received, so a
// smaller page size enforced by the API skips nothing.
func (s Source) readProducts(ctx context.Context, cfg Config, d streamDef, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	for _, a := range cfg.Accounts {
		c := s.apiClient(a)
		for offset := 0; ; {
			q := url.Values{}
			q.Set("pagination[limit]", strconv.Itoa(productsPageSize))
			q.Set("pagination[offset]", strconv.Itoa(offset))
			var page struct {
				Data []json.RawMessage `json:"data"`
			}
			if err := c.getJSON(ctx, c.subscriptionsURL, "products?"+q.Encode(), &page); err != nil {
				return fmt.Errorf("%s: account %q: %w", d.name, a.Name, err)
			}
			objs, err := decodeAll(page.Data)
			if err != nil {
				return fmt.Errorf("%s: account %q: %w", d.name, a.Name, err)
			}
			for _, obj := range objs {
				rec, err := record(d.fields, obj, a.Name)
				if err != nil {
					return fmt.Errorf("%s: account %q: %w", d.name, a.Name, err)
				}
				if err := em.Record(desc, rec); err != nil {
					return err
				}
			}
			if len(page.Data) == 0 {
				break
			}
			offset += len(page.Data)
		}
	}
	return nil
}
