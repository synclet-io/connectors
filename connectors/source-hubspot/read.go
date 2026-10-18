package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// pageSize is the limit of list requests, the most the v3 list endpoints
// return.
const pageSize = "100"

// Read emits every configured stream in catalog order. All streams share
// one client, so its pacing holds across streams. The schemas API is
// requested only when a stream is not a standard object, a fixed stream or
// an association of two standard objects.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	var objs []object
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		if f, ok := findFixed(desc.Name); ok {
			if err := readFixed(ctx, c, f, desc, em); err != nil {
				return err
			}
			continue
		}
		known := standardObjects
		if _, _, ok := resolve(known, desc.Name); !ok {
			if objs == nil {
				all, err := allObjects(ctx, c)
				if err != nil {
					return err
				}
				objs = all
			}
			known = objs
		}
		o, p, ok := resolve(known, desc.Name)
		switch {
		case !ok:
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		case p != nil:
			if err := (assocRead{c: c, p: *p, desc: desc, em: em}).read(ctx); err != nil {
				return fmt.Errorf("stream %s: %w", desc.Name, err)
			}
		default:
			r, err := newObjectRead(c, o, cs.Stream, em)
			if err != nil {
				return err
			}
			if cs.SyncMode == synclet.SyncModeIncremental {
				err = r.incremental(ctx, cfg, state.Stream(desc))
			} else {
				err = r.fullRefresh(ctx)
			}
			if err != nil {
				return fmt.Errorf("stream %s: %w", desc.Name, err)
			}
		}
	}
	return nil
}

// resolve finds the object or the association pair of objs named name.
func resolve(objs []object, name string) (object, *pair, bool) {
	for _, o := range objs {
		if o.name == name {
			return o, nil, true
		}
	}
	for _, p := range pairs(objs) {
		if p.name() == name {
			return object{}, &p, true
		}
	}
	return object{}, nil, false
}

func findFixed(name string) (fixedStream, bool) {
	for _, f := range fixedStreams {
		if f.name == name {
			return f, true
		}
	}
	return fixedStream{}, false
}

func objectsPath(o object) string { return "crm/v3/objects/" + o.path }

// listPage is a page of a v3 list endpoint.
type listPage[T any] struct {
	Results []T    `json:"results"`
	Paging  paging `json:"paging"`
}

type paging struct {
	Next struct {
		After string `json:"after"`
	} `json:"next"`
}

// listIDs calls fn with the ids of every unarchived record of o, one list
// page at a time.
func listIDs(ctx context.Context, c *apiClient, o object, fn func(ids []string) error) error {
	query := url.Values{"limit": {pageSize}, "archived": {"false"}}
	for {
		var page listPage[struct {
			ID string `json:"id"`
		}]
		if err := c.get(ctx, objectsPath(o), query, &page); err != nil {
			return err
		}
		ids := make([]string, 0, len(page.Results))
		for _, r := range page.Results {
			if r.ID == "" {
				return fmt.Errorf("GET %s: record without an id", objectsPath(o))
			}
			ids = append(ids, r.ID)
		}
		if len(ids) > 0 {
			if err := fn(ids); err != nil {
				return err
			}
		}
		if page.Paging.Next.After == "" {
			return nil
		}
		query.Set("after", page.Paging.Next.After)
	}
}

// readFixed emits a fixed stream, following paging.next.after when the
// endpoint pages; each record carries only the fields its schema declares.
func readFixed(ctx context.Context, c *apiClient, f fixedStream, desc synclet.StreamDescriptor, em *synclet.Emitter) error {
	var query url.Values
	if f.paged {
		query = url.Values{"limit": {pageSize}, "archived": {"false"}}
	}
	for {
		var page listPage[map[synclet.PropertyName]json.RawMessage]
		if err := c.get(ctx, f.path, query, &page); err != nil {
			return fmt.Errorf("stream %s: %w", f.name, err)
		}
		for _, rec := range page.Results {
			for k := range rec {
				if _, ok := f.props[k]; !ok {
					delete(rec, k)
				}
			}
			if err := em.Record(desc, rec); err != nil {
				return err
			}
		}
		if !f.paged || page.Paging.Next.After == "" {
			return nil
		}
		query.Set("after", page.Paging.Next.After)
	}
}

// declared returns the names of the top-level properties of a stream's
// JSON schema with their schema, sorted by name.
func declared(schema json.RawMessage) ([]string, map[string]schemaField, error) {
	var s struct {
		Properties map[string]schemaField `json:"properties"`
	}
	if err := json.Unmarshal(schema, &s); err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(s.Properties))
	for n := range s.Properties {
		names = append(names, n)
	}
	slices.Sort(names)
	return names, s.Properties, nil
}
