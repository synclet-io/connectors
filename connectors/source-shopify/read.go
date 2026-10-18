package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

// lag is how far before now a read ends, so updates still being indexed
// are read by the next sync rather than missed.
const lag = time.Minute

// cursorState is the STREAM state of a windowed stream: the end of the
// last window whose records were all emitted.
type cursorState struct {
	Cursor string `json:"cursor"`
}

// reader reads the streams of one read command with one client, so query
// cost pacing holds across streams.
type reader struct {
	src     Source
	c       *apiClient
	cfg     Config
	em      *synclet.Emitter
	created time.Time
}

// Read emits every configured stream in catalog order.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	r := &reader{src: s, c: s.apiClient(cfg), cfg: cfg, em: em}
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		st, ok := findStream(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		conv, err := newConverter(cs.Stream.JSONSchema)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("stream %s: json_schema: %w", desc.Name, err))
		}
		incremental := cs.SyncMode == synclet.SyncModeIncremental
		if incremental && !st.windowed {
			return synclet.ConfigError(fmt.Errorf("stream %s supports full refresh only", desc.Name))
		}
		sr := streamRead{reader: r, st: st, desc: desc, conv: conv}
		if err := sr.read(ctx, incremental, state.Stream(desc)); err != nil {
			return fmt.Errorf("stream %s: %w", desc.Name, err)
		}
	}
	return nil
}

// start is start_date, or the shop's creation time when it is empty.
func (r *reader) start(ctx context.Context) (time.Time, error) {
	t, ok, err := r.cfg.start()
	if err != nil || ok {
		return t, err
	}
	if !r.created.IsZero() {
		return r.created, nil
	}
	var res struct {
		Shop struct {
			CreatedAt time.Time `json:"createdAt"`
		} `json:"shop"`
	}
	if err := r.c.query(ctx, "ShopCreatedAt", `query ShopCreatedAt { shop { createdAt } }`, nil, &res); err != nil {
		return time.Time{}, err
	}
	if res.Shop.CreatedAt.IsZero() {
		return time.Time{}, fmt.Errorf("ShopCreatedAt: response without shop.createdAt")
	}
	r.created = res.Shop.CreatedAt.UTC().Truncate(time.Second)
	return r.created, nil
}

// streamRead is the read of one configured stream.
type streamRead struct {
	*reader
	st   stream
	desc synclet.StreamDescriptor
	conv converter
}

// read reads a windowed stream window by window, from the saved cursor of
// an incremental read or the start, up to now minus lag; an incremental
// read saves each window's end once all its records are emitted. Other
// streams are read whole.
func (r streamRead) read(ctx context.Context, incremental bool, saved json.RawMessage) error {
	if !r.st.windowed {
		return r.window(ctx, nil)
	}
	from, err := r.start(ctx)
	if err != nil {
		return err
	}
	if incremental && saved != nil {
		var st cursorState
		if err := json.Unmarshal(saved, &st); err != nil {
			return synclet.ConfigError(fmt.Errorf("state: %w", err))
		}
		if st.Cursor != "" {
			if from, err = time.Parse(time.RFC3339, st.Cursor); err != nil {
				return synclet.ConfigError(fmt.Errorf("state cursor %q is not an RFC 3339 time", st.Cursor))
			}
			from = from.UTC()
		}
	}
	end := r.c.now().Add(-lag).UTC().Truncate(time.Second)
	if !from.Before(end) {
		if incremental {
			return r.em.StreamState(r.desc, cursorState{Cursor: from.Format(time.RFC3339)})
		}
		return nil
	}
	size := r.cfg.window()
	for w := (window{from: from}); w.from.Before(end); w.from = w.to {
		w.to = w.from.Add(size)
		if w.to.After(end) {
			w.to = end
		}
		if err := r.window(ctx, &w); err != nil {
			return err
		}
		if incremental {
			if err := r.em.StreamState(r.desc, cursorState{Cursor: w.to.Format(time.RFC3339)}); err != nil {
				return err
			}
		}
	}
	return nil
}

// window emits the records updated in w, or all records when w is nil.
func (r streamRead) window(ctx context.Context, w *window) error {
	label := "all records"
	if w != nil {
		label = "window " + w.String()
	}
	if !r.st.bulk {
		if err := r.paged(ctx, w); err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		return nil
	}
	res, err := r.src.runBulk(ctx, r.c, label, r.st.bulkQuery(w))
	if err != nil {
		return err
	}
	return multierr.Combine(res.each(ctx, r.emitBulk), res.Close())
}

// emitBulk emits the records of one root of a bulk output and its
// children, as the stream's shape says.
func (r streamRead) emitBulk(root map[string]json.RawMessage, children []map[string]json.RawMessage) error {
	switch r.st.shape {
	case shapeList:
		var items []map[string]json.RawMessage
		if raw, ok := root[r.st.list]; ok && string(raw) != "null" {
			if err := json.Unmarshal(raw, &items); err != nil {
				return fmt.Errorf("order %s: %s: %w", root["id"], r.st.list, err)
			}
		}
		for _, it := range items {
			it["order_id"], it["order_updated_at"] = root["id"], root["updatedAt"]
			if err := r.emit(it); err != nil {
				return err
			}
		}
		return nil
	case shapeChildren:
		for _, ch := range children {
			ch["location_id"] = root["id"]
			if err := r.emit(ch); err != nil {
				return err
			}
		}
		return nil
	}
	if r.st.nested != nil {
		if children == nil {
			children = []map[string]json.RawMessage{}
		}
		raw, err := json.Marshal(children)
		if err != nil {
			return err
		}
		root[r.st.nested.name] = raw
	} else if len(children) > 0 {
		return fmt.Errorf("record %s: %d unexpected child objects", root["id"], len(children))
	}
	return r.emit(root)
}

func (r streamRead) emit(obj map[string]json.RawMessage) error {
	rec, err := r.conv.record(obj)
	if err != nil {
		return fmt.Errorf("record %s: %w", obj["id"], err)
	}
	return r.em.Record(r.desc, rec)
}
