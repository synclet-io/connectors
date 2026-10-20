package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// listDef is a full refresh stream: one request whose response holds the
// records in the array under key.
type listDef struct {
	path, key string
}

var listDefs = map[string]listDef{
	streamCohorts:     {path: "3/cohorts", key: "cohorts"},
	streamAnnotations: {path: "2/annotations", key: "data"},
	streamEventsList:  {path: "2/events/list", key: "data"},
}

func (s Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// Read emits every configured stream in catalog order.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	for _, cs := range catalog.Streams {
		name := cs.Stream.Name
		var err error
		if m, ok := metricDefs[name]; ok {
			err = s.readMetric(ctx, cfg, c, m, cs, state, em)
		} else if l, ok := listDefs[name]; ok {
			err = readList(ctx, c, l, cs, em)
		} else if name == streamEvents {
			err = s.readEvents(ctx, cfg, c, cs, state, em)
		} else {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", name))
		}
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func readList(ctx context.Context, c *apiClient, d listDef, cs synclet.ConfiguredStream, em *synclet.Emitter) error {
	var resp map[string]json.RawMessage
	if err := c.getJSON(ctx, d.path, nil, &resp); err != nil {
		return err
	}
	raw, ok := resp[d.key]
	if !ok || string(raw) == "null" {
		return nil
	}
	var recs []map[string]any
	if err := decodeNumbers(raw, &recs); err != nil {
		return fmt.Errorf("GET /%s: decode %s: %w", d.path, d.key, err)
	}
	desc := cs.Stream.Descriptor()
	for _, rec := range recs {
		if err := em.Record(desc, rec); err != nil {
			return err
		}
	}
	return nil
}

// savedState decodes the stream's saved state into v; v is left as it is
// without one.
func savedState(state *synclet.InputState, desc synclet.StreamDescriptor, v any) error {
	if state == nil {
		return nil
	}
	raw := state.Stream(desc)
	if raw == nil {
		return nil
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("decode state: %w", err)
	}
	return nil
}

// decodeNumbers decodes raw into v with numbers kept as json.Number.
func decodeNumbers(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(v)
}
