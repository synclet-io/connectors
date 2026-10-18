package main

import (
	"context"
	"encoding/json"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Read emits every configured stream in catalog order. All streams share one
// client, so the spacing of export requests holds across streams.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		def, ok := streamByName(desc.Name)
		if !ok {
			return synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		r := streamRead{c: c, cfg: cfg, def: def, desc: desc, em: em, incremental: cs.SyncMode == synclet.SyncModeIncremental}
		if r.incremental {
			r.saved = state.Stream(desc)
		}
		var err error
		switch def.kind {
		case cursorExport:
			err = r.cursorExport(ctx)
		case timeExport:
			err = r.timeExport(ctx)
		case paged:
			err = r.paged(ctx)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// streamRead is the read of one configured stream. saved is its STREAM
// state, nil for a full refresh or a first incremental read.
type streamRead struct {
	c           *apiClient
	cfg         Config
	def         streamDef
	desc        synclet.StreamDescriptor
	em          *synclet.Emitter
	incremental bool
	saved       json.RawMessage
}

// emit emits the records under the stream's key of body, each with only the
// fields its schema declares.
func (r streamRead) emit(body map[string]json.RawMessage) error {
	raw, ok := body[r.def.key]
	if !ok {
		return fmt.Errorf("GET %s: response has no %q", r.def.path, r.def.key)
	}
	var recs []map[synclet.PropertyName]json.RawMessage
	if err := json.Unmarshal(raw, &recs); err != nil {
		return fmt.Errorf("GET %s: decode %s: %w", r.def.path, r.def.key, err)
	}
	for _, rec := range recs {
		for k := range rec {
			if _, ok := r.def.props[k]; !ok {
				delete(rec, k)
			}
		}
		if err := r.em.Record(r.desc, rec); err != nil {
			return err
		}
	}
	return nil
}

// field decodes body[name] into out; a missing or null field leaves out as
// it is.
func field(body map[string]json.RawMessage, name string, out any) error {
	raw, ok := body[name]
	if !ok {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}

func (r streamRead) decodeState(out any) error {
	if err := json.Unmarshal(r.saved, out); err != nil {
		return synclet.ConfigError(fmt.Errorf("state of %s: %w", r.desc.Name, err))
	}
	return nil
}
