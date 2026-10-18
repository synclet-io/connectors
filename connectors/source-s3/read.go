package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// streamState is the STREAM state of an incremental stream: the greatest
// LastModified of the objects read, and the keys of the objects read at
// exactly that time.
type streamState struct {
	Cursor string   `json:"cursor,omitempty"`
	Keys   []string `json:"keys,omitempty"`
}

// Read emits every configured stream in catalog order. A full refresh reads
// every matching object in key order; an incremental read the objects
// modified after the saved cursor, or at it under a key not read yet, in
// (LastModified, key) order, with a STATE after each object.
func (src Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	store, err := src.open(ctx, &cfg)
	if err != nil {
		return err
	}
	for _, cs := range catalog.Streams {
		i := slices.IndexFunc(cfg.Streams, func(s StreamConfig) bool { return s.Name == cs.Stream.Name })
		if i < 0 {
			return synclet.ConfigError(fmt.Errorf("stream %q is not in the configuration", cs.Stream.Name))
		}
		if err := readStream(ctx, store, cfg.Streams[i], cs, state, em); err != nil {
			return err
		}
	}
	return nil
}

func readStream(ctx context.Context, store objectStore, s StreamConfig, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	rs, err := parseSchema(cs.Stream.JSONSchema)
	if err != nil {
		return synclet.ConfigError(fmt.Errorf("stream %q: json_schema: %w", s.Name, err))
	}
	incremental := cs.SyncMode == synclet.SyncModeIncremental
	var st streamState
	var cursor time.Time
	if incremental {
		if len(cs.CursorField) > 0 && !slices.Equal(cs.CursorField, []string{fieldModified}) {
			return synclet.ConfigError(fmt.Errorf("stream %q: the cursor is %s, got cursor_field %q", s.Name, fieldModified, cs.CursorField))
		}
		if st, cursor, err = savedState(state.Stream(desc)); err != nil {
			return synclet.ConfigError(fmt.Errorf("stream %q: invalid state: %w", s.Name, err))
		}
	}
	objs, err := listMatching(ctx, store, s)
	if err != nil {
		return err
	}
	if incremental {
		slices.SortFunc(objs, func(a, b object) int {
			return cmp.Or(a.LastModified.Compare(b.LastModified), strings.Compare(a.Key, b.Key))
		})
		objs = slices.DeleteFunc(objs, func(o object) bool {
			return st.Cursor != "" && (o.LastModified.Before(cursor) || (o.LastModified.Equal(cursor) && slices.Contains(st.Keys, o.Key)))
		})
	} else {
		slices.SortFunc(objs, func(a, b object) int { return strings.Compare(a.Key, b.Key) })
	}
	for _, o := range objs {
		if err := readObject(ctx, store, s, rs, desc, o, em); err != nil {
			return err
		}
		if !incremental {
			continue
		}
		if st.Cursor == "" || o.LastModified.After(cursor) {
			cursor = o.LastModified
			st = streamState{Cursor: cursor.Format(time.RFC3339Nano), Keys: []string{o.Key}}
		} else {
			st.Keys = append(st.Keys, o.Key)
		}
		if err := em.StreamState(desc, st); err != nil {
			return err
		}
	}
	if incremental && len(objs) == 0 {
		return em.StreamState(desc, st)
	}
	return nil
}

func savedState(raw json.RawMessage) (streamState, time.Time, error) {
	var st streamState
	if raw == nil {
		return st, time.Time{}, nil
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return st, time.Time{}, err
	}
	if st.Cursor == "" {
		return streamState{}, time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339Nano, st.Cursor)
	if err != nil {
		return st, time.Time{}, fmt.Errorf("cursor: %w", err)
	}
	return st, t.UTC(), nil
}

// readObject emits the records of one object. A bad row fails the read
// unless the stream skips bad rows; then the skipped rows are counted and
// logged.
func readObject(ctx context.Context, store objectStore, s StreamConfig, rs *recordSchema, desc synclet.StreamDescriptor, o object, em *synclet.Emitter) (rerr error) {
	src, err := openRows(ctx, store, s, o.Key)
	if err != nil {
		return fmt.Errorf("stream %q: %w", s.Name, err)
	}
	defer func() { rerr = errors.Join(rerr, src.Close()) }()
	modified := o.LastModified.UTC().Format(time.RFC3339Nano)
	skipped := 0
	for row := 1; ; row++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		fields, err := src.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		var rec map[string]any
		if err == nil {
			rec, err = convertRow(rs, fields)
			if err != nil {
				err = &badRowError{err: err}
			}
		}
		var bad *badRowError
		switch {
		case errors.As(err, &bad) && s.SkipBadRows:
			skipped++
			continue
		case err != nil:
			return fmt.Errorf("stream %q: file %q row %d: %w", s.Name, o.Key, row, err)
		}
		rec[fieldFile] = o.Key
		rec[fieldRow] = row
		rec[fieldModified] = modified
		if err := em.Record(desc, rec); err != nil {
			return err
		}
	}
	if skipped > 0 {
		synclet.Logger(ctx).Warn("skipped bad rows", "stream", s.Name, "file", o.Key, "rows", skipped)
	}
	return nil
}
