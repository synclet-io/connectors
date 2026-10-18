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

// cursorState is the STREAM state of a cursor-based export: the
// after_cursor of the last page whose records were all emitted.
type cursorState struct {
	Cursor string `json:"cursor"`
}

// timeState is the STREAM state of ticket_events: the start_time of the
// next request.
type timeState struct {
	StartTime int64 `json:"start_time"`
}

// cursorExport requests the first page by start_time, or by the saved
// cursor, and then each after_cursor until end_of_stream. An incremental
// read saves the after_cursor of every page once its records are out.
func (r streamRead) cursorExport(ctx context.Context) error {
	query := url.Values{}
	if r.saved != nil {
		var st cursorState
		if err := r.decodeState(&st); err != nil {
			return err
		}
		if st.Cursor == "" {
			return synclet.ConfigError(fmt.Errorf("state of %s has no cursor", r.desc.Name))
		}
		query.Set("cursor", st.Cursor)
	} else {
		start, err := r.cfg.start()
		if err != nil {
			return synclet.ConfigError(err)
		}
		query.Set("start_time", strconv.FormatInt(startTime(start, r.c.now()), 10))
	}
	for {
		var body map[string]json.RawMessage
		if err := r.c.exportGet(ctx, r.def.path, query, &body); err != nil {
			return err
		}
		if err := r.emit(body); err != nil {
			return err
		}
		var after string
		var done bool
		if err := field(body, "after_cursor", &after); err != nil {
			return fmt.Errorf("GET %s: %w", r.def.path, err)
		}
		if err := field(body, "end_of_stream", &done); err != nil {
			return fmt.Errorf("GET %s: %w", r.def.path, err)
		}
		if after != "" && r.incremental {
			if err := r.em.StreamState(r.desc, cursorState{Cursor: after}); err != nil {
				return err
			}
		}
		if done || after == "" {
			return nil
		}
		query = url.Values{"cursor": {after}}
	}
}

// timeExport requests ticket_events from start_date, or from the saved
// start_time, and then from each response's end_time until end_of_stream or
// until end_time stops advancing. An incremental read saves the next
// start_time of every page once its records are out.
func (r streamRead) timeExport(ctx context.Context) error {
	from, err := r.cfg.start()
	if err != nil {
		return synclet.ConfigError(err)
	}
	if r.saved != nil {
		var st timeState
		if err := r.decodeState(&st); err != nil {
			return err
		}
		from = time.Unix(st.StartTime, 0).UTC()
	}
	start := startTime(from, r.c.now())
	for {
		var body map[string]json.RawMessage
		query := url.Values{"start_time": {strconv.FormatInt(start, 10)}}
		if err := r.c.exportGet(ctx, r.def.path, query, &body); err != nil {
			return err
		}
		if err := r.emit(body); err != nil {
			return err
		}
		var end int64
		var done bool
		if err := field(body, "end_time", &end); err != nil {
			return fmt.Errorf("GET %s: %w", r.def.path, err)
		}
		if err := field(body, "end_of_stream", &done); err != nil {
			return fmt.Errorf("GET %s: %w", r.def.path, err)
		}
		next := max(start, end)
		if r.incremental {
			if err := r.em.StreamState(r.desc, timeState{StartTime: next}); err != nil {
				return err
			}
		}
		if done || next == start {
			return nil
		}
		start = next
	}
}
