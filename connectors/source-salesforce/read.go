package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Read emits every configured stream in catalog order. When the token
// endpoint rotates the refresh token, Read emits the configuration with the
// new one as a CONTROL message, so Synclet saves it.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	c, err := s.apiClient(cfg)
	if err != nil {
		return err
	}
	c.auth.rotated = func(_ context.Context, refreshToken string) error {
		updated := cfg
		updated.RefreshToken = refreshToken
		return em.Control(updated)
	}
	start, err := cfg.start()
	if err != nil {
		return synclet.ConfigError(err)
	}
	for _, cs := range catalog.Streams {
		r, err := newStreamRead(c, em, cs, state, cfg)
		if err != nil {
			return err
		}
		r.q.start = start
		if err := r.run(ctx); err != nil {
			return err
		}
	}
	return nil
}

// streamRead is the read of one configured stream: its object's fields, Id
// first, and the fields whose datetime values are normalized.
type streamRead struct {
	c           *apiClient
	em          *synclet.Emitter
	desc        synclet.StreamDescriptor
	fields      []string
	datetimes   map[string]bool
	all         bool
	incremental bool
	q           soql
	threshold   int
}

func newStreamRead(c *apiClient, em *synclet.Emitter, cs synclet.ConfiguredStream, state *synclet.InputState, cfg Config) (*streamRead, error) {
	desc := cs.Stream.Descriptor()
	var schema struct {
		Properties map[string]struct {
			Format string `json:"format"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(cs.Stream.JSONSchema, &schema); err != nil {
		return nil, synclet.ConfigError(fmt.Errorf("schema of %s: %w", desc.Name, err))
	}
	r := &streamRead{
		c: c, em: em, desc: desc, datetimes: map[string]bool{},
		incremental: cs.SyncMode == synclet.SyncModeIncremental,
		q:           soql{object: desc.Name},
		threshold:   cfg.threshold(),
	}
	for name, p := range schema.Properties {
		if name != "Id" {
			r.fields = append(r.fields, name)
		}
		if p.Format == string(synclet.DateTime) {
			r.datetimes[name] = true
		}
	}
	slices.Sort(r.fields)
	_, hasID := schema.Properties["Id"]
	if hasID {
		r.fields = append([]string{"Id"}, r.fields...)
	}
	_, r.all = schema.Properties["IsDeleted"]
	if len(cs.Stream.DefaultCursorField) > 0 {
		r.q.cursor = cs.Stream.DefaultCursorField[0]
	}
	if !r.incremental {
		return r, nil
	}
	if r.q.cursor == "" || !hasID {
		return nil, synclet.ConfigError(fmt.Errorf("%s has no SystemModstamp or LastModifiedDate, or no Id, so it supports full refresh only", desc.Name))
	}
	r.q.ordered = true
	if saved := state.Stream(desc); saved != nil {
		var p position
		if err := json.Unmarshal(saved, &p); err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
		}
		p, err := parsePosition(p)
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("state of %s: %w", desc.Name, err))
		}
		r.q.after = &p
	}
	return r, nil
}

// run queries the stream's records. A query whose encoded length exceeds
// the client's limit is read as ids first and then field chunks.
func (r *streamRead) run(ctx context.Context) error {
	full := r.q.build(r.fields)
	if len(encodeQuery(full)) <= r.c.maxSelect {
		return r.c.query(ctx, r.all, full, func(recs []map[string]any) error {
			if len(recs) == 0 {
				return r.page(recs, nil)
			}
			last := recs[len(recs)-1]
			pos, err := r.position(last[r.q.cursor], last["Id"])
			if err != nil {
				return err
			}
			return r.page(recs, pos)
		})
	}
	if len(r.fields) == 0 || r.fields[0] != "Id" {
		return fmt.Errorf("%s: the SELECT of all fields is longer than %d encoded characters and the object has no Id to read it in chunks", r.desc.Name, r.c.maxSelect)
	}
	driver := []string{"Id"}
	if r.q.cursor != "" {
		driver = append(driver, r.q.cursor)
	}
	return r.c.query(ctx, r.all, r.q.build(driver), func(rows []map[string]any) error {
		if len(rows) == 0 {
			return r.page(nil, nil)
		}
		// The page's position is its last id row's: a chunk query runs later
		// and may see a newer cursor value of the same record.
		last := rows[len(rows)-1]
		pos, err := r.position(last[r.q.cursor], last["Id"])
		if err != nil {
			return err
		}
		recs, err := r.chunks(ctx, rows)
		if err != nil {
			return err
		}
		return r.page(recs, pos)
	})
}

// chunks reads the fields of rows, a page of ids, by groups of ids and of
// fields that each fit into one query, and returns the merged records in
// the order of rows. A record missing from any chunk fails the read.
func (r *streamRead) chunks(ctx context.Context, rows []map[string]any) ([]map[string]any, error) {
	ids := make([]string, len(rows))
	quoted := make([]string, len(rows))
	for i, row := range rows {
		id, _ := row["Id"].(string)
		if !idPattern.MatchString(id) {
			return nil, fmt.Errorf("%s: record without a valid Id: %v", r.desc.Name, row["Id"])
		}
		ids[i], quoted[i] = id, "'"+id+"'"
	}
	budget := r.c.maxSelect - len(encodeQuery(byIDs(r.desc.Name, nil, nil)))
	fieldGroups, err := pack(r.fields[1:], budget/2)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r.desc.Name, err)
	}
	idGroups, err := pack(quoted, budget-budget/2)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r.desc.Name, err)
	}
	merged := make(map[string]map[string]any, len(ids))
	for _, group := range idGroups {
		for i := range group {
			group[i] = strings.Trim(group[i], "'")
		}
		for _, fields := range fieldGroups {
			if err := r.chunk(ctx, fields, group, merged); err != nil {
				return nil, err
			}
		}
	}
	recs := make([]map[string]any, len(ids))
	for i, id := range ids {
		recs[i] = merged[id]
	}
	return recs, nil
}

// chunk reads fields of the records ids into merged, keyed by Id.
func (r *streamRead) chunk(ctx context.Context, fields, ids []string, merged map[string]map[string]any) error {
	seen := map[string]bool{}
	err := r.c.query(ctx, r.all, byIDs(r.desc.Name, fields, ids), func(recs []map[string]any) error {
		for _, rec := range recs {
			id, _ := rec["Id"].(string)
			if merged[id] == nil {
				merged[id] = map[string]any{}
			}
			for k, v := range rec {
				merged[id][k] = v
			}
			seen[id] = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range ids {
		if !seen[id] {
			return fmt.Errorf("%s record %s is missing from the query of fields %s; it was purged after the id query, and the read stopped rather than emit it incomplete; run the sync again", r.desc.Name, id, strings.Join(fields, ", "))
		}
	}
	return nil
}

// position returns the position of a record with cursor value cursor and
// id id as the query returned them, or nil for a full refresh.
func (r *streamRead) position(cursor, id any) (*position, error) {
	if !r.incremental {
		return nil, nil
	}
	c, _ := cursor.(string)
	i, _ := id.(string)
	if c == "" || i == "" {
		return nil, fmt.Errorf("%s record %v has no %s", r.desc.Name, id, r.q.cursor)
	}
	c, err := normalizeTime(c)
	if err != nil {
		return nil, fmt.Errorf("%s field %s of record %s: %w", r.desc.Name, r.q.cursor, i, err)
	}
	return &position{Cursor: c, ID: i}, nil
}

// page emits recs, then pos as the stream's state when it is not nil, then
// stops the read when the org's API usage has reached the threshold.
func (r *streamRead) page(recs []map[string]any, pos *position) error {
	for _, rec := range recs {
		for f := range r.datetimes {
			v, ok := rec[f].(string)
			if !ok {
				continue
			}
			t, err := normalizeTime(v)
			if err != nil {
				return fmt.Errorf("%s field %s of record %v: %w", r.desc.Name, f, rec["Id"], err)
			}
			rec[f] = t
		}
		if err := r.em.Record(r.desc, rec); err != nil {
			return err
		}
	}
	if pos != nil {
		if err := r.em.StreamState(r.desc, *pos); err != nil {
			return err
		}
	}
	used, allotted := r.c.usage()
	if allotted > 0 && used*100 >= r.threshold*allotted {
		return fmt.Errorf("the org's API usage %d/%d (%d%%) reached api_usage_threshold_percent %d; "+
			"the read stopped after its last complete page, whose state is saved, to leave requests for other integrations; "+
			"the next sync continues from there, or raise api_usage_threshold_percent",
			used, allotted, used*100/allotted, r.threshold)
	}
	return nil
}
