package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// lastSeenState is the state of engage and cohort_members: the latest
// $last_seen read.
type lastSeenState struct {
	LastSeen string `json:"last_seen,omitempty"`
}

// profile is one result of engage.
type profile struct {
	DistinctID any            `json:"$distinct_id"`
	Properties map[string]any `json:"$properties"`
}

type engagePage struct {
	Page      int       `json:"page"`
	PageSize  int       `json:"page_size"`
	SessionID string    `json:"session_id"`
	Total     int       `json:"total"`
	Results   []profile `json:"results"`
}

// profileFields converts profiles to records of one stream: it renames
// properties as discover did and turns the value of a string field that is
// not a string into its JSON text.
type profileFields struct {
	ren     renamer
	hints   map[string]bool
	strings map[string]bool
}

func newProfileFields(fixed map[synclet.PropertyName]synclet.PropertySpec, schema json.RawMessage) profileFields {
	pf := profileFields{ren: profileRenamer(fixed), hints: schemaHints(schema), strings: map[string]bool{}}
	var s struct {
		Properties map[string]struct {
			Type        synclet.PropTypes       `json:"type"`
			AirbyteType synclet.AirbytePropType `json:"airbyte_type"`
		} `json:"properties"`
	}
	if json.Unmarshal(schema, &s) == nil {
		for f, p := range s.Properties {
			if len(p.Type) > 0 && p.Type[0] == synclet.String && p.AirbyteType == "" {
				pf.strings[f] = true
			}
		}
	}
	return pf
}

func (pf profileFields) record(p profile) map[string]any {
	rec := make(map[string]any, len(p.Properties)+1)
	has := func(n string) bool {
		if pf.hints[n] {
			return true
		}
		_, ok := p.Properties[n]
		return ok
	}
	for k, v := range p.Properties {
		f := pf.ren.field(k, has)
		if pf.strings[f] {
			v = text(v)
		}
		rec[f] = v
	}
	rec["distinct_id"] = text(p.DistinctID)
	return rec
}

// profiles pages through engage with page_size and the session_id of the
// first page, optionally filtered to one cohort, until a page is shorter
// than its page_size.
func (r *reader) profiles(ctx context.Context, cohortID *int64, fn func(profile) error) error {
	size := r.cfg.pageSize()
	params := url.Values{"page_size": {strconv.Itoa(size)}}
	if cohortID != nil {
		params.Set("filter_by_cohort", fmt.Sprintf(`{"id":%d}`, *cohortID))
	}
	for page := 0; ; page++ {
		var resp engagePage
		req := request{api: queryAPI, method: http.MethodPost, path: "engage", params: params}
		if err := r.c.getJSON(ctx, req, &resp); err != nil {
			return err
		}
		for _, p := range resp.Results {
			if err := fn(p); err != nil {
				return err
			}
		}
		pageSize := resp.PageSize
		if pageSize <= 0 {
			pageSize = size
		}
		if len(resp.Results) < pageSize {
			return nil
		}
		if resp.SessionID == "" {
			return fmt.Errorf("page %d of engage has no session_id to request the next page", page)
		}
		next := url.Values{}
		for k, v := range params {
			next[k] = v
		}
		next.Set("session_id", resp.SessionID)
		next.Set("page", strconv.Itoa(page+1))
		params = next
	}
}

// emitProfile emits rec unless an incremental read saw a later last_seen
// before; it returns the greater of max and the record's last_seen.
// Mixpanel cannot filter profiles by $last_seen, so every read pages
// through all profiles and the cursor only spares the destination the
// unchanged ones. A profile without $last_seen is always emitted.
func (r *reader) emitProfile(desc synclet.StreamDescriptor, rec map[string]any, inc bool, saved, latest string) (string, error) {
	seen, _ := rec["last_seen"].(string)
	if inc && seen != "" && seen < saved {
		return latest, nil
	}
	if err := r.em.Record(desc, rec); err != nil {
		return latest, err
	}
	return max(latest, seen), nil
}

func (r *reader) engage(ctx context.Context, cs synclet.ConfiguredStream, inc bool, saved json.RawMessage) error {
	desc := cs.Stream.Descriptor()
	var st lastSeenState
	if err := decodeState(desc, saved, &st); err != nil {
		return err
	}
	pf := newProfileFields(profileProps(), cs.Stream.JSONSchema)
	latest := st.LastSeen
	err := r.profiles(ctx, nil, func(p profile) error {
		var err error
		latest, err = r.emitProfile(desc, pf.record(p), inc, st.LastSeen, latest)
		return err
	})
	if err != nil || !inc || latest == "" {
		return err
	}
	return r.em.StreamState(desc, lastSeenState{LastSeen: latest})
}

// cohort is one entry of cohorts/list with its fields as the API sent
// them.
type cohort struct {
	id      int64
	created string
	fields  map[string]any
}

// listCohorts returns the cohorts of the project, requested once per read.
func (r *reader) listCohorts(ctx context.Context) ([]cohort, error) {
	if r.cohorts != nil {
		return r.cohorts, nil
	}
	var raw []map[string]any
	if err := r.c.getJSON(ctx, request{api: queryAPI, method: http.MethodPost, path: "cohorts/list"}, &raw); err != nil {
		return nil, err
	}
	cohorts := make([]cohort, 0, len(raw))
	for i, f := range raw {
		n, _ := f["id"].(json.Number)
		id, err := n.Int64()
		if err != nil {
			return nil, fmt.Errorf("cohort %d of cohorts/list has no integer id", i)
		}
		created, _ := f["created"].(string)
		cohorts = append(cohorts, cohort{id: id, created: created, fields: f})
	}
	r.cohorts = cohorts
	return cohorts, nil
}

// createdState is the state of cohorts: the latest created read.
type createdState struct {
	Created string `json:"created,omitempty"`
}

// cohortList emits the cohorts; an incremental read skips those created
// before the saved created, which Mixpanel cannot filter by.
func (r *reader) cohortList(ctx context.Context, desc synclet.StreamDescriptor, inc bool, saved json.RawMessage) error {
	var st createdState
	if err := decodeState(desc, saved, &st); err != nil {
		return err
	}
	cohorts, err := r.listCohorts(ctx)
	if err != nil {
		return err
	}
	latest := st.Created
	for _, c := range cohorts {
		if inc && c.created != "" && c.created < st.Created {
			continue
		}
		if err := r.em.Record(desc, c.fields); err != nil {
			return err
		}
		latest = max(latest, c.created)
	}
	if !inc || latest == "" {
		return nil
	}
	return r.em.StreamState(desc, createdState{Created: latest})
}

// cohortMembers emits the profiles of each cohort with its cohort_id. The
// state is one last_seen across cohorts, emitted after the last one.
func (r *reader) cohortMembers(ctx context.Context, cs synclet.ConfiguredStream, inc bool, saved json.RawMessage) error {
	desc := cs.Stream.Descriptor()
	var st lastSeenState
	if err := decodeState(desc, saved, &st); err != nil {
		return err
	}
	cohorts, err := r.listCohorts(ctx)
	if err != nil {
		return err
	}
	pf := newProfileFields(cohortMembersFixed(), cs.Stream.JSONSchema)
	latest := st.LastSeen
	for _, c := range cohorts {
		id := c.id
		err := r.profiles(ctx, &id, func(p profile) error {
			rec := pf.record(p)
			rec["cohort_id"] = id
			var err error
			latest, err = r.emitProfile(desc, rec, inc, st.LastSeen, latest)
			return err
		})
		if err != nil {
			return fmt.Errorf("cohort %d: %w", id, err)
		}
	}
	if !inc || latest == "" {
		return nil
	}
	return r.em.StreamState(desc, lastSeenState{LastSeen: latest})
}
