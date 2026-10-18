package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	// searchCap is the most results one CRM search query returns.
	searchCap = 10000
	// searchLimit is the page size of a search, the most HubSpot allows.
	searchLimit = 200
	// indexLag is how far before now an incremental read ends: the search
	// index trails writes, and a window closer to now could miss records
	// that later appear in it.
	indexLag = time.Minute
	// cursorLayout is the layout of the saved cursor, RFC 3339 in UTC with
	// milliseconds.
	cursorLayout = "2006-01-02T15:04:05.000Z07:00"
)

// apiRecord is a CRM object record of the batch read and search APIs.
type apiRecord struct {
	ID         string                     `json:"id"`
	Properties map[string]json.RawMessage `json:"properties"`
	CreatedAt  json.RawMessage            `json:"createdAt"`
	UpdatedAt  json.RawMessage            `json:"updatedAt"`
	Archived   json.RawMessage            `json:"archived"`
}

// objectRead is the read of one CRM object stream. props are the HubSpot
// properties the configured schema declares, top the top-level fields it
// declares.
type objectRead struct {
	c     *apiClient
	o     object
	desc  synclet.StreamDescriptor
	em    *synclet.Emitter
	props []string
	kinds map[string]propKind
	top   map[string]bool
}

func newObjectRead(c *apiClient, o object, st synclet.Stream, em *synclet.Emitter) (objectRead, error) {
	names, fields, err := declared(st.JSONSchema)
	if err != nil {
		return objectRead{}, synclet.ConfigError(fmt.Errorf("stream %s: json_schema: %w", o.name, err))
	}
	r := objectRead{c: c, o: o, desc: st.Descriptor(), em: em, kinds: map[string]propKind{}, top: map[string]bool{}}
	for _, n := range names {
		if p, ok := propertyName(n); ok {
			r.props = append(r.props, p)
			r.kinds[p] = kindOfField(fields[n])
			continue
		}
		r.top[n] = true
	}
	return r, nil
}

// emit emits rec with the fields the schema declares; a property missing
// from the response is null.
func (r objectRead) emit(rec apiRecord) error {
	if rec.ID == "" {
		return fmt.Errorf("record without an id")
	}
	out := map[string]any{}
	top := map[string]struct {
		kind propKind
		raw  json.RawMessage
	}{
		"createdAt": {kindDateTime, rec.CreatedAt},
		"updatedAt": {kindDateTime, rec.UpdatedAt},
	}
	for name, f := range top {
		if !r.top[name] {
			continue
		}
		v, err := convert(f.kind, f.raw)
		if err != nil {
			return fmt.Errorf("record %s: %s: %w", rec.ID, name, err)
		}
		out[name] = v
	}
	if r.top["id"] {
		out["id"] = rec.ID
	}
	if r.top["archived"] {
		var b *bool
		if len(rec.Archived) > 0 {
			if err := json.Unmarshal(rec.Archived, &b); err != nil {
				return fmt.Errorf("record %s: archived: %w", rec.ID, err)
			}
		}
		out["archived"] = b
	}
	for _, p := range r.props {
		v, err := convert(r.kinds[p], rec.Properties[p])
		if err != nil {
			return fmt.Errorf("record %s: property %s: %w", rec.ID, p, err)
		}
		out[string(propertyColumn(p))] = v
	}
	return r.em.Record(r.desc, out)
}

// batchError is an error entry of a 207 batch response.
type batchError struct {
	Category    string `json:"category"`
	SubCategory string `json:"subCategory"`
	Message     string `json:"message"`
}

func (e batchError) String() string {
	s := e.Category
	if e.SubCategory != "" {
		s += " " + e.SubCategory
	}
	return s + ": " + e.Message
}

// fullRefresh lists the ids of every unarchived record and reads each
// page of them with all declared properties through batch read, whose
// POST body holds property lists too long for a GET URL. A record deleted
// between the list and the batch read is missing from the batch result;
// the read logs how many went missing.
func (r objectRead) fullRefresh(ctx context.Context) error {
	return listIDs(ctx, r.c, r.o, func(ids []string) error {
		path := objectsPath(r.o) + "/batch/read"
		inputs := make([]map[string]string, len(ids))
		for i, id := range ids {
			inputs[i] = map[string]string{"id": id}
		}
		var body struct {
			Results []apiRecord  `json:"results"`
			Errors  []batchError `json:"errors"`
		}
		req := map[string]any{"properties": nonNil(r.props), "inputs": inputs}
		if err := r.c.post(ctx, path, url.Values{"archived": {"false"}}, req, &body); err != nil {
			return err
		}
		for _, e := range body.Errors {
			if e.Category != "OBJECT_NOT_FOUND" {
				return fmt.Errorf("POST %s: %s", path, e)
			}
		}
		byID := make(map[string]apiRecord, len(body.Results))
		for _, rec := range body.Results {
			byID[rec.ID] = rec
		}
		var missing int
		for _, id := range ids {
			rec, ok := byID[id]
			if !ok {
				missing++
				continue
			}
			if err := r.emit(rec); err != nil {
				return err
			}
		}
		if missing > 0 {
			synclet.Logger(ctx).Warn("records listed but missing from the batch read, deleted or archived in between", "stream", r.o.name, "missing", missing)
		}
		return nil
	})
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// cursorState is the STREAM state of an object stream: the end of the
// last window whose records were all emitted.
type cursorState struct {
	Cursor string `json:"cursor"`
}

// incremental searches the records modified from the saved cursor, or
// start_date, up to now minus indexLag, and saves that end as the cursor.
func (r objectRead) incremental(ctx context.Context, cfg Config, saved json.RawMessage) error {
	from, err := cfg.start()
	if err != nil {
		return synclet.ConfigError(err)
	}
	if saved != nil {
		var st cursorState
		if err := json.Unmarshal(saved, &st); err != nil {
			return synclet.ConfigError(fmt.Errorf("state: %w", err))
		}
		if st.Cursor != "" {
			if from, err = time.Parse(time.RFC3339Nano, st.Cursor); err != nil {
				return synclet.ConfigError(fmt.Errorf("state cursor %q is not an RFC 3339 time", st.Cursor))
			}
		}
	}
	fromMS := from.UnixMilli()
	toMS := r.c.now().Add(-indexLag).UnixMilli()
	if fromMS >= toMS {
		return r.em.StreamState(r.desc, cursorState{Cursor: formatMS(fromMS)})
	}
	return r.window(ctx, fromMS, toMS)
}

func formatMS(ms int64) string { return time.UnixMilli(ms).UTC().Format(cursorLayout) }

// searchPage is a page of the CRM search API.
type searchPage struct {
	Total   int         `json:"total"`
	Results []apiRecord `json:"results"`
	Paging  *paging     `json:"paging"`
}

// window reads the records modified in [fromMS, toMS) and then saves toMS
// as the cursor. When the window holds more than searchCap records it is
// halved and each half is read in turn; a window of one millisecond that
// still holds more fails the read, since no query could return all of it.
// Pages follow each other by hs_object_id, so records that share a
// modification time are neither skipped nor repeated.
func (r objectRead) window(ctx context.Context, fromMS, toMS int64) error {
	page, err := r.search(ctx, fromMS, toMS, "0")
	if err != nil {
		return err
	}
	if page.Total > searchCap {
		if toMS-fromMS <= 1 {
			return fmt.Errorf("search window [%s, %s) holds %d records, more than the %d one search returns, and cannot be split below 1 ms",
				formatMS(fromMS), formatMS(toMS), page.Total, searchCap)
		}
		mid := fromMS + (toMS-fromMS)/2
		if err := r.window(ctx, fromMS, mid); err != nil {
			return err
		}
		return r.window(ctx, mid, toMS)
	}
	for {
		for _, rec := range page.Results {
			if err := r.emit(rec); err != nil {
				return err
			}
		}
		if page.Paging == nil || page.Paging.Next.After == "" || len(page.Results) == 0 {
			break
		}
		last := page.Results[len(page.Results)-1].ID
		if page, err = r.search(ctx, fromMS, toMS, last); err != nil {
			return err
		}
	}
	return r.em.StreamState(r.desc, cursorState{Cursor: formatMS(toMS)})
}

type filter struct {
	PropertyName string `json:"propertyName"`
	Operator     string `json:"operator"`
	Value        string `json:"value"`
}

// search requests the records modified in [fromMS, toMS) with an id above
// afterID, in id order.
func (r objectRead) search(ctx context.Context, fromMS, toMS int64, afterID string) (searchPage, error) {
	body := map[string]any{
		"filterGroups": []map[string]any{{"filters": []filter{
			{PropertyName: r.o.modified, Operator: "GTE", Value: fmt.Sprint(fromMS)},
			{PropertyName: r.o.modified, Operator: "LT", Value: fmt.Sprint(toMS)},
			{PropertyName: "hs_object_id", Operator: "GT", Value: afterID},
		}}},
		"sorts":      []map[string]string{{"propertyName": "hs_object_id", "direction": "ASCENDING"}},
		"properties": nonNil(r.props),
		"limit":      searchLimit,
	}
	var page searchPage
	err := r.c.post(ctx, objectsPath(r.o)+"/search", nil, body, &page)
	return page, err
}
