package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	idProperty          = "_airtable_id"
	createdTimeProperty = "_airtable_created_time"

	hintBase  = "x-airtable-base-id"
	hintTable = "x-airtable-table-id"
	hintField = "x-airtable-field-id"

	lastModifiedTime = "lastModifiedTime"
)

type base struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type table struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Fields []field `json:"fields"`
}

type field struct {
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Type    string          `json:"type"`
	Options json.RawMessage `json:"options,omitempty"`
}

// resultOf returns the type and options of the value a formula, rollup or
// lookup field computes, from the field's options, or ok false when the API
// gives none.
func resultOf(options json.RawMessage) (typ string, resultOptions json.RawMessage, ok bool) {
	var o struct {
		Result *struct {
			Type    string          `json:"type"`
			Options json.RawMessage `json:"options"`
		} `json:"result"`
	}
	if len(options) == 0 || json.Unmarshal(options, &o) != nil || o.Result == nil || o.Result.Type == "" {
		return "", nil, false
	}
	return o.Result.Type, o.Result.Options, true
}

// metadata loads bases and their tables once per run.
type metadata struct {
	c      *apiClient
	bases  []base
	tables map[string][]table
}

func newMetadata(c *apiClient) *metadata {
	return &metadata{c: c, tables: map[string][]table{}}
}

// listBases returns every base the token can see, following offset.
func (m *metadata) listBases(ctx context.Context) ([]base, error) {
	if m.bases != nil {
		return m.bases, nil
	}
	all := []base{}
	query := url.Values{}
	for {
		var page struct {
			Bases  []base `json:"bases"`
			Offset string `json:"offset"`
		}
		if err := m.c.get(ctx, "", "meta/bases", query, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Bases...)
		if page.Offset == "" {
			break
		}
		query.Set("offset", page.Offset)
	}
	m.bases = all
	return all, nil
}

// baseTables returns the tables of base id. A 403 or 404 means the base is
// not shared with the token.
func (m *metadata) baseTables(ctx context.Context, id string) ([]table, error) {
	if t, ok := m.tables[id]; ok {
		return t, nil
	}
	var body struct {
		Tables []table `json:"tables"`
	}
	if err := m.c.get(ctx, id, "meta/bases/"+id+"/tables", nil, &body); err != nil {
		return nil, baseError(id, err)
	}
	m.tables[id] = body.Tables
	return body.Tables, nil
}

func baseError(id string, err error) error {
	switch statusOf(err) {
	case 403, 404:
		return synclet.ConfigError(fmt.Errorf("base %s is not shared with the token or does not exist: %w", id, err))
	}
	return err
}

// selectedBases returns the configured bases, or every visible base when
// none is configured.
func (m *metadata) selectedBases(ctx context.Context, cfg Config) ([]base, error) {
	all, err := m.listBases(ctx)
	if err != nil {
		return nil, err
	}
	if len(cfg.BaseIDs) == 0 {
		return all, nil
	}
	byID := map[string]base{}
	for _, b := range all {
		byID[b.ID] = b
	}
	out := make([]base, 0, len(cfg.BaseIDs))
	for _, id := range cfg.BaseIDs {
		b, ok := byID[id]
		if !ok {
			return nil, synclet.ConfigError(fmt.Errorf("base %s is not shared with the token or does not exist", id))
		}
		out = append(out, b)
	}
	return out, nil
}

// tableStream is one table of one base, with its stream name.
type tableStream struct {
	name  string
	base  base
	table table
}

// streams returns one tableStream per table of the selected bases. Tables
// are named "<base name>/<table name>"; bases whose names collide are
// named "<base name> (<base id>)" instead.
func (m *metadata) streams(ctx context.Context, cfg Config) ([]tableStream, error) {
	bases, err := m.selectedBases(ctx, cfg)
	if err != nil {
		return nil, err
	}
	count := map[string]int{}
	for _, b := range bases {
		count[b.Name]++
	}
	var out []tableStream
	for _, b := range bases {
		tables, err := m.baseTables(ctx, b.ID)
		if err != nil {
			return nil, err
		}
		prefix := b.Name
		if count[b.Name] > 1 {
			prefix = fmt.Sprintf("%s (%s)", b.Name, b.ID)
		}
		for _, t := range tables {
			out = append(out, tableStream{name: prefix + "/" + t.Name, base: b, table: t})
		}
	}
	return out, nil
}

// Discover returns one stream per table of the selected bases.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	streams, err := newMetadata(s.apiClient(cfg)).streams(ctx, cfg)
	if err != nil {
		return nil, err
	}
	catalog := &synclet.Catalog{Streams: []synclet.Stream{}}
	for _, ts := range streams {
		catalog.Streams = append(catalog.Streams, ts.stream())
	}
	return catalog, nil
}

func (ts tableStream) stream() synclet.Stream {
	s := synclet.Stream{
		Name:                    ts.name,
		JSONSchema:              tableSchema(ts.base.ID, ts.table),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{idProperty}},
	}
	for _, f := range ts.table.Fields {
		if f.Type == lastModifiedTime {
			s.SupportedSyncModes = append(s.SupportedSyncModes, synclet.SyncModeIncremental)
			s.DefaultCursorField = []string{propertyName(f)}
			break
		}
	}
	return s
}

// tableSchema returns the JSON schema of a table's records: the record id,
// its created time and every field under its name, with the base, table and
// field ids as x-airtable-* hints.
func tableSchema(baseID string, t table) json.RawMessage {
	props := map[string]any{
		idProperty:          map[string]any{"type": "string"},
		createdTimeProperty: timestamp(),
	}
	for _, f := range t.Fields {
		p := fieldSchema(f.Type, f.Options)
		p[hintField] = f.ID
		props[propertyName(f)] = p
	}
	b, err := json.Marshal(map[string]any{
		"type":       "object",
		"properties": props,
		hintBase:     baseID,
		hintTable:    t.ID,
	})
	if err != nil {
		panic(fmt.Sprintf("marshal schema of %s: %v", t.ID, err))
	}
	return b
}

// propertyName returns the record property of a field: its name, or
// "<name> (<field id>)" for a field named like a record property of its
// own.
func propertyName(f field) string {
	if f.Name == idProperty || f.Name == createdTimeProperty {
		return f.Name + " (" + f.ID + ")"
	}
	return f.Name
}

func nullable(t string) map[string]any { return map[string]any{"type": []string{t, "null"}} }

func timestamp() map[string]any {
	p := nullable("string")
	p["format"] = "date-time"
	p["airbyte_type"] = "timestamp_with_timezone"
	return p
}

func object(props ...string) map[string]any {
	p := nullable("object")
	ps := map[string]any{}
	for i := 0; i+1 < len(props); i += 2 {
		ps[props[i]] = nullable(props[i+1])
	}
	p["properties"] = ps
	return p
}

func arrayOf(items map[string]any) map[string]any {
	p := nullable("array")
	p["items"] = items
	return p
}

func collaborator() map[string]any {
	return object("id", "string", "email", "string", "name", "string")
}

func attachment() map[string]any {
	thumbnail := func() map[string]any { return object("url", "string", "width", "integer", "height", "integer") }
	p := object("id", "string", "url", "string", "filename", "string", "size", "integer", "type", "string", "width", "integer", "height", "integer")
	thumbnails := nullable("object")
	thumbnails["properties"] = map[string]any{"small": thumbnail(), "large": thumbnail(), "full": thumbnail()}
	p["properties"].(map[string]any)["thumbnails"] = thumbnails
	return p
}

// fieldSchema maps an Airtable field type to the JSON schema of its value.
// Formula and rollup fields take the schema of their result type, lookups
// an array of it; a type without a mapping, or a computed field without a
// result type, accepts any value.
func fieldSchema(typ string, options json.RawMessage) map[string]any {
	switch typ {
	case "singleLineText", "multilineText", "richText", "email", "url", "phoneNumber", "singleSelect", "externalSyncSource":
		return nullable("string")
	case "number", "percent", "currency", "duration":
		return nullable("number")
	case "autoNumber", "count", "rating":
		return nullable("integer")
	case "checkbox":
		return nullable("boolean")
	case "date":
		p := nullable("string")
		p["format"] = "date"
		return p
	case "dateTime", "createdTime", lastModifiedTime:
		return timestamp()
	case "multipleSelects", "multipleRecordLinks":
		return arrayOf(map[string]any{"type": "string"})
	case "singleCollaborator", "createdBy", "lastModifiedBy":
		return collaborator()
	case "multipleCollaborators":
		return arrayOf(collaborator())
	case "multipleAttachments":
		return arrayOf(attachment())
	case "barcode":
		return object("text", "string", "type", "string")
	case "button":
		return object("label", "string", "url", "string")
	case "aiText":
		return object("state", "string", "value", "string", "isStale", "boolean", "errorType", "string")
	case "formula", "rollup":
		if rt, ro, ok := resultOf(options); ok {
			return fieldSchema(rt, ro)
		}
	case "multipleLookupValues":
		if rt, ro, ok := resultOf(options); ok {
			return arrayOf(fieldSchema(rt, ro))
		}
	}
	return map[string]any{}
}
