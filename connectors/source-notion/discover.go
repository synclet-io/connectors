package main

import (
	"context"
	"encoding/json"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	usersStream     = "users"
	databasesStream = "databases"
	pagesStream     = "pages"
	blocksStream    = "blocks"

	cursorField = "last_edited_time"
)

// column is one top-level property of a stream's records and its JSON
// schema.
type column struct {
	name   string
	schema func() map[string]any
}

func nullable(t string) map[string]any { return map[string]any{"type": []string{"null", t}} }

func str() map[string]any     { return nullable("string") }
func boolean() map[string]any { return nullable("boolean") }
func object() map[string]any  { return nullable("object") }
func array() map[string]any   { return nullable("array") }
func integer() map[string]any { return nullable("integer") }

func timestamp() map[string]any {
	p := str()
	p["format"] = "date-time"
	p["airbyte_type"] = "timestamp_with_timezone"
	return p
}

// streamDef is a stream of the connector: its columns and whether it reads
// incrementally on last_edited_time.
type streamDef struct {
	name        string
	columns     []column
	incremental bool
}

var auditColumns = []column{
	{"created_time", timestamp},
	{"last_edited_time", timestamp},
	{"created_by", object},
	{"last_edited_by", object},
	{"archived", boolean},
	{"in_trash", boolean},
}

func withAudit(cols ...column) []column { return append(cols, auditColumns...) }

var streamDefs = []streamDef{
	{
		name: usersStream,
		columns: []column{
			{"id", str}, {"object", str}, {"type", str}, {"name", str}, {"avatar_url", str},
			{"person", object}, {"bot", object},
		},
	},
	{
		name: databasesStream,
		columns: withAudit(
			column{"id", str}, column{"object", str}, column{"database_id", str},
			column{"title", array}, column{"description", array}, column{"icon", object}, column{"cover", object},
			column{"properties", object}, column{"parent", object}, column{"database_parent", object},
			column{"is_inline", boolean}, column{"url", str}, column{"public_url", str},
		),
		incremental: true,
	},
	{
		name: pagesStream,
		columns: withAudit(
			column{"id", str}, column{"object", str}, column{"parent", object}, column{"properties", object},
			column{"icon", object}, column{"cover", object}, column{"is_locked", boolean},
			column{"url", str}, column{"public_url", str},
		),
		incremental: true,
	},
	{
		name: blocksStream,
		columns: withAudit(
			column{"id", str}, column{"object", str}, column{"page_id", str}, column{"depth", integer},
			column{"parent", object}, column{"type", str}, column{"content", object}, column{"has_children", boolean},
		),
		incremental: true,
	},
}

func streamDefByName(name string) (streamDef, bool) {
	for _, d := range streamDefs {
		if d.name == name {
			return d, true
		}
	}
	return streamDef{}, false
}

func (d streamDef) stream() synclet.Stream {
	props := map[string]any{}
	for _, c := range d.columns {
		props[c.name] = c.schema()
	}
	schema, err := json.Marshal(map[string]any{"type": "object", "properties": props})
	if err != nil {
		panic(fmt.Sprintf("marshal schema of %s: %v", d.name, err))
	}
	s := synclet.Stream{
		Name:                    d.name,
		JSONSchema:              schema,
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{"id"}},
	}
	if d.incremental {
		s.SupportedSyncModes = append(s.SupportedSyncModes, synclet.SyncModeIncremental)
		s.SourceDefinedCursor = true
		s.DefaultCursorField = []string{cursorField}
	}
	return s
}

// record returns the columns of d that obj has.
func (d streamDef) record(obj map[string]json.RawMessage) map[string]json.RawMessage {
	out := make(map[string]json.RawMessage, len(d.columns))
	for _, c := range d.columns {
		if v, ok := obj[c.name]; ok {
			out[c.name] = v
		}
	}
	return out
}

// Discover returns the users, databases, pages and blocks streams. Their
// schemas are fixed: page properties are the raw Notion property values.
func (s Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	catalog := &synclet.Catalog{Streams: []synclet.Stream{}}
	for _, d := range streamDefs {
		catalog.Streams = append(catalog.Streams, d.stream())
	}
	return catalog, nil
}
