package main

import (
	"encoding/json"
	"fmt"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	colRawID       = "_airbyte_raw_id"
	colExtractedAt = "_airbyte_extracted_at"
	shadowPrefix   = "_synclet_tmp_"
	oldPrefix      = "_synclet_old_"
	// rowAlias names the incoming row in ON DUPLICATE KEY UPDATE.
	rowAlias = "_synclet_new"
	// maxStringKeys is how many VARCHAR(255) key columns fit InnoDB's
	// 3,072-byte index key.
	maxStringKeys = 3
)

// metaColumns are written to every table ahead of the catalog's columns.
var metaColumns = []column{
	{name: colRawID, ident: colRawID, kind: kindUUID, sqlType: kindUUID.sqlType()},
	{name: colExtractedAt, ident: colExtractedAt, kind: kindTimestampTZ, sqlType: kindTimestampTZ.sqlType()},
}

// column is one property of a stream's schema and the column it lands in.
type column struct {
	name    string
	ident   string
	kind    kind
	sqlType string
	// key marks a string primary key column, held as VARCHAR(255).
	key bool
}

// streamPlan is where and how one configured stream is written.
type streamPlan struct {
	index  int
	desc   synclet.StreamDescriptor
	mode   synclet.DestinationSyncMode
	table  string
	shadow string
	old    string
	// columns are the schema's properties sorted by name.
	columns []column
	byName  map[string]int
	byIdent map[string]string
	// pk and cursor index columns; they are set for append_dedup only, and
	// cursor is -1 when the stream has no cursor.
	pk     []int
	cursor int
}

// target is the table batches are inserted into: the shadow table for an
// overwrite stream, the table itself otherwise.
func (p *streamPlan) target() string {
	if p.mode == synclet.DestinationSyncModeOverwrite {
		return p.shadow
	}
	return p.table
}

// allColumns are the metadata columns followed by the schema's columns.
func (p *streamPlan) allColumns() []column {
	return append(slices.Clone(metaColumns), p.columns...)
}

// planStreams maps every configured stream to its table and columns. Every
// table goes in the configured database; namespaces are ignored, so two
// streams whose names map to the same table fail here, before anything is
// written.
func planStreams(catalog *synclet.ConfiguredCatalog) ([]*streamPlan, error) {
	tables := newNameSet("table")
	plans := make([]*streamPlan, 0, len(catalog.Streams))
	for i, cs := range catalog.Streams {
		p, err := planStream(i, cs)
		if err != nil {
			return nil, fmt.Errorf("stream %q: %w", cs.Stream.Name, err)
		}
		label := cs.Stream.Name
		if cs.Stream.Namespace != "" {
			label = cs.Stream.Namespace + "." + cs.Stream.Name
		}
		if err := tables.claim(p.table, label); err != nil {
			return nil, err
		}
		if p.shadow != "" {
			if err := tables.claim(p.shadow, "overwrite staging of "+label); err != nil {
				return nil, err
			}
			if err := tables.claim(p.old, "overwrite swap of "+label); err != nil {
				return nil, err
			}
		}
		plans = append(plans, p)
	}
	return plans, nil
}

func planStream(i int, cs synclet.ConfiguredStream) (*streamPlan, error) {
	p := &streamPlan{
		index:   i,
		desc:    cs.Stream.Descriptor(),
		mode:    cs.DestinationSyncMode,
		table:   myIdent(cs.Stream.Name),
		byName:  map[string]int{},
		byIdent: map[string]string{},
		cursor:  -1,
	}
	switch p.mode {
	case synclet.DestinationSyncModeOverwrite:
		p.shadow = myIdent(shadowPrefix + p.table)
		p.old = myIdent(oldPrefix + p.table)
	case synclet.DestinationSyncModeAppend, synclet.DestinationSyncModeAppendDedup:
	default:
		return nil, fmt.Errorf("destination_sync_mode %q is not one of overwrite, append, append_dedup", p.mode)
	}

	props, err := schemaProperties(cs.Stream.JSONSchema)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(props))
	for name := range props {
		names = append(names, name)
	}
	slices.Sort(names)

	cols := newNameSet("column")
	for _, m := range metaColumns {
		if err := cols.claim(m.ident, m.name); err != nil {
			return nil, err
		}
	}
	for _, name := range names {
		ident := myIdent(name)
		if err := cols.claim(ident, name); err != nil {
			return nil, err
		}
		prop, _ := props[name].(map[string]any)
		k := columnKind(prop)
		p.byName[name] = len(p.columns)
		p.byIdent[ident] = name
		p.columns = append(p.columns, column{name: name, ident: ident, kind: k, sqlType: k.sqlType()})
	}

	if p.mode != synclet.DestinationSyncModeAppendDedup {
		return p, nil
	}
	if len(cs.PrimaryKey) == 0 {
		return nil, fmt.Errorf("append_dedup needs a primary key")
	}
	stringKeys := 0
	for _, path := range cs.PrimaryKey {
		idx, err := p.topLevel("primary key", path)
		if err != nil {
			return nil, err
		}
		c := &p.columns[idx]
		switch c.kind {
		case kindJSON:
			return nil, fmt.Errorf("primary key field %q is an object, array or untyped value, which cannot be a MySQL key", c.name)
		case kindText, kindTimeTZ:
			stringKeys++
			c.key = true
			c.sqlType = keyTextType
		}
		p.pk = append(p.pk, idx)
	}
	if stringKeys > maxStringKeys {
		return nil, fmt.Errorf("primary key has %d string fields; at most %d fit a MySQL key", stringKeys, maxStringKeys)
	}
	if len(cs.CursorField) > 0 {
		idx, err := p.topLevel("cursor", cs.CursorField)
		if err != nil {
			return nil, err
		}
		if p.columns[idx].kind == kindJSON {
			return nil, fmt.Errorf("cursor field %q is an object, array or untyped value, which cannot be compared", p.columns[idx].name)
		}
		p.cursor = idx
	}
	return p, nil
}

// topLevel returns the column of a field path, which must name one top-level
// property of the schema.
func (p *streamPlan) topLevel(what string, path []string) (int, error) {
	if len(path) != 1 {
		return 0, fmt.Errorf("%s field %v is nested; only top-level fields are supported", what, path)
	}
	idx, ok := p.byName[path[0]]
	if !ok {
		return 0, fmt.Errorf("%s field %q is not a property of the stream's schema", what, path[0])
	}
	return idx, nil
}

// schemaProperties returns the "properties" of a stream's JSON schema.
func schemaProperties(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var schema struct {
		Properties map[string]any `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, fmt.Errorf("json_schema: %w", err)
	}
	return schema.Properties, nil
}
