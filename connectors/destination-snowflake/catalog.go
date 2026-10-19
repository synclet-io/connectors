package main

import (
	"encoding/json"
	"fmt"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	colRawID       = "_AIRBYTE_RAW_ID"
	colExtractedAt = "_AIRBYTE_EXTRACTED_AT"
	colSeq         = "_SYNCLET_SEQ"
	shadowPrefix   = "_SYNCLET_TMP_"
)

// metaColumns are written to every table ahead of the catalog's columns.
var metaColumns = []column{
	{name: colRawID, ident: colRawID, typ: colVarchar},
	{name: colExtractedAt, ident: colExtractedAt, typ: colTimestampTZ},
}

// column is one property of a stream's schema and the column it lands in.
type column struct {
	name  string
	ident string
	typ   colType
}

// streamPlan is where and how one configured stream is written.
type streamPlan struct {
	index  int
	desc   synclet.StreamDescriptor
	mode   synclet.DestinationSyncMode
	table  string
	shadow string
	// columns are the schema's properties sorted by name.
	columns []column
	byName  map[string]int
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

func (p *streamPlan) pkColumns() []column {
	out := make([]column, len(p.pk))
	for i, idx := range p.pk {
		out[i] = p.columns[idx]
	}
	return out
}

// planStreams maps every configured stream to its table and columns. Every
// table goes in the configured schema and namespaces are ignored, so two
// streams whose names map to one table fail here, before anything is
// written.
func planStreams(catalog *synclet.ConfiguredCatalog) ([]*streamPlan, error) {
	tables := map[string]string{}
	claim := func(table, label string) error {
		if prev, ok := tables[table]; ok {
			return fmt.Errorf("streams %q and %q both map to table %q", prev, label, table)
		}
		tables[table] = label
		return nil
	}
	plans := make([]*streamPlan, 0, len(catalog.Streams))
	for i, cs := range catalog.Streams {
		p, err := planStream(i, cs)
		if err != nil {
			return nil, fmt.Errorf("stream %q: %w", cs.Stream.Name, err)
		}
		if err := claim(p.table, cs.Stream.Name); err != nil {
			return nil, err
		}
		if p.shadow != "" {
			if err := claim(p.shadow, cs.Stream.Name+" (overwrite staging)"); err != nil {
				return nil, err
			}
		}
		plans = append(plans, p)
	}
	return plans, nil
}

func planStream(i int, cs synclet.ConfiguredStream) (*streamPlan, error) {
	table, err := sfName("stream", cs.Stream.Name)
	if err != nil {
		return nil, err
	}
	p := &streamPlan{
		index:  i,
		desc:   cs.Stream.Descriptor(),
		mode:   cs.DestinationSyncMode,
		table:  table,
		byName: map[string]int{},
		cursor: -1,
	}
	switch p.mode {
	case synclet.DestinationSyncModeOverwrite:
		if p.shadow, err = sfName("stream", shadowPrefix+cs.Stream.Name); err != nil {
			return nil, fmt.Errorf("%w (with the prefix %s of the overwrite table)", err, shadowPrefix)
		}
	case synclet.DestinationSyncModeAppend, synclet.DestinationSyncModeAppendDedup:
	default:
		return nil, fmt.Errorf("destination sync mode %q is not supported: use overwrite, append or append_dedup", p.mode)
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

	owner := map[string]string{}
	for _, name := range names {
		id, err := sfName("property", name)
		if err != nil {
			return nil, err
		}
		if id == colSeq || slices.ContainsFunc(metaColumns, func(m column) bool { return m.ident == id }) {
			return nil, fmt.Errorf("property %q maps to column %q, which the connector writes", name, id)
		}
		if prev, ok := owner[id]; ok {
			return nil, fmt.Errorf("properties %q and %q both map to column %q", prev, name, id)
		}
		owner[id] = name
		prop, _ := props[name].(map[string]any)
		p.byName[name] = len(p.columns)
		p.columns = append(p.columns, column{name: name, ident: id, typ: columnType(prop)})
	}

	if p.mode != synclet.DestinationSyncModeAppendDedup {
		return p, nil
	}
	if len(cs.PrimaryKey) == 0 {
		return nil, fmt.Errorf("append_dedup needs a primary key")
	}
	for _, path := range cs.PrimaryKey {
		idx, err := p.topLevel("primary key", path)
		if err != nil {
			return nil, err
		}
		p.pk = append(p.pk, idx)
	}
	if len(cs.CursorField) > 0 {
		idx, err := p.topLevel("cursor", cs.CursorField)
		if err != nil {
			return nil, err
		}
		p.cursor = idx
	}
	return p, nil
}

// topLevel returns the column of a field path, which must name one top-level
// property of the schema.
func (p *streamPlan) topLevel(what string, path []string) (int, error) {
	if len(path) != 1 {
		return 0, fmt.Errorf("nested %s %v: only top-level fields are supported", what, path)
	}
	idx, ok := p.byName[path[0]]
	if !ok {
		return 0, fmt.Errorf("%s %q is not a property of the stream's schema", what, path[0])
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
