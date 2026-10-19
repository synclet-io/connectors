package main

import (
	"fmt"

	"github.com/apache/iceberg-go"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// streamPlan is where and how one configured stream is written.
type streamPlan struct {
	index int
	desc  synclet.StreamDescriptor
	mode  synclet.DestinationSyncMode
	table string
	// columns are the metadata columns followed by the schema's properties
	// in schema order.
	columns []column
	byName  map[string]int
	// pk and cursor name columns; they are set for append_dedup only, and
	// cursor is "" when the stream has no cursor.
	pk     []string
	cursor string
}

// planStreams maps every configured stream to its table and columns. Every
// table goes in the configured namespace; stream namespaces are ignored, so
// two streams of the same name fail here, before anything is written.
func planStreams(catalog *synclet.ConfiguredCatalog, cfg Config) ([]*streamPlan, error) {
	tables := map[string]string{}
	plans := make([]*streamPlan, 0, len(catalog.Streams))
	for i, cs := range catalog.Streams {
		p, err := planStream(i, cs, cfg)
		if err != nil {
			return nil, fmt.Errorf("stream %q: %w", cs.Stream.Name, err)
		}
		label := cs.Stream.Name
		if cs.Stream.Namespace != "" {
			label = cs.Stream.Namespace + "." + cs.Stream.Name
		}
		if prev, ok := tables[p.table]; ok {
			return nil, fmt.Errorf("streams %q and %q both map to table %q", prev, label, p.table)
		}
		tables[p.table] = label
		plans = append(plans, p)
	}
	return plans, nil
}

func planStream(i int, cs synclet.ConfiguredStream, cfg Config) (*streamPlan, error) {
	if cs.Stream.Name == "" {
		return nil, fmt.Errorf("stream name is empty")
	}
	p := &streamPlan{
		index:  i,
		desc:   cs.Stream.Descriptor(),
		mode:   cs.DestinationSyncMode,
		table:  cs.Stream.Name,
		byName: map[string]int{},
	}
	switch p.mode {
	case synclet.DestinationSyncModeOverwrite, synclet.DestinationSyncModeAppend, synclet.DestinationSyncModeAppendDedup:
	default:
		return nil, fmt.Errorf("destination_sync_mode %q is not one of overwrite, append, append_dedup", p.mode)
	}
	cols, err := streamColumns(cs.Stream.JSONSchema, cfg)
	if err != nil {
		return nil, err
	}
	p.columns = cols
	for idx, c := range cols {
		p.byName[c.name] = idx
	}

	if p.mode != synclet.DestinationSyncModeAppendDedup {
		return p, nil
	}
	if len(cs.PrimaryKey) == 0 {
		return nil, fmt.Errorf("append_dedup needs a primary key")
	}
	for _, path := range cs.PrimaryKey {
		name, err := p.topLevel("primary key", path)
		if err != nil {
			return nil, err
		}
		if _, ok := p.columns[p.byName[name]].typ.(iceberg.Float64Type); ok {
			return nil, fmt.Errorf("primary key field %q is a double column; equality deletes need an exact type, so set number_type to decimal", name)
		}
		p.pk = append(p.pk, name)
	}
	if len(cs.CursorField) > 0 {
		name, err := p.topLevel("cursor", cs.CursorField)
		if err != nil {
			return nil, err
		}
		p.cursor = name
	}
	return p, nil
}

// topLevel returns the property a field path names, which must be one
// top-level property of the schema.
func (p *streamPlan) topLevel(what string, path []string) (string, error) {
	if len(path) != 1 {
		return "", fmt.Errorf("%s field %v is nested; only top-level fields are supported", what, path)
	}
	idx, ok := p.byName[path[0]]
	if !ok || idx < len(metaColumns()) {
		return "", fmt.Errorf("%s field %q is not a property of the stream's schema", what, path[0])
	}
	return path[0], nil
}
