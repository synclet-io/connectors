package manifest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// ProtocolVersion is the Airbyte protocol version every manifest connector
// reports.
const ProtocolVersion = "0.5.2"

// Incremental reports whether any stream supports incremental reads.
func (m *Manifest) Incremental() bool {
	return slices.ContainsFunc(m.Streams, func(s Stream) bool { return s.Incremental != nil })
}

// ConnectorSpec maps the manifest's spec block onto the connector
// specification: secret becomes airbyte_secret, and supportsIncremental,
// the protocol version and the destination sync modes are fixed. enum and
// pattern have no field in the SDK's PropertySpec; ConfigSchema keeps them
// and the engine validates every config against it.
func (m *Manifest) ConnectorSpec() *synclet.ConnectorSpecification {
	props := map[synclet.PropertyName]synclet.PropertySpec{}
	for name, p := range m.Spec.Properties {
		ps := synclet.PropertySpec{
			Title:        p.Title,
			Description:  p.Description,
			PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.PropType(p.Type)}},
			Format:       synclet.FormatType(p.Format),
			Order:        p.Order,
			IsSecret:     p.Secret,
		}
		if p.Default != nil {
			ps.Default = decodeAny(p.Default)
		}
		for _, e := range p.Examples {
			ps.Examples = append(ps.Examples, scalarText(e))
		}
		if p.Items != nil {
			ps.Items = map[string]any{"type": p.Items.Type}
		}
		props[synclet.PropertyName(name)] = ps
	}
	required := make([]synclet.PropertyName, len(m.Spec.Required))
	for i, r := range m.Spec.Required {
		required[i] = synclet.PropertyName(r)
	}
	return &synclet.ConnectorSpecification{
		DocumentationURL:    m.Connector.DocumentationURL,
		SupportsIncremental: m.Incremental(),
		ProtocolVersion:     ProtocolVersion,
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       m.Spec.Title,
			Description: m.Spec.Description,
			Type:        "object",
			Required:    required,
			Properties:  synclet.Properties{Properties: props},
		},
	}
}

// ConfigSchema returns the JSON Schema a config must satisfy: the spec
// block with enum, pattern and format kept.
func (m *Manifest) ConfigSchema() json.RawMessage {
	props := map[string]any{}
	for name, p := range m.Spec.Properties {
		props[name] = propertySchema(p)
	}
	schema := map[string]any{"type": "object", "properties": props}
	if len(m.Spec.Required) > 0 {
		schema["required"] = m.Spec.Required
	}
	b, err := json.Marshal(schema)
	if err != nil {
		panic(fmt.Sprintf("config schema: %v", err))
	}
	return b
}

func propertySchema(p Property) map[string]any {
	s := map[string]any{"type": p.Type}
	if len(p.Enum) > 0 {
		enum := make([]any, len(p.Enum))
		for i, e := range p.Enum {
			enum[i] = decodeAny(e)
		}
		s["enum"] = enum
	}
	if p.Pattern != "" {
		s["pattern"] = p.Pattern
	}
	if p.Format != "" {
		s["format"] = p.Format
	}
	if p.Items != nil {
		s["items"] = map[string]any{"type": p.Items.Type}
	}
	return s
}

// ApplyDefaults sets every spec property that has a default and is absent
// or null in cfg to that default.
func (m *Manifest) ApplyDefaults(cfg map[string]any) {
	for name, p := range m.Spec.Properties {
		if p.Default == nil {
			continue
		}
		if v, ok := cfg[name]; !ok || v == nil {
			cfg[name] = decodeAny(p.Default)
		}
	}
}

func decodeAny(raw json.RawMessage) any {
	var v any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return nil
	}
	return v
}

func scalarText(raw json.RawMessage) string {
	s, err := scalar(decodeAny(raw))
	if err != nil {
		return string(raw)
	}
	return s
}
