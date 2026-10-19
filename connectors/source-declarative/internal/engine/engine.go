// Package engine runs a manifest as a synclet source.
package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"source-declarative/internal/manifest"
)

// ManifestKey is the reserved config key that carries an injected manifest.
const ManifestKey = "__manifest"

// MaxManifestSize is the largest injected manifest, as compact JSON.
const MaxManifestSize = 512 * 1024

// Options are the engine's dependencies; zero values are the production
// ones. BaseURL replaces the manifest's base_url. OnRequest, when set, is
// called before every request of a stream with the page number within the
// current partition, window or parent.
type Options struct {
	BaseURL   string
	Client    *http.Client
	Now       func() time.Time
	Sleep     func(ctx context.Context, d time.Duration) error
	OnRequest func(stream string, page int)
}

func (o Options) withDefaults() Options {
	if o.Client == nil {
		o.Client = &http.Client{}
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.Sleep == nil {
		o.Sleep = sleep
	}
	return o
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

var printer = message.NewPrinter(language.English)

// source runs one manifest: the baked one, or, with m nil, the one each
// config injects.
type source struct {
	m *manifest.Manifest
	o Options
}

// New returns the source of a manifest baked into a catalog image. A config
// that carries __manifest is rejected.
func New(m *manifest.Manifest, o Options) synclet.Source[json.RawMessage] {
	return &source{m: m, o: o.withDefaults()}
}

// NewGeneric returns the source of the generic image: every config must
// carry its manifest in __manifest, with inline schemas.
func NewGeneric(o Options) synclet.Source[json.RawMessage] {
	return &source{o: o.withDefaults()}
}

// Failed returns a source whose every command fails with err; main uses it
// when the baked manifest does not load.
func Failed(err error) synclet.Source[json.RawMessage] { return failed{err} }

type failed struct{ err error }

func (f failed) Spec(context.Context) (*synclet.ConnectorSpecification, error) { return nil, f.err }
func (f failed) Check(context.Context, json.RawMessage) error                  { return f.err }
func (f failed) Discover(context.Context, json.RawMessage) (*synclet.Catalog, error) {
	return nil, f.err
}
func (f failed) Read(context.Context, json.RawMessage, *synclet.ConfiguredCatalog, *synclet.InputState, *synclet.Emitter) error {
	return f.err
}

// genericSpec is the spec of the generic image, whose form Synclet takes
// from the stored manifest instead.
func genericSpec() *synclet.ConnectorSpecification {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-declarative",
		SupportsIncremental: true,
		ProtocolVersion:     manifest.ProtocolVersion,
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Declarative HTTP Source Spec",
			Description: "The generic source-declarative image runs the manifest the config carries in __manifest, with the configuration that manifest's spec describes beside it. Synclet takes a builder connector's form from its stored manifest, not from this spec.",
			Type:        "object",
			Required:    []synclet.PropertyName{ManifestKey},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				ManifestKey: {
					Title:        "Manifest",
					Description:  "The connector's manifest as a JSON object, schemas inline.",
					PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Object}},
				},
			}},
		},
	}
}

// Spec returns the baked manifest's spec, or the generic placeholder.
func (s *source) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	if s.m == nil {
		return genericSpec(), nil
	}
	return s.m.ConnectorSpec(), nil
}

// prepare resolves the manifest of one command and validates the rest of
// the config against its spec.
func (s *source) prepare(raw json.RawMessage) (*manifest.Manifest, map[string]any, error) {
	return s.resolve(raw, false)
}

// resolve is prepare; relaxed loads an injected manifest whose streams may
// have no schema, for infer.
func (s *source) resolve(raw json.RawMessage, relaxed bool) (*manifest.Manifest, map[string]any, error) {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return nil, nil, synclet.ConfigError(fmt.Errorf("config is not a JSON object: %w", err))
	}
	injected, has := top[ManifestKey]
	delete(top, ManifestKey)
	m := s.m
	switch {
	case m != nil && has:
		return nil, nil, synclet.ConfigError(errors.New("this connector's manifest is built into its image; remove `__manifest` from the config"))
	case m == nil && !has:
		return nil, nil, synclet.ConfigError(errors.New("`__manifest` is required on the generic source-declarative image"))
	case m == nil:
		var err error
		if m, err = loadInjected(injected, relaxed); err != nil {
			return nil, nil, err
		}
	}
	cfg := map[string]any{}
	for k, v := range top {
		d := json.NewDecoder(bytes.NewReader(v))
		d.UseNumber()
		var x any
		if err := d.Decode(&x); err != nil {
			return nil, nil, synclet.ConfigError(fmt.Errorf("config.%s: %w", k, err))
		}
		cfg[k] = x
	}
	if err := validateConfig(m, cfg); err != nil {
		return nil, nil, synclet.ConfigError(err)
	}
	return m, cfg, nil
}

func loadInjected(raw json.RawMessage, relaxed bool) (*manifest.Manifest, error) {
	var compact bytes.Buffer
	if err := json.Compact(&compact, raw); err != nil {
		return nil, synclet.ConfigError(fmt.Errorf("`__manifest`: %w", err))
	}
	if n := compact.Len(); n > MaxManifestSize {
		return nil, synclet.ConfigError(fmt.Errorf("`__manifest` is %d KiB as JSON; at most 512 KiB is allowed", (n+1023)/1024))
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var doc any
	if err := d.Decode(&doc); err != nil {
		return nil, synclet.ConfigError(fmt.Errorf("`__manifest`: %w", err))
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return nil, synclet.ConfigError(errors.New("`__manifest` must be a JSON object, the parsed manifest"))
	}
	var m *manifest.Manifest
	var err error
	if relaxed {
		m, err = manifest.LoadDocumentRelaxed(obj)
	} else {
		m, err = manifest.LoadDocument(obj, nil)
	}
	if err != nil {
		var lines []string
		for _, e := range manifest.List(err) {
			p := ManifestKey
			if e.Path != "" {
				p += "." + e.Path
			}
			lines = append(lines, p+": "+e.Message)
		}
		return nil, synclet.ConfigError(fmt.Errorf("invalid `__manifest`: %s", strings.Join(lines, "; ")))
	}
	return m, nil
}

func validateConfig(m *manifest.Manifest, cfg map[string]any) error {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(m.ConfigSchema()))
	if err != nil {
		return err
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	const url = "mem://config.schema.json"
	if err := c.AddResource(url, doc); err != nil {
		return err
	}
	sch, err := c.Compile(url)
	if err != nil {
		return fmt.Errorf("spec: %w", err)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	if err := sch.Validate(inst); err != nil {
		var ve *jsonschema.ValidationError
		if errors.As(err, &ve) {
			return fmt.Errorf("invalid config: %s", configErrors(ve))
		}
		return fmt.Errorf("invalid config: %w", err)
	}
	return nil
}

func configErrors(ve *jsonschema.ValidationError) string {
	var out []string
	var walk func(*jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) > 0 {
			for _, c := range e.Causes {
				walk(c)
			}
			return
		}
		at := "config"
		if len(e.InstanceLocation) > 0 {
			at += "." + strings.Join(e.InstanceLocation, ".")
		}
		out = append(out, at+": "+e.ErrorKind.LocalizedString(printer))
	}
	walk(ve)
	return strings.Join(out, "; ")
}

// Check sends the first request of check.stream.
func (s *source) Check(ctx context.Context, raw json.RawMessage) error {
	m, cfg, err := s.prepare(raw)
	if err != nil {
		return err
	}
	return newRun(m, cfg, s.o, nil).check(ctx)
}

// Discover returns the manifest's streams; it sends no request.
func (s *source) Discover(_ context.Context, raw json.RawMessage) (*synclet.Catalog, error) {
	m, _, err := s.prepare(raw)
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{}
	for i := range m.Streams {
		cat.Streams = append(cat.Streams, catalogStream(&m.Streams[i], m.Streams[i].Schema))
	}
	return cat, nil
}

// catalogStream is st in a catalog, with schema as its JSON Schema.
func catalogStream(st *manifest.Stream, schema json.RawMessage) synclet.Stream {
	cs := synclet.Stream{
		Name:               st.Name,
		JSONSchema:         schema,
		SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh},
	}
	if st.Incremental != nil {
		cs.SupportedSyncModes = append(cs.SupportedSyncModes, synclet.SyncModeIncremental)
		cs.SourceDefinedCursor = true
		cs.DefaultCursorField = st.Incremental.CursorField.Parts
	}
	for _, f := range st.PrimaryKey {
		cs.SourceDefinedPrimaryKey = append(cs.SourceDefinedPrimaryKey, f.Parts)
	}
	return cs
}

// Read emits the configured streams in catalog order; a parent and its
// children are read in one pass where the first of them is configured.
func (s *source) Read(ctx context.Context, raw json.RawMessage, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	m, cfg, err := s.prepare(raw)
	if err != nil {
		return err
	}
	return newRun(m, cfg, s.o, em).read(ctx, catalog, state)
}
