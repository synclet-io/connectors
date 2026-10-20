package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	connectorsDir = "connectors"
	schemaPath    = "schema/meta.schema.json"
)

// Meta mirrors schema/meta.schema.json.
type Meta struct {
	Name                      string                    `json:"name"`
	ConnectorType             string                    `json:"connector_type"`
	Version                   string                    `json:"version"`
	ReleaseStage              string                    `json:"release_stage"`
	SupportLevel              string                    `json:"support_level"`
	SourceType                string                    `json:"source_type"`
	License                   string                    `json:"license"`
	DocumentationURL          string                    `json:"documentation_url"`
	IconURL                   string                    `json:"icon_url"`
	ErdURL                    string                    `json:"erd_url"`
	Tombstone                 bool                      `json:"tombstone"`
	Language                  string                    `json:"language"`
	ReleaseDate               string                    `json:"release_date"`
	Tags                      []string                  `json:"tags"`
	BreakingChanges           map[string]BreakingChange `json:"breaking_changes"`
	MigrationDocumentationURL string                    `json:"migration_documentation_url"`
	ExternalDocumentationURLs []ExternalDoc             `json:"external_documentation_urls"`
	AllowedHosts              []string                  `json:"allowed_hosts"`
	SuggestedStreams          []string                  `json:"suggested_streams"`
	MaxSecondsBetweenMessages *int                      `json:"max_seconds_between_messages"`
	SupportsRefreshes         bool                      `json:"supports_refreshes"`
	SupportsFileTransfer      bool                      `json:"supports_file_transfer"`
	SupportsDataActivation    bool                      `json:"supports_data_activation"`
	ResourceRequirements      []JobResources            `json:"resource_requirements"`
	APIVersions               []APIVersion              `json:"api_versions"`
}

// APIVersion is one upstream API version the connector pins. Line is the
// line of the first match of Pattern in File, filled in by parseConnector.
type APIVersion struct {
	API     string            `json:"api"`
	Pinned  string            `json:"pinned"`
	File    string            `json:"file"`
	Pattern string            `json:"pattern"`
	Checker string            `json:"checker"`
	Params  map[string]string `json:"params,omitempty"`
	Docs    string            `json:"docs"`
	Line    int               `json:"-"`
}

type BreakingChange struct {
	Message                   string `json:"message"`
	MigrationDocumentationURL string `json:"migration_documentation_url"`
	UpgradeDeadline           string `json:"upgrade_deadline"`
}

type ExternalDoc struct {
	Title string `json:"title"`
	Type  string `json:"type"`
	URL   string `json:"url"`
}

type JobResources struct {
	JobType       string `json:"job_type"`
	MemoryRequest string `json:"memory_request"`
	MemoryLimit   string `json:"memory_limit"`
	CPURequest    string `json:"cpu_request"`
	CPULimit      string `json:"cpu_limit"`
}

// Connector is one directory under connectors/ with its parsed metadata and spec.
type Connector struct {
	Dir  string
	Meta Meta
	Spec json.RawMessage
}

// ReleaseTag is the git tag that marks the connector's current version as released.
func (c Connector) ReleaseTag() string {
	return c.Dir + "-v" + c.Meta.Version
}

func connectorDirs(root string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(root, connectorsDir))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", connectorsDir, err)
	}

	var dirs []string

	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			dirs = append(dirs, e.Name())
		}
	}

	sort.Strings(dirs)

	return dirs, nil
}

func compileSchema(root string) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.AssertFormat()

	schema, err := c.Compile(filepath.Join(root, schemaPath))
	if err != nil {
		return nil, fmt.Errorf("compiling %s: %w", schemaPath, err)
	}

	return schema, nil
}

// loadConnectors validates every connector and returns them sorted by directory.
// All problems are collected so one run reports everything.
func loadConnectors(root string) ([]Connector, error) {
	schema, err := compileSchema(root)
	if err != nil {
		return nil, err
	}

	dirs, err := connectorDirs(root)
	if err != nil {
		return nil, err
	}

	var (
		connectors []Connector
		errs       []error
	)

	for _, dir := range dirs {
		c, err := loadConnector(root, dir, schema)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", dir, err))

			continue
		}

		connectors = append(connectors, c)
	}

	return connectors, errors.Join(errs...)
}

// fileReader reads a file of one connector directory by its name, e.g. "meta.json".
type fileReader func(name string) ([]byte, error)

func workTreeReader(root, dir string) fileReader {
	return func(name string) ([]byte, error) {
		return os.ReadFile(filepath.Join(root, connectorsDir, dir, name))
	}
}

func loadConnector(root, dir string, schema *jsonschema.Schema) (Connector, error) {
	return parseConnector(dir, schema, workTreeReader(root, dir))
}

func parseConnector(dir string, schema *jsonschema.Schema, read fileReader) (Connector, error) {
	metaBytes, err := read("meta.json")
	if err != nil {
		return Connector{}, fmt.Errorf("reading meta.json: %w", err)
	}

	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(metaBytes))
	if err != nil {
		return Connector{}, fmt.Errorf("parsing meta.json: %w", err)
	}

	if err := schema.Validate(doc); err != nil {
		return Connector{}, fmt.Errorf("meta.json does not match %s:\n%w", schemaPath, err)
	}

	var meta Meta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return Connector{}, fmt.Errorf("decoding meta.json: %w", err)
	}

	if !strings.HasPrefix(dir, meta.ConnectorType+"-") {
		return Connector{}, fmt.Errorf("directory must start with %q to match connector_type", meta.ConnectorType+"-")
	}

	if err := checkAPIVersions(meta.APIVersions, read); err != nil {
		return Connector{}, err
	}

	if _, err := read("Dockerfile"); err != nil {
		return Connector{}, errors.New("missing Dockerfile")
	}

	specBytes, err := read("spec.json")
	if err != nil {
		return Connector{}, fmt.Errorf("reading spec.json (generate it with `task spec CONNECTOR=...`): %w", err)
	}

	spec, err := parseSpec(specBytes)
	if err != nil {
		return Connector{}, err
	}

	return Connector{Dir: dir, Meta: meta, Spec: spec}, nil
}

func parseSpec(data []byte) (json.RawMessage, error) {
	var spec struct {
		ConnectionSpecification map[string]any `json:"connectionSpecification"`
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parsing spec.json: %w", err)
	}

	if spec.ConnectionSpecification == nil {
		return nil, errors.New("spec.json has no connectionSpecification object")
	}

	return data, nil
}

// checkAPIVersions proves each declared pin against the code: every match of
// the pattern in the file captures exactly the pinned version.
func checkAPIVersions(versions []APIVersion, read fileReader) error {
	var errs []error

	seen := map[string]bool{}

	for i := range versions {
		v := &versions[i]
		if seen[v.API] {
			errs = append(errs, fmt.Errorf("api_versions: %q is declared twice", v.API))
		}

		seen[v.API] = true

		line, err := findPin(*v, read)
		if err != nil {
			errs = append(errs, fmt.Errorf("api_versions %q: %w", v.API, err))

			continue
		}

		v.Line = line
	}

	return errors.Join(errs...)
}

func findPin(v APIVersion, read fileReader) (int, error) {
	name := filepath.Clean(v.File)
	if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
		return 0, fmt.Errorf("file %s must be inside the connector directory", v.File)
	}

	re, err := regexp.Compile(v.Pattern)
	if err != nil {
		return 0, fmt.Errorf("pattern: %w", err)
	}

	if re.NumSubexp() != 1 {
		return 0, fmt.Errorf("pattern %s must have exactly one capture group", v.Pattern)
	}

	data, err := read(name)
	if err != nil {
		return 0, fmt.Errorf("reading %s: %w", v.File, err)
	}

	matches := re.FindAllSubmatchIndex(data, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("pattern %s matches nothing in %s", v.Pattern, v.File)
	}

	for _, m := range matches {
		if got := string(data[m[2]:m[3]]); got != v.Pinned {
			return 0, fmt.Errorf("%s:%d captures %q, meta.json pins %q", v.File, lineAt(data, m[2]), got, v.Pinned)
		}
	}

	return lineAt(data, matches[0][2]), nil
}

func lineAt(data []byte, offset int) int {
	return bytes.Count(data[:offset], []byte("\n")) + 1
}
