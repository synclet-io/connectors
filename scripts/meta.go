package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
