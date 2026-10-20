package main

import (
	"encoding/json"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateAPIVersions(t *testing.T) {
	schema, err := compileSchema("..")
	if err != nil {
		t.Fatal(err)
	}

	const client = "package main\n\nconst (\n\tprod = \"https://api.example.com/v1.3/\"\n\tsandbox = \"https://sandbox.example.com/v1.3/\"\n)\n"

	cases := []struct {
		name    string
		version map[string]any
		files   map[string]string
		wantErr string
	}{
		{name: "every match is the pin"},
		{
			name:    "pinned differs from the code",
			version: map[string]any{"pinned": "v1.4"},
			wantErr: `captures "v1.3", meta.json pins "v1.4"`,
		},
		{
			name:    "one of two matches differs",
			files:   map[string]string{"client.go": strings.Replace(client, "sandbox.example.com/v1.3", "sandbox.example.com/v1.2", 1)},
			wantErr: `client.go:5 captures "v1.2"`,
		},
		{
			name:    "pattern matches nothing",
			version: map[string]any{"pattern": `api\.example\.com/(v\d+)/`},
			wantErr: "matches nothing",
		},
		{
			name:    "pattern with two capture groups",
			version: map[string]any{"pattern": `(example)\.com/(v[\d.]+)/`},
			wantErr: "exactly one capture group",
		},
		{
			name:    "unknown checker",
			version: map[string]any{"checker": "crystal-ball"},
			wantErr: "does not match",
		},
		{
			name:    "file outside the connector",
			version: map[string]any{"file": "../other/client.go"},
			wantErr: "inside the connector directory",
		},
		{
			name:    "missing file",
			version: map[string]any{"file": "config.go"},
			wantErr: "config.go",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := map[string]any{
				"api":     "Example API",
				"pinned":  "v1.3",
				"file":    "client.go",
				"pattern": `example\.com/(v[\d.]+)/`,
				"checker": "tiktok-probe",
				"docs":    "https://example.com/docs",
			}
			for k, val := range tc.version {
				v[k] = val
			}

			meta := map[string]any{
				"name": "Acme", "connector_type": "source", "version": "0.1.0",
				"release_stage": "alpha", "support_level": "community", "license": "MIT",
				"api_versions": []any{v},
			}

			metaJSON, err := json.Marshal(meta)
			if err != nil {
				t.Fatal(err)
			}

			files := map[string]string{
				"meta.json":  string(metaJSON),
				"Dockerfile": "FROM scratch\n",
				"spec.json":  `{"connectionSpecification":{}}`,
				"client.go":  client,
			}
			for k, val := range tc.files {
				files[k] = val
			}

			c, err := parseConnector("source-acme", schema, mapReader(files))

			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}

				got := c.Meta.APIVersions
				if len(got) != 1 || got[0].Pinned != "v1.3" || got[0].Line != 4 {
					t.Fatalf("APIVersions = %+v, want v1.3 at line 4", got)
				}

				return
			}

			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestEveryConnectorPinValidates(t *testing.T) {
	connectors, err := loadConnectors("..")
	if err != nil {
		t.Fatal(err)
	}

	pinned := map[string]bool{}

	for _, c := range connectors {
		for _, v := range c.Meta.APIVersions {
			pinned[c.Dir] = true

			if v.Line == 0 {
				t.Errorf("%s %s: no line recorded", c.Dir, v.API)
			}
		}
	}

	for _, dir := range []string{
		"source-facebook-marketing", "source-google-ads", "source-linkedin-ads", "source-shopify",
		"source-tiktok-marketing", "source-stripe", "source-klaviyo", "source-intercom", "source-notion",
		"source-github", "source-salesforce",
	} {
		if !pinned[dir] {
			t.Errorf("%s declares no api_versions", dir)
		}
	}
}

func mapReader(files map[string]string) fileReader {
	return func(name string) ([]byte, error) {
		data, ok := files[filepath.ToSlash(name)]
		if !ok {
			return nil, fs.ErrNotExist
		}

		return []byte(data), nil
	}
}
