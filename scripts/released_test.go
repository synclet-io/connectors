package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestReleasedVersionsNewestFirst(t *testing.T) {
	tags := []string{
		"source-acme-v0.9.0", "source-acme-v0.10.0", "source-acme-v0.10.0-rc.1", "source-acme-v1.0",
		"source-acme-extra-v2.0.0", "destination-acme-v3.0.0", "source-acme-v0.9.1",
	}

	got := releasedVersions(tags, "source-acme")
	want := []string{"0.10.0", "0.9.1", "0.9.0"}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("releasedVersions = %v, want %v", got, want)
	}
}

func TestLoadReleasedSkipsUnpublishedTag(t *testing.T) {
	root := t.TempDir()

	schema, err := os.ReadFile(filepath.Join("..", schemaPath))
	if err != nil {
		t.Fatal(err)
	}

	writeFile(t, root, schemaPath, string(schema))
	writeFile(t, root, "connectors/source-acme/Dockerfile", "FROM scratch\n")
	writeFile(t, root, "connectors/source-acme/spec.json", `{"connectionSpecification":{}}`)

	runGit(t, root, "init", "-q")

	for _, v := range []string{"0.1.0", "0.2.0"} {
		writeFile(t, root, "connectors/source-acme/meta.json", `{"name":"Acme","connector_type":"source","version":"`+v+
			`","release_stage":"alpha","support_level":"community","license":"MIT"}`)
		runGit(t, root, "add", ".")
		runGit(t, root, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", v)
		runGit(t, root, "tag", "source-acme-v"+v)
	}

	// A release tag of a connector directory that was since deleted.
	runGit(t, root, "tag", "source-gone-v1.0.0")

	// The working tree is ahead of every tag.
	writeFile(t, root, "connectors/source-acme/meta.json", `{"name":"Acme","connector_type":"source","version":"0.3.0",`+
		`"release_stage":"alpha","support_level":"community","license":"MIT"}`)

	var checked []string

	published := func(repo, tag string) (bool, error) {
		checked = append(checked, repo+":"+tag)

		return tag == "0.1.0", nil
	}

	connectors, err := loadReleased(root, "ghcr.io/Org/Repo", published)
	if err != nil {
		t.Fatal(err)
	}

	if len(connectors) != 1 || connectors[0].Meta.Version != "0.1.0" {
		t.Fatalf("loadReleased = %+v, want source-acme at 0.1.0", connectors)
	}

	want := []string{"ghcr.io/org/repo/source-acme:0.2.0", "ghcr.io/org/repo/source-acme:0.1.0"}
	if !reflect.DeepEqual(checked, want) {
		t.Errorf("checked %v, want %v", checked, want)
	}

	none, err := loadReleased(root, "ghcr.io/org/repo", func(string, string) (bool, error) { return false, nil })
	if err != nil {
		t.Fatal(err)
	}

	if len(none) != 0 {
		t.Errorf("loadReleased with nothing published = %+v, want none", none)
	}
}

func TestCheckReleasePlease(t *testing.T) {
	connectors := []Connector{{Dir: "source-acme", Meta: Meta{Version: "0.2.0"}}}
	pkg := `"connectors/source-acme": {"component": "source-acme", "extra-files": [{"type": "json", "path": "meta.json", "jsonpath": "$.version"}]}`

	tests := []struct {
		name     string
		config   string
		manifest string
		wantErr  string
	}{
		{"consistent", `{"packages": {` + pkg + `}}`, `{"connectors/source-acme": "0.2.0"}`, ""},
		{"manifest behind meta.json", `{"packages": {` + pkg + `}}`, `{"connectors/source-acme": "0.1.0"}`, `has version 0.2.0`},
		{"missing package", `{"packages": {}}`, `{"connectors/source-acme": "0.2.0"}`, `no package "connectors/source-acme"`},
		{"meta.json not bumped", `{"packages": {"connectors/source-acme": {"component": "source-acme"}}}`, `{"connectors/source-acme": "0.2.0"}`, `extra-files`},
		{"stale package", `{"packages": {` + pkg + `, "connectors/gone": {"component": "gone"}}}`, `{"connectors/source-acme": "0.2.0"}`, `"connectors/gone" is not a connector`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, root, releasePleaseConfig, tt.config)
			writeFile(t, root, releasePleaseManifest, tt.manifest)

			err := checkReleasePlease(root, connectors)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func writeFile(t *testing.T, root, name, content string) {
	t.Helper()

	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runGit(t *testing.T, root string, args ...string) {
	t.Helper()

	cmd := exec.Command("git", args...)
	cmd.Dir = root

	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}
