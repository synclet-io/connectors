package main

import (
	"io"
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

	connectors, err := loadReleased(root, "ghcr.io/Org/Repo", published, io.Discard)
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

	none, err := loadReleased(root, "ghcr.io/org/repo", func(string, string) (bool, error) { return false, nil }, io.Discard)
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
		{"unreleased at initial-version", `{"initial-version": "0.2.0", "packages": {` + pkg + `}}`, `{}`, ""},
		{"unreleased at package initial-version", `{"initial-version": "0.1.0", "packages": {"connectors/source-acme": {"component": "source-acme", "initial-version": "0.2.0", "extra-files": [{"type": "json", "path": "meta.json", "jsonpath": "$.version"}]}}}`, `{}`, ""},
		{"unreleased past initial-version", `{"initial-version": "0.1.0", "packages": {` + pkg + `}}`, `{}`, `must have the initial-version 0.1.0, not 0.2.0`},
		{"unreleased without initial-version", `{"packages": {` + pkg + `}}`, `{}`, `sets no initial-version`},
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

func TestResolveReleaseLatestOnlyForNewestTag(t *testing.T) {
	root := t.TempDir()

	schema, err := os.ReadFile(filepath.Join("..", schemaPath))
	if err != nil {
		t.Fatal(err)
	}

	writeFile(t, root, schemaPath, string(schema))
	writeFile(t, root, "connectors/source-acme/Dockerfile", "FROM scratch\n")
	writeFile(t, root, "connectors/source-acme/spec.json", `{"connectionSpecification":{}}`)

	runGit(t, root, "init", "-q")

	for _, v := range []string{"0.1.0", "0.2.0", "0.10.0"} {
		commitAcme(t, root, v, `"license":"MIT"`)
		runGit(t, root, "tag", "source-acme-v"+v)
	}

	runGit(t, root, "tag", "source-acme-extra-v9.0.0")

	for _, tt := range []struct {
		version string
		latest  bool
	}{{"0.1.0", false}, {"0.2.0", false}, {"0.10.0", true}} {
		tag := "source-acme-v" + tt.version
		runGit(t, root, "checkout", "-q", tag)

		got, err := resolveRelease(root, tag)
		if err != nil {
			t.Fatal(err)
		}

		want := Release{Connector: "source-acme", Version: tt.version, Tag: tag, Latest: tt.latest}
		if got != want {
			t.Errorf("resolveRelease(%s) = %+v, want %+v", tag, got, want)
		}
	}
}

func TestLoadReleasedOldTagFailingTodaysSchema(t *testing.T) {
	root := t.TempDir()

	schema, err := os.ReadFile(filepath.Join("..", schemaPath))
	if err != nil {
		t.Fatal(err)
	}

	writeFile(t, root, "connectors/source-acme/Dockerfile", "FROM scratch\n")
	writeFile(t, root, "connectors/source-acme/spec.json", `{"connectionSpecification":{}}`)

	runGit(t, root, "init", "-q")

	// 0.1.0 has no license, which its own schema allowed.
	writeFile(t, root, schemaPath, `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["name","version"]}`)
	commitAcme(t, root, "0.1.0", `"tags":[]`)
	runGit(t, root, "tag", "source-acme-v0.1.0")

	// 0.2.0 has no license although its schema already required one.
	writeFile(t, root, schemaPath, string(schema))
	commitAcme(t, root, "0.2.0", `"tags":[]`)
	runGit(t, root, "tag", "source-acme-v0.2.0")

	commitAcme(t, root, "0.3.0", `"license":"MIT"`)

	var warn strings.Builder

	connectors, err := loadReleased(root, "ghcr.io/org/repo", func(string, string) (bool, error) { return true, nil }, &warn)
	if err != nil {
		t.Fatal(err)
	}

	if len(connectors) != 1 || connectors[0].Meta.Version != "0.1.0" {
		t.Fatalf("loadReleased = %+v, want source-acme at 0.1.0", connectors)
	}

	if !strings.Contains(warn.String(), "::warning::source-acme: falling back to source-acme-v0.1.0") ||
		!strings.Contains(warn.String(), "source-acme-v0.2.0") {
		t.Errorf("warnings = %q, want a ::warning:: annotation for the fallback past source-acme-v0.2.0", warn.String())
	}
}

func TestLoadReleasedFailsWhenNoPublishedTagLoads(t *testing.T) {
	root := t.TempDir()

	schema, err := os.ReadFile(filepath.Join("..", schemaPath))
	if err != nil {
		t.Fatal(err)
	}

	writeFile(t, root, schemaPath, string(schema))
	writeFile(t, root, "connectors/source-acme/Dockerfile", "FROM scratch\n")

	runGit(t, root, "init", "-q")

	// 0.1.0 has no spec.json.
	commitAcme(t, root, "0.1.0", `"license":"MIT"`)
	runGit(t, root, "tag", "source-acme-v0.1.0")

	// 0.2.0 has a meta.json that is not JSON.
	writeFile(t, root, "connectors/source-acme/spec.json", `{"connectionSpecification":{}}`)
	writeFile(t, root, "connectors/source-acme/meta.json", `{"name":`)
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "0.2.0")
	runGit(t, root, "tag", "source-acme-v0.2.0")

	commitAcme(t, root, "0.3.0", `"license":"MIT"`)

	connectors, err := loadReleased(root, "ghcr.io/org/repo", func(string, string) (bool, error) { return true, nil }, io.Discard)
	if err == nil {
		t.Fatalf("loadReleased = %+v, want an error: no published tag of source-acme loads", connectors)
	}

	for _, want := range []string{"source-acme", "source-acme-v0.2.0: parsing meta.json", "source-acme-v0.1.0: reading spec.json"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to contain %q", err, want)
		}
	}
}

func commitAcme(t *testing.T, root, version, extra string) {
	t.Helper()

	writeFile(t, root, "connectors/source-acme/meta.json", `{"name":"Acme","connector_type":"source","version":"`+version+
		`","release_stage":"alpha","support_level":"community",`+extra+`}`)
	runGit(t, root, "add", ".")
	runGit(t, root, "-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", version)
}

func TestNewestPublishedIgnoresWhichReleaseAsks(t *testing.T) {
	tags := []string{"source-acme-v0.1.0", "source-acme-v0.10.0", "source-acme-v0.2.0", "source-acme-extra-v9.0.0"}
	repo := "ghcr.io/org/repo/source-acme"

	pushed := map[string]bool{}
	published := func(r, v string) (bool, error) {
		if r != repo {
			t.Fatalf("checked %s, want %s", r, repo)
		}

		return pushed[v], nil
	}

	// Releases of 0.10.0 and 0.1.0 race; 0.10.0's :latest step runs first, then 0.1.0's.
	steps := []struct {
		pushed string
		want   string
	}{{"0.2.0", "0.2.0"}, {"0.10.0", "0.10.0"}, {"0.1.0", "0.10.0"}}

	for _, s := range steps {
		pushed[s.pushed] = true

		got, err := newestPublished(tags, "source-acme", repo, published)
		if err != nil {
			t.Fatal(err)
		}

		if got != s.want {
			t.Errorf("after pushing %s, :latest -> %q, want %q", s.pushed, got, s.want)
		}
	}

	none, err := newestPublished(tags, "source-gone", "ghcr.io/org/repo/source-gone", func(string, string) (bool, error) { return true, nil })
	if err != nil || none != "" {
		t.Errorf("newestPublished without tags = %q, %v, want empty", none, err)
	}
}
