package declarativetest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const pathsManifest = `version: 1
connector: {name: source-paths}
base_url: https://api.example.com/
spec:
  required: [repositories]
  properties:
    repositories: {type: array, items: {type: string}}
auth: {type: none}
check: {stream: repos}
streams:
  - name: repos
    path: "repos/{{ partition.value }}"
    partition: {values: "{{ config.repositories }}"}
    records: {mode: object}
    schema: {type: object, properties: {id: {type: integer}}}
`

func TestFixturesMatchTheEscapedPath(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "source-paths")
	write := func(name, body string) {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("manifest.yaml", pathsManifest)
	write("testdata/repos/octo/api.json", `{"id": 1}`)
	write("testdata/repos/octo/web%20app.json", `{"id": 2}`)

	h := New(t, Suite{Dir: dir, Config: json.RawMessage(`{"repositories":["octo/api","octo/web app"]}`)})
	out := h.Read(t, h.Catalog(t, synclet.SyncModeFullRefresh), nil)
	if out.Err != nil {
		t.Fatalf("read: %v", out.Err)
	}
	if n := len(out.Records(synclet.StreamDescriptor{Name: "repos"})); n != 2 {
		t.Errorf("records: got %d, want 2", n)
	}
}
