package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "write the generated fixtures to testdata")

// largeRows is the number of data rows of the Large sheet: more than one
// batch of the default batch_size.
const largeRows = defaultBatchSize + 500

// largeSheet is the values fixture of the Large sheet of spreadsheet A, one
// row per line: a header and largeRows synthetic rows.
func largeSheet() []byte {
	var b bytes.Buffer
	b.WriteString("{\"values\": [\n[\"ID\", \"Item\", \"Added\"]")
	day := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= largeRows; i++ {
		fmt.Fprintf(&b, ",\n[\"%d\", \"item-%05d\", \"%s\"]", i, i, day.AddDate(0, 0, i%365).Format(dateLayout))
	}
	b.WriteString("\n]}\n")
	return b.Bytes()
}

const dateLayout = "2006-01-02"

// generated are the fixtures TestGenerateFixtures writes, by path.
func generated() map[string][]byte {
	return map[string][]byte{
		filepath.Join("testdata", "sheets", testSheetA, "values", "Large.json"): largeSheet(),
	}
}

// TestGenerateFixtures checks that the generated fixtures in testdata are
// current; with -update it writes them:
//
//	go test -run TestGenerateFixtures -update ./...
func TestGenerateFixtures(t *testing.T) {
	for path, want := range generated() {
		if !json.Valid(want) {
			t.Fatalf("%s: generated fixture is not JSON", path)
		}
		if *update {
			if err := os.WriteFile(path, want, 0o644); err != nil { //nolint:gosec // fixtures are world-readable source files
				t.Fatal(err)
			}
			continue
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%v: run go test -run TestGenerateFixtures -update ./...", err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("%s is out of date: run go test -run TestGenerateFixtures -update ./...", path)
		}
	}
}

var (
	emailInFixture = regexp.MustCompile(`[A-Za-z0-9._%+-]+@([A-Za-z0-9-]+\.)+[A-Za-z]+`)
	// forbiddenInFixture are markers of real credentials: a private key, a
	// Google OAuth access token, refresh token or client secret.
	forbiddenInFixture = []string{"private_key", "PRIVATE KEY", "ya29.", "1//0", "GOCSPX-"}
)

// TestFixturesClean fails when a file in testdata holds a credential or an
// email address outside example.com.
func TestFixturesClean(t *testing.T) {
	err := filepath.WalkDir("testdata", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, f := range forbiddenInFixture {
			if bytes.Contains(raw, []byte(f)) {
				t.Errorf("%s contains %q", path, f)
			}
		}
		for _, m := range emailInFixture.FindAll(raw, -1) {
			if !strings.HasSuffix(string(m), "@example.com") {
				t.Errorf("%s contains the email %s; fixtures use example.com addresses only", path, m)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
