package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func row(cells ...any) []any { return cells }

func TestColumnNames(t *testing.T) {
	tests := []struct {
		name    string
		header  []any
		mode    string
		convert bool
		want    []string
		dropped int
		err     string
	}{
		{name: "legacy duplicates get their column letter", header: row("Name", "Name", "Age"), mode: columnNamingLegacy, want: []string{"Name_A", "Name_B", "Age"}},
		{name: "legacy stops at the first empty cell", header: row("Name", "", "Age"), mode: columnNamingLegacy, want: []string{"Name"}, dropped: 1},
		{name: "stable names an empty cell by its column", header: row("Name", "", "Age"), mode: columnNamingStable, want: []string{"Name", "column_B", "Age"}},
		{name: "legacy keeps non-ASCII names", header: row("Prénom", "名前"), mode: columnNamingLegacy, convert: true, want: []string{"prénom", "名前"}},
		{name: "stable keeps non-ASCII names", header: row("Prénom", "名前", "Ünïcödé Name"), mode: columnNamingStable, convert: true, want: []string{"prénom", "名前", "ünïcödé_name"}},
		{
			name: "legacy collision after conversion fails", header: row("First Name", "first_name"), mode: columnNamingLegacy, convert: true,
			err: `header columns A ("First Name") and B ("first_name") both become column "first_name": set column_naming: stable to name them apart`,
		},
		{name: "stable dedupes after conversion", header: row("First Name", "first_name"), mode: columnNamingStable, convert: true, want: []string{"first_name_A", "first_name_B"}},
		{name: "legacy without conversion", header: row("First Name", "first_name"), mode: columnNamingLegacy, want: []string{"First Name", "first_name"}},
		{name: "legacy duplicates then conversion", header: row("Name", "Name"), mode: columnNamingLegacy, convert: true, want: []string{"name_a", "name_b"}},
		{name: "stable stops at the last non-empty cell", header: row("A", "", "B", "", " "), mode: columnNamingStable, want: []string{"A", "column_B", "B"}},
		{name: "stable names a non-string cell by its column", header: row("A", json.Number("2026"), "C"), mode: columnNamingStable, want: []string{"A", "column_B", "C"}},
		{name: "stable suffix never collides with a header", header: row("x", "x", "x_A"), mode: columnNamingStable, want: []string{"x_A_A", "x_B", "x_A"}},
		{name: "stable placeholder colliding with a header", header: row("column_B", "", "x"), mode: columnNamingStable, want: []string{"column_B_A", "column_B_B", "x"}},
		{name: "legacy empty header", header: row("", "Age"), mode: columnNamingLegacy, err: "no headers found in header row", dropped: 1},
		{name: "stable empty header", header: row("", " "), mode: columnNamingStable, err: "no headers found in header row"},
		{name: "past column Z", header: append(make([]any, 27), "AB header"), mode: columnNamingStable, want: append(letters(27), "AB header")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for range 2 {
				h, err := columnNames(tt.header, tt.mode, tt.convert)
				if tt.err != "" {
					require.EqualError(t, err, tt.err)
					return
				}
				require.NoError(t, err)
				assert.Equal(t, tt.want, h.names)
				assert.Equal(t, tt.dropped, h.dropped)
			}
		})
	}
}

func letters(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "column_" + columnLetter(i)
	}
	return out
}

func TestColumnNamesDiscover(t *testing.T) {
	header := func(cells ...string) fixture {
		raw, err := json.Marshal(map[string]any{"valueRanges": []any{
			map[string]any{"values": [][]string{cells}}, map[string]any{}, map[string]any{}, map[string]any{}, map[string]any{}, map[string]any{},
		}})
		if err != nil {
			t.Fatal(err)
		}
		return fixture{status: http.StatusOK, body: raw}
	}
	t.Run("legacy collision fails discover", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string][]fixture{batchGetKey(testSheetA): {header("First Name", "first_name")}})
		src, _ := testSource(srv)
		out := runDiscover(t, src, testConfig(t, func(c *Config) { c.NamesConversion = true }))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `sheet "People": header columns A ("First Name") and B ("first_name") both become column "first_name": set column_naming: stable to name them apart`)
	})
	t.Run("stable", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string][]fixture{batchGetKey(testSheetA): {header("First Name", "first_name", "", "Città")}})
		src, _ := testSource(srv)
		out := runDiscover(t, src, testConfig(t, func(c *Config) { c.NamesConversion = true; withStable(c) }))
		require.NoError(t, out.Err)
		var schema struct {
			Properties map[string]any `json:"properties"`
		}
		require.NoError(t, json.Unmarshal(out.Catalog().Streams[0].JSONSchema, &schema))
		assert.ElementsMatch(t, []string{"first_name_A", "first_name_B", "column_C", "città"}, keys(schema.Properties))
	})
	t.Run("cursor column name fails incremental discover", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string][]fixture{batchGetKey(testSheetA): {header("Name", cursorColumn)}})
		src, _ := testSource(srv)
		out := runDiscover(t, src, testConfig(t, withIncremental))
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), `sheet "People" has a column named _spreadsheet_modified_time, the cursor column incremental: true adds: rename the column`)
	})
	t.Run("cursor column name is a plain column without incremental", func(t *testing.T) {
		srv, _ := fixtureServer(t, map[string][]fixture{batchGetKey(testSheetA): {header("Name", cursorColumn)}})
		src, _ := testSource(srv)
		out := runDiscover(t, src, testConfig(t, nil))
		require.NoError(t, out.Err)
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, out.Catalog().Streams[0].SupportedSyncModes)
	})
}

func keys(m map[string]any) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestToSnakeCase(t *testing.T) {
	for in, want := range map[string]string{
		"First Name":      "first_name",
		"firstName":       "first_name",
		"Already_Snake":   "already_snake",
		"PascalCase":      "pascal_case",
		"some-kebab-case": "some_kebab_case",
		"ALLCAPS":         "allcaps",
		"XMLParser":       "xml_parser",
		"Prénom":          "prénom",
		"名前":              "名前",
	} {
		assert.Equal(t, want, toSnakeCase(in), in)
	}
}

func TestColumnLetter(t *testing.T) {
	for i, want := range map[int]string{0: "A", 1: "B", 25: "Z", 26: "AA", 27: "AB", 51: "AZ", 52: "BA", 701: "ZZ", 702: "AAA"} {
		assert.Equal(t, want, columnLetter(i))
	}
}

// headerRanges is a values:batchGet body whose first range holds header and
// whose other ranges, up to n in all, are empty.
func headerRanges(t *testing.T, n int, header ...any) fixture {
	t.Helper()
	ranges := []any{map[string]any{"values": [][]any{header}}}
	for len(ranges) < n {
		ranges = append(ranges, map[string]any{})
	}
	raw, err := json.Marshal(map[string]any{"valueRanges": ranges})
	require.NoError(t, err)
	return fixture{status: http.StatusOK, body: raw}
}

func TestColumnNamesMoved(t *testing.T) {
	tests := []struct {
		name          string
		before, after []any
		mode          string
		err           string
		warn          string
	}{
		{
			name: "stable duplicate inserted in front", mode: columnNamingStable,
			before: row("Name", "Name"), after: row("Name", "Name", "Name"),
			err: `column C of sheet "S" is named "Name_C" by its position and is not in the configured schema: columns named by position have moved, so their values would land under other columns; refresh the source schema`,
		},
		{
			name: "stable column inserted among empty headers", mode: columnNamingStable,
			before: row("id", "", "", "x"), after: row("id", "", "", "", "x"),
			err: `column D of sheet "S" is named "column_D" by its position`,
		},
		{
			name: "stable column removed among empty headers", mode: columnNamingStable,
			before: row("id", "", "", "x"), after: row("id", "", "x"),
			err: `column "column_C" is no longer in the header of sheet "S"; refresh the source schema`,
		},
		{
			name: "stable named column added", mode: columnNamingStable,
			before: row("id", "", "x"), after: row("id", "", "x", "y"),
		},
		{
			name: "legacy duplicate inserted warns", mode: columnNamingLegacy,
			before: row("Name", "Name"), after: row("Name", "Name", "Name"),
			warn: `sheet "S": column C is named "Name_C" by its position and is not in the configured schema; duplicate columns may have moved, so their values may land under other columns: refresh the source schema (column_naming: stable fails instead)`,
		},
		{
			name: "legacy unchanged duplicates", mode: columnNamingLegacy,
			before: row("Name", "Name", "Age"), after: row("Name", "Name", "Age", "Email"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before, err := columnNames(tt.before, tt.mode, false)
			require.NoError(t, err)
			after, err := columnNames(tt.after, tt.mode, false)
			require.NoError(t, err)
			var logs bytes.Buffer
			log := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey || a.Key == slog.LevelKey {
					return slog.Attr{}
				}
				return a
			}}))
			err = checkColumns(log, tt.mode, before.names, sheet{props: sheetProperties{Title: "S"}, header: after})
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			} else {
				require.NoError(t, err)
			}
			if tt.warn != "" {
				assert.Contains(t, logs.String(), strings.ReplaceAll(strings.ReplaceAll(tt.warn, `\`, `\\`), `"`, `\"`))
			} else {
				assert.Empty(t, logs.String())
			}
		})
	}
}

func TestReadColumnsMoved(t *testing.T) {
	for _, tt := range []struct {
		mode string
		edit func(*Config)
	}{{mode: columnNamingStable, edit: withStable}, {mode: columnNamingLegacy}} {
		t.Run(tt.mode, func(t *testing.T) {
			discoverSrv, _ := fixtureServer(t, map[string][]fixture{batchGetKey(testSheetA): {headerRanges(t, 6, "Full Name", "Full Name")}})
			discoverSrc, _ := testSource(discoverSrv)
			cfg := testConfig(t, tt.edit)
			cat := configured(t, discoverSrc, cfg, synclet.SyncModeFullRefresh, "People")

			srv, _ := fixtureServer(t, map[string][]fixture{batchGetKey(testSheetA): {headerRanges(t, 1, "Full Name", "Full Name", "Full Name"), {}}})
			src, _ := testSource(srv)
			out := runRead(t, src, cfg, cat, nil)
			if tt.mode == columnNamingStable {
				require.Error(t, out.Err)
				assert.Contains(t, out.Err.Error(), `column C of sheet "People" is named "Full Name_C" by its position`)
				assert.Empty(t, out.Records(stream("People")))
				return
			}
			require.NoError(t, out.Err)
			assert.Len(t, out.Records(stream("People")), 4, "legacy keeps reading as before")
			assert.Contains(t, warnings(out), `sheet "People": column C is named "Full Name_C" by its position and is not in the configured schema; duplicate columns may have moved, so their values may land under other columns: refresh the source schema (column_naming: stable fails instead)`)
		})
	}
}
