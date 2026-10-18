package main

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// header is the column names of a sheet's first row, by column: names[i]
// is the name of column i. Columns past len(names) have no name.
type header struct {
	names []string
	// dropped is, in legacy mode, the number of non-empty header cells after
	// the first empty one, which get no name.
	dropped int
}

var errNoHeader = errors.New("no headers found in header row")

// columnNames names the columns of a header row.
//
// legacy: the header stops at the first empty or non-string cell; each
// duplicated name gets _<column letter>, then names_conversion applies, and
// two columns that end with the same name are an error.
//
// stable: every column up to the last non-empty header cell gets a name; an
// empty or non-string cell is column_<column letter>; names_conversion
// applies before duplicates get _<column letter>.
func columnNames(row []any, mode string, convert bool) (header, error) {
	if mode == columnNamingStable {
		return stableNames(row, convert)
	}
	return legacyNames(row, convert)
}

func legacyNames(row []any, convert bool) (header, error) {
	var h header
	raw := make([]string, 0, len(row))
	for i, v := range row {
		s, ok := v.(string)
		if !ok || s == "" {
			for _, rest := range row[i+1:] {
				if s, ok := rest.(string); ok && s != "" {
					h.dropped++
				}
			}
			break
		}
		raw = append(raw, s)
	}
	if len(raw) == 0 {
		return h, errNoHeader
	}
	counts := map[string]int{}
	for _, n := range raw {
		counts[n]++
	}
	h.names = make([]string, len(raw))
	owner := map[string]int{}
	for i, n := range raw {
		name := n
		if counts[n] > 1 {
			name += "_" + columnLetter(i)
		}
		if convert {
			name = toSnakeCase(name)
		}
		if j, ok := owner[name]; ok {
			return h, fmt.Errorf("header columns %s (%q) and %s (%q) both become column %q: set column_naming: stable to name them apart",
				columnLetter(j), raw[j], columnLetter(i), raw[i], name)
		}
		owner[name] = i
		h.names[i] = name
	}
	return h, nil
}

func stableNames(row []any, convert bool) (header, error) {
	var h header
	last := -1
	for i, v := range row {
		if headerText(v) != "" {
			last = i
		}
	}
	if last < 0 {
		return h, errNoHeader
	}
	h.names = make([]string, last+1)
	for i := range h.names {
		s := headerText(row[i])
		switch {
		case s == "":
			h.names[i] = "column_" + columnLetter(i)
		case convert:
			h.names[i] = toSnakeCase(s)
		default:
			h.names[i] = s
		}
	}
	counts := map[string]int{}
	for _, n := range h.names {
		counts[n]++
	}
	taken := map[string]bool{}
	for _, n := range h.names {
		if counts[n] == 1 {
			taken[n] = true
		}
	}
	for i, n := range h.names {
		if counts[n] == 1 {
			continue
		}
		name := n + "_" + columnLetter(i)
		for taken[name] {
			name += "_" + columnLetter(i)
		}
		taken[name] = true
		h.names[i] = name
	}
	return h, nil
}

// headerText is the name a header cell gives in stable mode: its string,
// or "" for an empty, blank or non-string cell.
func headerText(v any) string {
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return ""
	}
	return s
}

// columnLetter converts a 0-based column index to an A1 column letter:
// 0 is A, 25 is Z, 26 is AA.
func columnLetter(index int) string {
	result := ""
	for {
		result = string(rune('A'+index%26)) + result
		index = index/26 - 1
		if index < 0 {
			break
		}
	}
	return result
}

// toSnakeCase converts camelCase, PascalCase, spaces and hyphens to
// snake_case; letters without case, such as CJK, stay as they are.
func toSnakeCase(s string) string {
	var result strings.Builder
	runes := []rune(s)
	for i, r := range runes {
		if r == ' ' || r == '-' {
			result.WriteRune('_')
			continue
		}
		if unicode.IsUpper(r) && i > 0 {
			prev := runes[i-1]
			if prev != ' ' && prev != '-' && prev != '_' {
				if unicode.IsLower(prev) || i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
					result.WriteRune('_')
				}
			}
		}
		result.WriteRune(unicode.ToLower(r))
	}
	return result.String()
}
