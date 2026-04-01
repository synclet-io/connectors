package main

import (
	"fmt"
	"strings"
	"unicode"
)

// parseHeaders reads the header row values and returns column names.
// Stops at the first empty or non-string cell per D-15.
// Deduplicates by appending _<column letter> to all occurrences of duplicated names.
func parseHeaders(row []interface{}) ([]string, error) {
	// First pass: collect raw header strings, stop at first empty
	raw := make([]string, 0, len(row))
	for _, val := range row {
		s, ok := val.(string)
		if !ok || s == "" {
			break
		}

		raw = append(raw, s)
	}

	if len(raw) == 0 {
		return nil, fmt.Errorf("no headers found in header row")
	}

	// Count occurrences to detect duplicates
	counts := make(map[string]int)
	for _, h := range raw {
		counts[h]++
	}

	// Build result: append column letter for duplicated names
	result := make([]string, len(raw))
	for i, h := range raw {
		if counts[h] > 1 {
			result[i] = h + "_" + columnLetter(i)
		} else {
			result[i] = h
		}
	}

	return result, nil
}

// columnLetter converts a 0-based column index to an Excel-style column letter.
// 0 -> A, 1 -> B, ..., 25 -> Z, 26 -> AA, 27 -> AB, etc.
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

// toSnakeCase converts a string to snake_case.
// Handles camelCase, PascalCase, spaces, hyphens.
func toSnakeCase(s string) string {
	var result strings.Builder
	runes := []rune(s)

	for i, r := range runes {
		if r == ' ' || r == '-' {
			result.WriteRune('_')
			continue
		}

		if unicode.IsUpper(r) {
			// Insert underscore before uppercase if:
			// - not first character
			// - previous char is lowercase, OR
			// - next char is lowercase (handles "XMLParser" -> "xml_parser")
			if i > 0 {
				prev := runes[i-1]
				if prev != ' ' && prev != '-' && prev != '_' {
					if unicode.IsLower(prev) {
						result.WriteRune('_')
					} else if i+1 < len(runes) && unicode.IsLower(runes[i+1]) {
						result.WriteRune('_')
					}
				}
			}
			result.WriteRune(unicode.ToLower(r))
		} else {
			result.WriteRune(unicode.ToLower(r))
		}
	}

	return result.String()
}
