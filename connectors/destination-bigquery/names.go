package main

import (
	"sort"
	"strconv"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// Names follow Airbyte's BigQuery destination (BigQuerySQLNameTransformer and
// Transformations.toAlphanumericAndUnderscore), so a stream lands in the table
// Airbyte would write it to.

// alphanumericAndUnderscore decomposes s (NFKD), drops combining marks, turns
// each run of ASCII whitespace into "_" and every other character that is not
// an ASCII letter, digit or "_" into "_": "№ of items" becomes
// "No_of_items", "Price (USD)" becomes "Price__USD_".
func alphanumericAndUnderscore(s string) string {
	var b strings.Builder
	inSpace := false
	for _, r := range norm.NFKD.String(s) {
		if unicode.Is(unicode.M, r) {
			continue
		}
		if isASCIISpace(r) {
			if !inSpace {
				b.WriteByte('_')
			}
			inSpace = true
			continue
		}
		inSpace = false
		if isASCIIAlnum(r) || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// isASCIISpace matches Java's \s: space, tab, newline, vertical tab, form feed
// and carriage return.
func isASCIISpace(r rune) bool {
	switch r {
	case ' ', '\t', '\n', '\v', '\f', '\r':
		return true
	}
	return false
}

func isASCIIAlnum(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
}

func isASCIILetter(r byte) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
}

// tableOrColumnName is the BigQuery name of a stream's table or of a field's
// column: alphanumericAndUnderscore, prefixed with "_" when it does not start
// with a letter or "_".
func tableOrColumnName(s string) string {
	n := alphanumericAndUnderscore(s)
	if n == "" || !isASCIILetter(n[0]) && n[0] != '_' {
		return "_" + n
	}
	return n
}

// datasetName is the BigQuery dataset of a namespace: alphanumericAndUnderscore,
// prefixed with "n" when it does not start with a letter or digit, since a
// dataset starting with "_" is hidden.
func datasetName(s string) string {
	n := alphanumericAndUnderscore(s)
	if n == "" || !isASCIILetter(n[0]) && (n[0] < '0' || n[0] > '9') {
		return "n" + n
	}
	return n
}

// columnNames maps each property name to its column. BigQuery column names
// are case-insensitive, so two properties whose columns differ only in case
// collide too. A colliding property takes the first free name of
// "<property>_1", "<property>_2", ... as Airbyte does; properties are taken in
// name order, so the first name keeps the plain column.
func columnNames(props []string) map[string]string {
	sorted := append([]string(nil), props...)
	sort.Strings(sorted)
	taken := map[string]bool{}
	out := make(map[string]string, len(sorted))
	for _, p := range sorted {
		col := tableOrColumnName(p)
		for i := 1; taken[strings.ToLower(col)]; i++ {
			col = tableOrColumnName(p + "_" + strconv.Itoa(i))
		}
		taken[strings.ToLower(col)] = true
		out[p] = col
	}
	return out
}
