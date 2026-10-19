package main

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// maxIdentLen is Snowflake's limit on an identifier, in characters.
const maxIdentLen = 255

// ident quotes each part of a Snowflake object name and joins them with dots.
func ident(parts ...string) string {
	q := make([]string, len(parts))
	for i, p := range parts {
		q[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}
	return strings.Join(q, ".")
}

// sfName is the identifier a stream or property name lands under: the name
// in upper case, so it also resolves when written unquoted in SQL.
func sfName(what, name string) (string, error) {
	if name == "" {
		return "", fmt.Errorf("empty %s name", what)
	}
	up := strings.ToUpper(name)
	if n := utf8.RuneCountInString(up); n > maxIdentLen {
		return "", fmt.Errorf("%s name %q is longer than %d characters", what, name, maxIdentLen)
	}
	return up, nil
}
