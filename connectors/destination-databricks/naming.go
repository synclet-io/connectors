package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
)

// maxNameLen is Unity Catalog's limit on table names.
const maxNameLen = 255

// tableName maps a stream name to a Unity Catalog table name: lower case,
// every character outside [a-z0-9_] replaced with "_". A name longer than 255
// bytes keeps its first 246 bytes, "_" and the first 8 hex digits of the
// SHA-256 of the full name.
func tableName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) <= maxNameLen {
		return out
	}
	sum := sha256.Sum256([]byte(name))
	return out[:maxNameLen-9] + "_" + hex.EncodeToString(sum[:4])
}

// ident renders a backquoted identifier.
func ident(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// sqlString renders a string literal.
func sqlString(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// nameSet tracks the names in one namespace (the tables of a schema, the
// columns of a table), which Databricks compares without case, and the
// names they came from.
type nameSet struct {
	kind string
	used map[string]string
}

func newNameSet(kind string) *nameSet {
	return &nameSet{kind: kind, used: map[string]string{}}
}

// claim records that original maps to name. It fails when name is empty or
// holds a NUL, or another name already maps to it ignoring case.
func (n *nameSet) claim(name, original string) error {
	switch {
	case name == "":
		return fmt.Errorf("%s name is empty", n.kind)
	case strings.ContainsRune(name, 0):
		return fmt.Errorf("%s name %q contains a NUL character", n.kind, original)
	}
	key := strings.ToLower(name)
	if prev, ok := n.used[key]; ok {
		return fmt.Errorf("%s names %q and %q both map to %q (Databricks ignores case)", n.kind, prev, original, key)
	}
	n.used[key] = original
	return nil
}
