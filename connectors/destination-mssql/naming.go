package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// maxIdentLen is SQL Server's limit for schema, table and column names, in
// UTF-16 code units (sysname is NVARCHAR(128)).
const maxIdentLen = 128

// hashedPrefixLen is the part of a long name kept before "_" and 8 hex
// digits of its hash.
const hashedPrefixLen = maxIdentLen - 1 - 8

// identLen is the length of s in UTF-16 code units, as SQL Server counts it.
func identLen(s string) int {
	n := 0
	for _, r := range s {
		n += utf16.RuneLen(r)
	}
	return n
}

// msIdent maps a stream or field name to a SQL Server identifier. A name of
// at most 128 UTF-16 code units is kept as given; a longer one becomes its
// first 119 code units (cut back to a whole character), "_" and the first 8
// hex digits of the SHA-256 of the full name. The result depends on the name
// alone.
func msIdent(name string) string {
	if identLen(name) <= maxIdentLen {
		return name
	}
	cut, n := 0, 0
	for cut < len(name) {
		r, size := utf8.DecodeRuneInString(name[cut:])
		if n+utf16.RuneLen(r) > hashedPrefixLen {
			break
		}
		n += utf16.RuneLen(r)
		cut += size
	}
	sum := sha256.Sum256([]byte(name))
	return name[:cut] + "_" + hex.EncodeToString(sum[:4])
}

// quote renders an identifier in brackets.
func quote(ident string) string {
	return "[" + strings.ReplaceAll(ident, "]", "]]") + "]"
}

// checkName fails when ident cannot be a SQL Server name.
func checkName(kind, ident string) error {
	switch {
	case ident == "":
		return fmt.Errorf("%s name is empty", kind)
	case !utf8.ValidString(ident):
		return fmt.Errorf("%s name %q is not valid UTF-8", kind, ident)
	case strings.ContainsRune(ident, 0):
		return fmt.Errorf("%s name %q contains a NUL character", kind, ident)
	case strings.HasSuffix(ident, " "):
		return fmt.Errorf("%s name %q ends with a space, which SQL Server ignores when it compares names", kind, ident)
	}
	return nil
}

// nameSet tracks the identifiers in one namespace (the tables of a schema,
// the columns of a table) and the names they came from. SQL Server compares
// names by the collation of the database, which usually ignores case, so two
// names differing only in case collide.
type nameSet struct {
	kind string
	used map[string]string
}

func newNameSet(kind string) *nameSet {
	return &nameSet{kind: kind, used: map[string]string{}}
}

// claim records that original maps to ident. It fails when ident is not a
// valid SQL Server identifier or another name already maps to it.
func (n *nameSet) claim(ident, original string) error {
	if err := checkName(n.kind, ident); err != nil {
		return err
	}
	key := strings.ToLower(ident)
	if prev, ok := n.used[key]; ok {
		return fmt.Errorf("%s names %q and %q both map to %q (SQL Server names usually ignore case)", n.kind, prev, original, ident)
	}
	n.used[key] = original
	return nil
}
