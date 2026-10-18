package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

// maxIdentLen is MySQL's limit for table and column names, in characters.
const maxIdentLen = 64

// hashedPrefixLen is the part of a long name kept before "_" and 8 hex
// digits of its hash.
const hashedPrefixLen = maxIdentLen - 1 - 8

// myIdent maps a stream or field name to a MySQL identifier. A name of at
// most 64 characters is kept as given; a longer one becomes its first 55
// characters, "_" and the first 8 hex digits of the SHA-256 of the full name.
// The result depends on the name alone.
func myIdent(name string) string {
	if utf8.RuneCountInString(name) <= maxIdentLen {
		return name
	}
	cut, n := 0, 0
	for cut < len(name) && n < hashedPrefixLen {
		_, size := utf8.DecodeRuneInString(name[cut:])
		cut += size
		n++
	}
	sum := sha256.Sum256([]byte(name))
	return name[:cut] + "_" + hex.EncodeToString(sum[:4])
}

// quote renders an identifier in backticks.
func quote(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// nameSet tracks the identifiers in one namespace (the tables of a
// database, the columns of a table) and the names they came from. MySQL
// compares column names, and on some platforms table names, without regard
// to case, so two names differing only in case collide.
type nameSet struct {
	kind string
	used map[string]string
}

func newNameSet(kind string) *nameSet {
	return &nameSet{kind: kind, used: map[string]string{}}
}

// claim records that original maps to ident. It fails when ident is not a
// valid MySQL identifier or another name already maps to it.
func (n *nameSet) claim(ident, original string) error {
	switch {
	case ident == "":
		return fmt.Errorf("%s name is empty", n.kind)
	case !utf8.ValidString(ident):
		return fmt.Errorf("%s name %q is not valid UTF-8", n.kind, original)
	case strings.ContainsRune(ident, 0):
		return fmt.Errorf("%s name %q contains a NUL character", n.kind, original)
	case strings.HasSuffix(ident, " "):
		return fmt.Errorf("%s name %q ends with a space, which MySQL does not allow", n.kind, original)
	}
	for _, r := range ident {
		if r > 0xFFFF {
			return fmt.Errorf("%s name %q has a character outside the Basic Multilingual Plane, which MySQL names cannot hold", n.kind, original)
		}
	}
	key := strings.ToLower(ident)
	if prev, ok := n.used[key]; ok {
		return fmt.Errorf("%s names %q and %q both map to %q (MySQL names ignore case)", n.kind, prev, original, ident)
	}
	n.used[key] = original
	return nil
}
