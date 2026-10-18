package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

// maxIdentLen bounds table and column names. ClickHouse stores both in file
// names, escaping every byte outside [A-Za-z0-9_] as three, and file names
// stop at 255 bytes; 64 bytes stay under that escaped, with room for the
// suffixes ClickHouse adds.
const maxIdentLen = 64

// hashedPrefixLen is the part of a long name kept before "_" and 8 hex
// digits of its hash.
const hashedPrefixLen = maxIdentLen - 1 - 8

// chIdent maps a stream or field name to a ClickHouse identifier. A name of
// at most 64 bytes is kept as given; a longer one becomes its first 55
// bytes, cut back to a UTF-8 character boundary, "_" and the first 8 hex
// digits of the SHA-256 of the full name. The result depends on the name
// alone.
func chIdent(name string) string {
	if len(name) <= maxIdentLen {
		return name
	}
	cut := hashedPrefixLen
	for cut > 0 && !utf8.RuneStart(name[cut]) {
		cut--
	}
	sum := sha256.Sum256([]byte(name))
	return name[:cut] + "_" + hex.EncodeToString(sum[:4])
}

var backquoter = strings.NewReplacer("\\", "\\\\", "`", "\\`")

// quote renders a possibly database-qualified identifier, each part in
// backquotes.
func quote(parts ...string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = "`" + backquoter.Replace(p) + "`"
	}
	return strings.Join(quoted, ".")
}

// nameSet tracks the identifiers in one namespace (the tables of a database,
// the columns of a table) and the names they came from.
type nameSet struct {
	kind string
	used map[string]string
}

func newNameSet(kind string) *nameSet {
	return &nameSet{kind: kind, used: map[string]string{}}
}

// claim records that original maps to ident. It fails when ident is not a
// valid identifier or another name already maps to it.
func (n *nameSet) claim(ident, original string) error {
	switch {
	case ident == "":
		return fmt.Errorf("%s name is empty", n.kind)
	case strings.ContainsRune(ident, 0):
		return fmt.Errorf("%s name %q contains a NUL character", n.kind, original)
	case !utf8.ValidString(ident):
		return fmt.Errorf("%s name %q is not valid UTF-8", n.kind, original)
	}
	if prev, ok := n.used[ident]; ok {
		return fmt.Errorf("%s names %q and %q both map to %q", n.kind, prev, original, ident)
	}
	n.used[ident] = original
	return nil
}
