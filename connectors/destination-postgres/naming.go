package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
)

// maxIdentLen is Postgres' identifier limit (NAMEDATALEN - 1); longer names
// would be truncated by the server.
const maxIdentLen = 63

// hashedPrefixLen is the part of a long name kept before "_" and 8 hex
// digits of its hash.
const hashedPrefixLen = maxIdentLen - 1 - 8

// pgIdent maps a stream or field name to a Postgres identifier. A name of at
// most 63 bytes is kept as given; a longer one becomes its first 54 bytes,
// cut back to a UTF-8 character boundary, "_" and the first 8 hex digits of
// the SHA-256 of the full name. The result depends on the name alone.
func pgIdent(name string) string {
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

// quote renders a possibly schema-qualified identifier, double-quoted.
func quote(parts ...string) string {
	return pgx.Identifier(parts).Sanitize()
}

// nameSet tracks the identifiers in one namespace (the tables of a schema,
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
	}
	if prev, ok := n.used[ident]; ok {
		return fmt.Errorf("%s names %q and %q both map to %q", n.kind, prev, original, ident)
	}
	n.used[ident] = original
	return nil
}
