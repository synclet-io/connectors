package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode/utf8"
)

// maxIdentLen is Redshift's identifier limit in bytes.
const maxIdentLen = 127

// hashedPrefixLen is the part of a long name kept before "_" and 8 hex
// digits of its hash.
const hashedPrefixLen = maxIdentLen - 1 - 8

// rsIdent maps a stream or field name to a Redshift identifier. Redshift
// folds ASCII letters of identifiers to lower case, quoted or not, so the
// name is folded the same way. A result of at most 127 bytes is kept; a
// longer one becomes its first 118 bytes, cut back to a UTF-8 character
// boundary, "_" and the first 8 hex digits of the SHA-256 of the full name.
func rsIdent(name string) string {
	name = lowerASCII(name)
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

func lowerASCII(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'A' && r <= 'Z' {
			return r + ('a' - 'A')
		}
		return r
	}, s)
}

// quote renders a possibly schema-qualified identifier, double-quoted.
func quote(parts ...string) string {
	q := make([]string, len(parts))
	for i, p := range parts {
		q[i] = `"` + strings.ReplaceAll(p, `"`, `""`) + `"`
	}
	return strings.Join(q, ".")
}

// literal renders s as a Redshift string literal. Redshift reads a
// backslash in a literal as an escape, so it is doubled like the quote.
func literal(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(s) + "'"
}

// checkIdentName fails for a name Redshift cannot hold as an identifier.
func checkIdentName(kind, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%s name is empty", kind)
	case !utf8.ValidString(name):
		return fmt.Errorf("%s name %q is not valid UTF-8", kind, name)
	case strings.ContainsFunc(name, isControl):
		return fmt.Errorf("%s name %q contains a control character", kind, name)
	case len(name) > maxIdentLen:
		return fmt.Errorf("%s name %q is longer than %d bytes", kind, name, maxIdentLen)
	}
	return nil
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
	if err := checkIdentName(n.kind, ident); err != nil {
		return err
	}
	if prev, ok := n.used[ident]; ok {
		return fmt.Errorf("%s names %q and %q both map to %q", n.kind, prev, original, ident)
	}
	n.used[ident] = original
	return nil
}
