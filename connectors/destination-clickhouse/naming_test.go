package main

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdent(t *testing.T) {
	t.Run("64 bytes kept verbatim", func(t *testing.T) {
		name := strings.Repeat("A", 64)
		assert.Equal(t, name, chIdent(name))
	})
	t.Run("case and punctuation kept", func(t *testing.T) {
		assert.Equal(t, "Order Items-2024.`x`", chIdent("Order Items-2024.`x`"))
	})
	t.Run("65 bytes shortened", func(t *testing.T) {
		name := strings.Repeat("a", 65)
		got := chIdent(name)
		assert.Len(t, got, 64)
		assert.Equal(t, strings.Repeat("a", 55)+"_", got[:56])
		assert.Regexp(t, `^[0-9a-f]{8}$`, got[56:])
	})
	t.Run("multibyte cut on a rune boundary", func(t *testing.T) {
		name := strings.Repeat("a", 54) + strings.Repeat("é", 10)
		got := chIdent(name)
		assert.True(t, utf8.ValidString(got))
		assert.LessOrEqual(t, len(got), 64)
		assert.Equal(t, strings.Repeat("a", 54)+"_", got[:55])
	})
	t.Run("names sharing a prefix stay unique", func(t *testing.T) {
		prefix := strings.Repeat("p", 60)
		a, b := chIdent(prefix+"_first_name"), chIdent(prefix+"_other_name")
		assert.Len(t, a, 64)
		assert.NotEqual(t, a, b)
	})
	t.Run("quote escapes backquotes and backslashes", func(t *testing.T) {
		assert.Equal(t, "`db`.`a\\`b\\\\c`", quote("db", "a`b\\c"))
	})
	t.Run("collision fails naming both", func(t *testing.T) {
		n := newNameSet("table")
		require.NoError(t, n.claim("users", "users"))
		err := n.claim("users", "Users2")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"users"`)
		assert.Contains(t, err.Error(), `"Users2"`)
	})
	t.Run("empty, NUL and invalid UTF-8 names fail", func(t *testing.T) {
		n := newNameSet("column")
		require.Error(t, n.claim("", ""))
		require.Error(t, n.claim("a\x00b", "a\x00b"))
		require.Error(t, n.claim("a\xffb", "a\xffb"))
	})
}
