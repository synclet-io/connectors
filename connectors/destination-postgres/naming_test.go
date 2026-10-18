package main

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdent(t *testing.T) {
	t.Run("63 bytes kept verbatim", func(t *testing.T) {
		name := strings.Repeat("A", 63)
		assert.Equal(t, name, pgIdent(name))
	})
	t.Run("case and punctuation kept", func(t *testing.T) {
		assert.Equal(t, `Order Items-2024."x"`, pgIdent(`Order Items-2024."x"`))
	})
	t.Run("64 bytes shortened", func(t *testing.T) {
		name := strings.Repeat("a", 64)
		got := pgIdent(name)
		assert.Len(t, got, 63)
		assert.Equal(t, strings.Repeat("a", 54)+"_", got[:55])
		assert.Regexp(t, `^[0-9a-f]{8}$`, got[55:])
	})
	t.Run("multibyte cut on a rune boundary", func(t *testing.T) {
		name := strings.Repeat("a", 53) + strings.Repeat("é", 10)
		got := pgIdent(name)
		assert.True(t, utf8.ValidString(got))
		assert.LessOrEqual(t, len(got), 63)
		assert.Equal(t, strings.Repeat("a", 53)+"_", got[:54])
	})
	t.Run("names sharing a prefix stay unique", func(t *testing.T) {
		prefix := strings.Repeat("p", 60)
		a, b := pgIdent(prefix+"_first_name"), pgIdent(prefix+"_other_name")
		assert.Len(t, a, 63)
		assert.Len(t, b, 63)
		assert.NotEqual(t, a, b)
	})
	t.Run("stable", func(t *testing.T) {
		name := strings.Repeat("stable_", 20)
		assert.Equal(t, pgIdent(name), pgIdent(name))
		assert.Equal(t, "stable_stable_stable_stable_stable_stable_stable_stabl_", pgIdent(name)[:55])
	})
	t.Run("collision fails naming both", func(t *testing.T) {
		n := newNameSet("table")
		require.NoError(t, n.claim("users", "users"))
		err := n.claim("users", "Users2")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"users"`)
		assert.Contains(t, err.Error(), `"Users2"`)
	})
	t.Run("empty and NUL names fail", func(t *testing.T) {
		n := newNameSet("column")
		require.Error(t, n.claim("", ""))
		require.Error(t, n.claim("a\x00b", "a\x00b"))
	})
}
