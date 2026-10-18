package main

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdent(t *testing.T) {
	t.Run("64 characters kept verbatim", func(t *testing.T) {
		name := strings.Repeat("é", 64)
		assert.Equal(t, name, myIdent(name))
	})
	t.Run("case and punctuation kept", func(t *testing.T) {
		assert.Equal(t, "Order Items-2024.`x`", myIdent("Order Items-2024.`x`"))
	})
	t.Run("65 characters shortened", func(t *testing.T) {
		name := strings.Repeat("a", 65)
		got := myIdent(name)
		assert.Len(t, got, 64)
		assert.Equal(t, strings.Repeat("a", 55)+"_", got[:56])
		assert.Regexp(t, `^[0-9a-f]{8}$`, got[56:])
	})
	t.Run("multibyte names counted in characters", func(t *testing.T) {
		name := strings.Repeat("é", 70)
		got := myIdent(name)
		assert.True(t, utf8.ValidString(got))
		assert.Equal(t, 64, utf8.RuneCountInString(got))
		assert.Equal(t, strings.Repeat("é", 55)+"_", string([]rune(got)[:56]))
	})
	t.Run("names sharing a prefix stay unique", func(t *testing.T) {
		prefix := strings.Repeat("p", 55)
		a, b := myIdent(prefix+"_first_name"), myIdent(prefix+"_other_name")
		assert.Len(t, a, 64)
		assert.Len(t, b, 64)
		assert.NotEqual(t, a, b)
	})
	t.Run("stable", func(t *testing.T) {
		name := strings.Repeat("stable_", 20)
		assert.Equal(t, myIdent(name), myIdent(name))
		assert.Equal(t, "stable_stable_stable_stable_stable_stable_stable_stable_", myIdent(name)[:56])
	})
	t.Run("quote doubles backticks", func(t *testing.T) {
		assert.Equal(t, "`a``b`", quote("a`b"))
	})
	t.Run("collision fails naming both, ignoring case", func(t *testing.T) {
		n := newNameSet("column")
		require.NoError(t, n.claim("Name", "Name"))
		err := n.claim("name", "name")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"Name"`)
		assert.Contains(t, err.Error(), `"name"`)
	})
	t.Run("names MySQL cannot hold fail", func(t *testing.T) {
		n := newNameSet("column")
		for _, bad := range []string{"", "a\x00b", "trailing ", "emoji \U0001F600", "bad \xff"} {
			require.Error(t, n.claim(bad, bad), "%q", bad)
		}
	})
}
