package main

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdent(t *testing.T) {
	t.Run("128 code units kept verbatim", func(t *testing.T) {
		name := strings.Repeat("é", 128)
		assert.Equal(t, name, msIdent(name))
	})
	t.Run("case and punctuation kept", func(t *testing.T) {
		assert.Equal(t, "Order Items-2024.[x]", msIdent("Order Items-2024.[x]"))
	})
	t.Run("129 characters shortened", func(t *testing.T) {
		name := strings.Repeat("a", 129)
		got := msIdent(name)
		assert.Len(t, got, 128)
		assert.Equal(t, strings.Repeat("a", 119)+"_", got[:120])
		assert.Regexp(t, `^[0-9a-f]{8}$`, got[120:])
	})
	t.Run("characters outside the BMP count twice and are not split", func(t *testing.T) {
		name := strings.Repeat("\U0001F600", 64)
		assert.Equal(t, name, msIdent(name))
		got := msIdent(name + "x")
		assert.True(t, utf8.ValidString(got))
		assert.Equal(t, 127, identLen(got))
		assert.Equal(t, strings.Repeat("\U0001F600", 59)+"_", string([]rune(got)[:60]))
	})
	t.Run("names sharing a prefix stay unique", func(t *testing.T) {
		prefix := strings.Repeat("p", 119)
		a, b := msIdent(prefix+"_first_name"), msIdent(prefix+"_other_name")
		assert.Len(t, a, 128)
		assert.Len(t, b, 128)
		assert.NotEqual(t, a, b)
	})
	t.Run("stable", func(t *testing.T) {
		name := strings.Repeat("stable_", 20)
		assert.Equal(t, msIdent(name), msIdent(name))
	})
	t.Run("quote doubles closing brackets", func(t *testing.T) {
		assert.Equal(t, "[a]]b[c]", quote("a]b[c"))
		assert.Equal(t, "[s].[t]]]", qualified("s", "t]"))
	})
	t.Run("collision fails naming both, ignoring case", func(t *testing.T) {
		n := newNameSet("column")
		require.NoError(t, n.claim("Name", "Name"))
		err := n.claim("name", "name")
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"Name"`)
		assert.Contains(t, err.Error(), `"name"`)
	})
	t.Run("names SQL Server cannot hold fail", func(t *testing.T) {
		n := newNameSet("column")
		for _, bad := range []string{"", "a\x00b", "trailing ", "bad \xff"} {
			require.Error(t, n.claim(bad, bad), "%q", bad)
		}
	})
}
