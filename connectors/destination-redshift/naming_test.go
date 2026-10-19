package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdent(t *testing.T) {
	assert.Equal(t, "createdat", rsIdent("CreatedAt"))
	assert.Equal(t, "Äbc", rsIdent("ÄBC"), "only ASCII letters fold")
	n127 := strings.Repeat("a", 127)
	assert.Equal(t, n127, rsIdent(n127))

	long := strings.Repeat("b", 200)
	got := rsIdent(long)
	assert.Len(t, got, 127)
	assert.Equal(t, got, rsIdent(long), "stable")
	assert.NotEqual(t, got, rsIdent(strings.Repeat("b", 199)+"c"))

	multi := strings.Repeat("a", 117) + strings.Repeat("é", 10)
	got = rsIdent(multi)
	assert.LessOrEqual(t, len(got), 127)
	assert.True(t, strings.HasPrefix(got, strings.Repeat("a", 117)+"_"), got)
}

func TestNameCollision(t *testing.T) {
	cols := newNameSet("column")
	require.NoError(t, cols.claim(rsIdent("Id"), "Id"))
	err := cols.claim(rsIdent("id"), "id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `column names "Id" and "id" both map to "id"`)
	assert.ErrorContains(t, cols.claim("a\nb", "a\nb"), "control character")
}
