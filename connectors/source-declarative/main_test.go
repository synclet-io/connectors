package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func spec(t *testing.T, src synclet.Source[json.RawMessage]) *testkit.Output {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandSpec})
	require.NoError(t, err)
	return out
}

func TestResolveBaked(t *testing.T) {
	out := spec(t, resolve("example"))
	require.NoError(t, out.Err)
	assert.Equal(t, "Acme Source Spec", out.Spec().ConnectionSpecification.Title)
}

func TestResolveGeneric(t *testing.T) {
	out := spec(t, resolve(t.TempDir()))
	require.NoError(t, out.Err)
	assert.Equal(t, []synclet.PropertyName{"__manifest"}, out.Spec().ConnectionSpecification.Required)
}

func TestResolveBrokenBakedManifest(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "manifest.yaml"), []byte("version: 2\n"), 0o600))
	out := spec(t, resolve(dir))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "version: value must be 1")
	assert.Nil(t, out.Spec())
}
