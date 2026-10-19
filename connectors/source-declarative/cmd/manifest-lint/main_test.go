package main

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLintBadManifest(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"../../testdata/bad"}, &out, &errOut)
	assert.Equal(t, 1, code)
	assert.Contains(t, errOut.String(), "streams[0].foo: unknown key")
}

func TestLintExample(t *testing.T) {
	var out, errOut bytes.Buffer
	require.Equal(t, 0, run([]string{"../../example"}, &out, &errOut), errOut.String())
	assert.Empty(t, out.String())
}

func TestSpec(t *testing.T) {
	var out, errOut bytes.Buffer
	require.Equal(t, 0, run([]string{"-spec", "../../example"}, &out, &errOut), errOut.String())
	var spec struct {
		SupportsIncremental     bool `json:"supportsIncremental"`
		ConnectionSpecification struct {
			Properties map[string]map[string]any `json:"properties"`
		} `json:"connectionSpecification"`
	}
	require.NoError(t, json.Unmarshal(out.Bytes(), &spec))
	assert.True(t, spec.SupportsIncremental)
	assert.Equal(t, true, spec.ConnectionSpecification.Properties["api_key"]["airbyte_secret"])
}

func TestUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	assert.Equal(t, 2, run(nil, &out, &errOut))
}
