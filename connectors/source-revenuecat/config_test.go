package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidate(t *testing.T) {
	assert.NoError(t, Config{APIKey: testKey}.validate())
	assert.NoError(t, Config{APIKey: testKey, ProjectID: testProj, Environment: "sandbox"}.validate())
	assert.NoError(t, Config{APIKey: testKey, Environment: "production"}.validate())
	assert.ErrorContains(t, Config{}.validate(), "api_key is required")
	assert.ErrorContains(t, Config{APIKey: testKey, Environment: "Production"}.validate(), `environment "Production" must be sandbox, production or empty`)
}
