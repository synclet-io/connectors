package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intp(n int) *int { return &n }

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		err  string
	}{
		{"valid", Config{AccessToken: testToken}, ""},
		{"valid bounds", Config{AccessToken: testToken, LookbackMinutes: intp(0), MaxBlockDepth: intp(100), MaxBlocksPerPage: intp(1)}, ""},
		{"no token", Config{}, "access_token is required"},
		{"negative lookback", Config{AccessToken: testToken, LookbackMinutes: intp(-1)}, "lookback_minutes must be 0 or more, got -1"},
		{"depth 0", Config{AccessToken: testToken, MaxBlockDepth: intp(0)}, "max_block_depth must be between 1 and 100, got 0"},
		{"depth 101", Config{AccessToken: testToken, MaxBlockDepth: intp(101)}, "max_block_depth must be between 1 and 100, got 101"},
		{"blocks 0", Config{AccessToken: testToken, MaxBlocksPerPage: intp(0)}, "max_blocks_per_page must be 1 or more, got 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.err == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.err)
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{AccessToken: testToken}
	assert.Equal(t, 5*60, int(c.lookback().Seconds()))
	assert.Equal(t, 16, c.maxBlockDepth())
	assert.Equal(t, 10000, c.maxBlocksPerPage())
	assert.Equal(t, 0, int(Config{LookbackMinutes: intp(0)}.lookback()))
}
