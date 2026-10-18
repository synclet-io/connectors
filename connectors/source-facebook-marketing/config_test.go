package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	neg := -1
	valid := func(edit func(*Config)) Config {
		c := Config{AccessToken: "x", AccountIDs: []string{"act_123", "456"}, StartDate: "2026-01-31"}
		if edit != nil {
			edit(&c)
		}
		return c
	}
	tests := []struct {
		name string
		cfg  Config
		err  string
	}{
		{name: "valid", cfg: valid(nil)},
		{name: "no access_token", cfg: valid(func(c *Config) { c.AccessToken = "" }), err: "access_token is required"},
		{name: "no accounts", cfg: valid(func(c *Config) { c.AccountIDs = nil }), err: "account_ids needs at least one ad account id"},
		{name: "bad account", cfg: valid(func(c *Config) { c.AccountIDs = []string{"act_12a"} }), err: `account id "act_12a" is not a number`},
		{name: "bad business", cfg: valid(func(c *Config) { c.BusinessIDs = []string{"biz"} }), err: `business id "biz" is not a number`},
		{name: "no start_date", cfg: valid(func(c *Config) { c.StartDate = "" }), err: "start_date is required"},
		{name: "bad start_date", cfg: valid(func(c *Config) { c.StartDate = "2026-1-31" }), err: `start_date "2026-1-31" is not a YYYY-MM-DD date`},
		{name: "bad end_date", cfg: valid(func(c *Config) { c.EndDate = "2026-02-30" }), err: `end_date "2026-02-30" is not a YYYY-MM-DD date`},
		{name: "end before start", cfg: valid(func(c *Config) { c.EndDate = "2026-01-30" }), err: "end_date 2026-01-30 is before start_date 2026-01-31"},
		{name: "negative lookback", cfg: valid(func(c *Config) { c.LookbackDays = &neg }), err: "lookback_days -1 is negative"},
		{name: "bad base URL", cfg: valid(func(c *Config) { c.GraphAPIBaseURL = "graph.example.com" }), err: "graph_api_base_url"},
		{name: "header without name", cfg: valid(func(c *Config) { c.GraphAPIHeaders = []Header{{Value: "v"}} }), err: "graph_api_headers[0] has no name"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.err == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestConfigAccounts(t *testing.T) {
	c := Config{AccountIDs: []string{"act_123", " 456 "}}
	assert.Equal(t, []string{"123", "456"}, c.accounts())
	assert.Equal(t, defaultLookbackDays, c.lookback())
}
