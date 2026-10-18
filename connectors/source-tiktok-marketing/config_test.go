package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	neg, big := -1, 31
	valid := func(edit func(*Config)) Config {
		c := Config{AccessToken: "x", AdvertiserIDs: []string{"123", "456"}, StartDate: "2026-01-31"}
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
		{name: "sandbox", cfg: valid(func(c *Config) { c.Environment = envSandbox })},
		{name: "no access_token", cfg: valid(func(c *Config) { c.AccessToken = "" }), err: "access_token is required"},
		{name: "no advertisers", cfg: valid(func(c *Config) { c.AdvertiserIDs = nil }), err: "advertiser_ids needs at least one advertiser id"},
		{name: "bad advertiser", cfg: valid(func(c *Config) { c.AdvertiserIDs = []string{"12a"} }), err: `advertiser id "12a" is not a number`},
		{name: "no start_date", cfg: valid(func(c *Config) { c.StartDate = "" }), err: "start_date is required"},
		{name: "bad start_date", cfg: valid(func(c *Config) { c.StartDate = "2026-1-31" }), err: `start_date "2026-1-31" is not a YYYY-MM-DD date`},
		{name: "bad end_date", cfg: valid(func(c *Config) { c.EndDate = "2026-02-30" }), err: `end_date "2026-02-30" is not a YYYY-MM-DD date`},
		{name: "end before start", cfg: valid(func(c *Config) { c.EndDate = "2026-01-30" }), err: "end_date 2026-01-30 is before start_date 2026-01-31"},
		{name: "negative lookback", cfg: valid(func(c *Config) { c.LookbackDays = &neg }), err: "lookback_days -1 is not between 0 and 30"},
		{name: "lookback too long", cfg: valid(func(c *Config) { c.LookbackDays = &big }), err: "lookback_days 31 is not between 0 and 30"},
		{name: "unknown environment", cfg: valid(func(c *Config) { c.Environment = "staging" }), err: `environment "staging" is neither production nor sandbox`},
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

func TestConfigDefaults(t *testing.T) {
	c := Config{AdvertiserIDs: []string{"123", " 456 "}}
	assert.Equal(t, []string{"123", "456"}, c.advertisers())
	assert.Equal(t, defaultLookbackDays, c.lookback())
	assert.Equal(t, envProduction, c.environment())
}
