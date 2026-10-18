package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		err  string
	}{
		{name: "valid"},
		{name: "missing developer token", edit: func(c *Config) { c.DeveloperToken = "" }, err: "developer_token is required"},
		{name: "missing refresh token", edit: func(c *Config) { c.RefreshToken = " " }, err: "refresh_token is required"},
		{name: "no customers", edit: func(c *Config) { c.CustomerIDs = nil }, err: "customer_ids needs at least one customer id"},
		{name: "bad customer", edit: func(c *Config) { c.CustomerIDs = []string{"abc"} }, err: `customer id "abc" is not a number`},
		{name: "bad child", edit: func(c *Config) { c.ChildCustomerIDs = []string{"x-1"} }, err: `child customer id "x-1" is not a number`},
		{name: "bad login", edit: func(c *Config) { c.LoginCustomerID = "mgr" }, err: `login_customer_id "mgr" is not a number`},
		{name: "missing start", edit: func(c *Config) { c.StartDate = "" }, err: "start_date is required"},
		{name: "bad start", edit: func(c *Config) { c.StartDate = "01.10.2026" }, err: `start_date "01.10.2026" is not a YYYY-MM-DD date`},
		{name: "end before start", edit: func(c *Config) { c.EndDate = "2026-09-01" }, err: "end_date 2026-09-01 is before start_date 2026-10-01"},
		{name: "negative lookback", edit: func(c *Config) { c.LookbackDays = lookback(-1) }, err: "lookback_days -1 is negative"},
		{name: "bad custom query", edit: func(c *Config) { c.CustomQueries = []CustomQuery{{Name: "x", Query: "FROM campaign"}} }, err: "custom query x: query must start with SELECT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			require.NoError(t, json.Unmarshal(testConfig(t, tt.edit), &cfg))
			err := cfg.validate()
			if tt.err == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.err)
		})
	}

	var cfg Config
	require.NoError(t, json.Unmarshal(testConfig(t, func(c *Config) {
		c.CustomerIDs = []string{"123-456-7890", " 2345678901 "}
		c.LoginCustomerID = "111-111-1111"
	}), &cfg))
	assert.Equal(t, []string{testClientA, testClientB}, cfg.customers())
	assert.Equal(t, testManager, cfg.login())
	assert.Equal(t, defaultLookbackDays, cfg.lookback())
}
