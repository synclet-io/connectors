package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigValidate(t *testing.T) {
	valid := Config{ClientID: "id", ClientSecret: "secret", RefreshToken: "token", InstanceURL: "https://acme.my.example.com/"}
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{name: "valid"},
		{name: "missing credentials", edit: func(c *Config) { c.ClientID, c.ClientSecret, c.RefreshToken = "", "", "" }, want: "client_id is required\nclient_secret is required\nrefresh_token is required"},
		{name: "missing instance", edit: func(c *Config) { c.InstanceURL = "" }, want: "instance_url is required"},
		{name: "instance with path", edit: func(c *Config) { c.InstanceURL = "https://acme.my.example.com/services/data" }, want: "must be the https URL of the org"},
		{name: "instance without scheme", edit: func(c *Config) { c.InstanceURL = "acme.my.example.com" }, want: "must be the https URL of the org"},
		{name: "start date", edit: func(c *Config) { c.StartDate = "01/02/2026" }, want: `start_date "01/02/2026" is neither`},
		{name: "start date time", edit: func(c *Config) { c.StartDate = "2026-01-02T03:04:05+02:00" }},
		{name: "threshold too high", edit: func(c *Config) { c.APIUsageThresholdPercent = 101 }, want: "api_usage_threshold_percent 101 must be between 1 and 100"},
		{name: "threshold negative", edit: func(c *Config) { c.APIUsageThresholdPercent = -1 }, want: "api_usage_threshold_percent -1 must be between 1 and 100"},
		{name: "bad exclude pattern", edit: func(c *Config) { c.StreamsExclude = []string{"[a-"} }, want: `stream pattern "[a-" is malformed`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			if tt.edit != nil {
				tt.edit(&cfg)
			}
			err := cfg.validate()
			if tt.want == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tt.want)
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := Config{InstanceURL: "https://Acme.My.Example.com/"}
	inst, err := cfg.instance()
	assert.NoError(t, err)
	assert.Equal(t, "https://acme.my.example.com", inst)
	assert.Equal(t, 90, cfg.threshold())
	start, err := Config{StartDate: "2026-01-02T03:04:05+02:00"}.start()
	assert.NoError(t, err)
	assert.Equal(t, "2026-01-02T01:04:05Z", start.Format("2006-01-02T15:04:05Z07:00"))
}

func TestConfigSelected(t *testing.T) {
	cfg := Config{StreamsInclude: []string{"*__c", "Account"}, StreamsExclude: []string{"Invoice*"}}
	assert.True(t, cfg.selected("Account"))
	assert.True(t, cfg.selected("Region__c"))
	assert.False(t, cfg.selected("Invoice__c"))
	assert.False(t, cfg.selected("Contact"))
	assert.True(t, Config{}.selected("Contact"))
}
