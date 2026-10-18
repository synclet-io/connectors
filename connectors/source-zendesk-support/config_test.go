package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidate(t *testing.T) {
	valid := Config{Subdomain: "acme", Email: testEmail, APIToken: testToken, StartDate: "2026-10-01"}
	with := func(f func(*Config)) Config {
		c := valid
		f(&c)
		return c
	}
	tests := []struct {
		name string
		cfg  Config
		err  string
	}{
		{name: "valid", cfg: valid},
		{name: "valid with hyphen and digits", cfg: with(func(c *Config) { c.Subdomain = "acme-2" })},
		{name: "valid RFC 3339 start", cfg: with(func(c *Config) { c.StartDate = "2026-10-01T08:30:00+02:00" })},
		{name: "no subdomain", cfg: with(func(c *Config) { c.Subdomain = "" }), err: "subdomain is required"},
		{name: "no email", cfg: with(func(c *Config) { c.Email = "" }), err: "email is required"},
		{name: "no api_token", cfg: with(func(c *Config) { c.APIToken = "" }), err: "api_token is required"},
		{name: "no start_date", cfg: with(func(c *Config) { c.StartDate = "" }), err: "start_date is required"},
		{
			name: "full host", cfg: with(func(c *Config) { c.Subdomain = "acme.zendesk.com" }),
			err: `subdomain "acme.zendesk.com" must be the account name only, such as "acme" for https://acme.zendesk.com`,
		},
		{
			name: "url", cfg: with(func(c *Config) { c.Subdomain = "https://acme.zendesk.com/" }),
			err: `subdomain "https://acme.zendesk.com/" must be the account name only, such as "acme"`,
		},
		{name: "uppercase", cfg: with(func(c *Config) { c.Subdomain = "Acme" }), err: `subdomain "Acme" may contain only lowercase letters, digits and hyphens`},
		{name: "leading hyphen", cfg: with(func(c *Config) { c.Subdomain = "-acme" }), err: "may contain only lowercase letters"},
		{name: "dot", cfg: with(func(c *Config) { c.Subdomain = "acme.eu" }), err: "may contain only lowercase letters"},
		{
			name: "bad start_date", cfg: with(func(c *Config) { c.StartDate = "2026-10-1" }),
			err: `start_date "2026-10-1" is neither an RFC 3339 time nor a YYYY-MM-DD date`,
		},
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

func TestConfigStart(t *testing.T) {
	day, err := Config{StartDate: "2026-10-01"}.start()
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), day)
	ts, err := Config{StartDate: "2026-10-01T08:30:00+02:00"}.start()
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 1, 6, 30, 0, 0, time.UTC), ts)
}

func TestStartTimeClamp(t *testing.T) {
	assert.Equal(t, int64(1790812800), startTime(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), testNow), "a past start is kept")
	assert.Equal(t, testNow.Add(-time.Minute).Unix(), startTime(testNow, testNow), "now is moved a minute back")
	assert.Equal(t, testNow.Add(-time.Minute).Unix(), startTime(testNow.Add(time.Hour), testNow), "a future start is moved a minute before now")
	assert.Equal(t, testNow.Add(-2*time.Minute).Unix(), startTime(testNow.Add(-2*time.Minute), testNow))
}
