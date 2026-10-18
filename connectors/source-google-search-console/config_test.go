package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intp(n int) *int { return &n }

func decodeConfig(t *testing.T, raw json.RawMessage) Config {
	t.Helper()
	var c Config
	require.NoError(t, json.Unmarshal(raw, &c))
	return c
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		errs []string
	}{
		{name: "service account"},
		{name: "oauth", edit: withOAuth},
		{name: "custom report", edit: withDiscoverPages},
		{name: "no start_date", edit: func(c *Config) { c.StartDate = "" }},
		{name: "data_state all", edit: func(c *Config) { c.DataState = dataStateAll }},
		{name: "URL-prefix property with a path", edit: func(c *Config) { c.SiteURLs = []string{"https://www.example.com/blog/"} }},
		{name: "http URL-prefix property", edit: func(c *Config) { c.SiteURLs = []string{"http://www.example.com/", "https://www.example.com/"} }},
		{
			name: "missing fields",
			edit: func(c *Config) { c.Credentials = Credentials{}; c.SiteURLs = nil },
			errs: []string{"credentials.auth_type is required", "site_urls needs at least one site"},
		},
		{
			name: "missing service account key",
			edit: func(c *Config) { c.Credentials.ServiceAccountKey = "" },
			errs: []string{"credentials.service_account_key is required"},
		},
		{
			name: "malformed service account key",
			edit: func(c *Config) {
				c.Credentials.ServiceAccountKey = `{"type":"service_account","client_email":"a@b.test","private_key":"not a key"}`
			},
			errs: []string{"service_account_key is not a valid service account key: private_key is not PEM encoded"},
		},
		{
			name: "missing oauth fields",
			edit: func(c *Config) { c.Credentials = Credentials{AuthType: authOAuth} },
			errs: []string{"credentials.client_id is required", "credentials.client_secret is required", "credentials.refresh_token is required"},
		},
		{
			name: "unknown auth type",
			edit: func(c *Config) { c.Credentials.AuthType = "api_key" },
			errs: []string{`credentials.auth_type "api_key" is neither service_account nor oauth`},
		},
		{
			name: "bad site urls",
			edit: func(c *Config) {
				c.SiteURLs = []string{"www.example.com", "https://www.example.com", "http://www.example.com", "sc-domain:", "sc-domain:example.org/x", "https:///", "ftp://www.example.com/", testSiteB, testSiteB}
			},
			errs: []string{
				`site "www.example.com" is neither a URL-prefix property http(s)://<host>/ nor a domain property sc-domain:<domain>`,
				`site "https://www.example.com" is not a URL-prefix property http(s)://<host>/...: it must end with /`,
				`site "http://www.example.com" is not a URL-prefix property http(s)://<host>/...: it must end with /`,
				`site "ftp://www.example.com/" is neither a URL-prefix property`,
				`site "sc-domain:" is not a domain property sc-domain:<domain>`,
				`site "sc-domain:example.org/x" is not a domain property sc-domain:<domain>`,
				`site "https:///" is not a URL-prefix property`,
				"site sc-domain:example.org is listed twice",
			},
		},
		{
			name: "bad dates and lookback",
			edit: func(c *Config) { c.StartDate = "2026-10-14"; c.EndDate = "2026-10-01"; c.LookbackDays = intp(-1) },
			errs: []string{"end_date 2026-10-01 is before start_date 2026-10-14", "lookback_days -1 is negative"},
		},
		{
			name: "start_date not a date",
			edit: func(c *Config) { c.StartDate = "14/10/2026" },
			errs: []string{`start_date "14/10/2026" is not a YYYY-MM-DD date`},
		},
		{
			name: "bad data_state",
			edit: func(c *Config) { c.DataState = "hourly_all" },
			errs: []string{`data_state "hourly_all" is neither final nor all`},
		},
		{
			name: "bad custom reports",
			edit: func(c *Config) {
				c.CustomReports = []CustomReport{
					{Name: "Bad-Name"},
					{Name: "search_analytics_by_date"},
					{Name: "sites"},
					{Name: "by_keyword", Dimensions: []string{"keyword"}},
					{Name: "twice", Dimensions: []string{"query", "query"}},
					{Name: "news_queries", Dimensions: []string{"query"}, SearchType: "googleNews"},
					{Name: "shopping", SearchType: "shopping"},
					discoverPages,
					discoverPages,
				}
			},
			errs: []string{
				`custom_reports[0]: name "Bad-Name" does not match ^[a-z][a-z0-9_]*$`,
				"custom_reports[1]: name search_analytics_by_date is taken by another stream",
				"custom_reports[2]: name sites is taken by another stream",
				`custom_reports[3]: by_keyword: dimension "keyword" is not one of date, query, page, country, device, searchAppearance`,
				"custom_reports[4]: twice: dimension query is listed twice",
				"custom_reports[5]: news_queries: search type googleNews does not support the query dimension",
				`custom_reports[6]: shopping: search_type "shopping" is not one of web, image, video, news, discover, googleNews`,
				"custom_reports[8]: name discover_pages is taken by another stream",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := decodeConfig(t, testConfig(t, tt.edit)).validate()
			if tt.errs == nil {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			for _, e := range tt.errs {
				assert.True(t, strings.Contains(err.Error(), e), "%q lacks %q", err.Error(), e)
			}
		})
	}
}

func TestReportDefsAddDate(t *testing.T) {
	cfg := decodeConfig(t, testConfig(t, func(c *Config) {
		c.CustomReports = []CustomReport{
			discoverPages,
			{Name: "date_later", Dimensions: []string{"country", "date", "device"}},
		}
	}))
	defs, err := reportDefs(cfg)
	require.NoError(t, err)
	require.Len(t, defs, len(builtinDefs)+2)
	assert.Equal(t, []string{"date", "page"}, defs[len(builtinDefs)].dimensions)
	assert.Equal(t, []string{"discover"}, defs[len(builtinDefs)].searchTypes)
	assert.Equal(t, []string{"date", "country", "device"}, defs[len(builtinDefs)+1].dimensions)
	assert.Equal(t, []string{"web"}, defs[len(builtinDefs)+1].searchTypes, "search_type defaults to web")
	for _, d := range defs {
		assert.Equal(t, dimDate, d.dimensions[0], d.name)
	}
}

func TestDefaults(t *testing.T) {
	cfg := decodeConfig(t, testConfig(t, func(c *Config) { c.StartDate = "" }))
	start, err := cfg.start(testNow.Truncate(24 * time.Hour))
	require.NoError(t, err)
	assert.Equal(t, "2025-06-16", start.Format(dateLayout), "16 months before today")
	assert.Equal(t, defaultLookbackDays, cfg.lookback())
	assert.Equal(t, dataStateFinal, cfg.dataState())
}
