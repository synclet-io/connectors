package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func intp(n int) *int    { return &n }
func boolp(b bool) *bool { return &b }

func decodeConfig(t *testing.T, raw json.RawMessage) Config {
	t.Helper()
	var c Config
	require.NoError(t, json.Unmarshal(raw, &c))
	return c
}

func metricNames(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "customEvent:m" + string(rune('a'+i))
	}
	return out
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		errs []string
	}{
		{name: "service account"},
		{name: "oauth", edit: withOAuth},
		{name: "custom report", edit: withMyReport},
		{
			name: "missing fields",
			edit: func(c *Config) { c.Credentials = Credentials{}; c.PropertyIDs = nil; c.StartDate = "" },
			errs: []string{"credentials.auth_type is required", "property_ids needs at least one property id", "start_date is required"},
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
			name: "bad property ids",
			edit: func(c *Config) { c.PropertyIDs = []string{"UA-1234-1", testPropA, "properties/" + testPropA} },
			errs: []string{`property id "UA-1234-1" is not a number`, "property id 123456789 is listed twice"},
		},
		{
			name: "bad dates",
			edit: func(c *Config) { c.StartDate = "2026/10/05"; c.EndDate = "yesterday" },
			errs: []string{`start_date "2026/10/05" is not a YYYY-MM-DD date`, `end_date "yesterday" is not a YYYY-MM-DD date`},
		},
		{
			name: "end before start",
			edit: func(c *Config) { c.EndDate = "2026-10-01" },
			errs: []string{"end_date 2026-10-01 is before start_date 2026-10-05"},
		},
		{
			name: "negative lookback",
			edit: func(c *Config) { c.LookbackDays = intp(-1) },
			errs: []string{"lookback_days -1 is negative"},
		},
		{
			name: "bad report name",
			edit: func(c *Config) { c.CustomReports = []CustomReport{{Name: "My Report", Metrics: []string{"sessions"}}} },
			errs: []string{`custom_reports[0]: name "My Report" does not match ^[a-z][a-z0-9_]*$`},
		},
		{
			name: "duplicate report name",
			edit: func(c *Config) { c.CustomReports = []CustomReport{myReport, myReport} },
			errs: []string{"custom_reports[1]: name my_report is taken by another report"},
		},
		{
			name: "built-in report name",
			edit: func(c *Config) { c.CustomReports = []CustomReport{{Name: "pages", Metrics: []string{"sessions"}}} },
			errs: []string{"custom_reports[0]: name pages is taken by another report"},
		},
		{
			name: "ten dimensions with date",
			edit: func(c *Config) {
				c.CustomReports = []CustomReport{{Name: "wide", Dimensions: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, Metrics: []string{"sessions"}}}
			},
			errs: []string{"custom_reports[0]: wide: 10 dimensions with date, at most 9"},
		},
		{
			name: "nine dimensions with date listed",
			edit: func(c *Config) {
				c.CustomReports = []CustomReport{{Name: "wide", Dimensions: []string{"a", "b", "c", "date", "e", "f", "g", "h", "i"}, Metrics: []string{"sessions"}}}
			},
		},
		{
			name: "no metrics",
			edit: func(c *Config) { c.CustomReports = []CustomReport{{Name: "empty", Dimensions: []string{"country"}}} },
			errs: []string{"custom_reports[0]: empty: 0 metrics, needs 1 to 10"},
		},
		{
			name: "eleven metrics",
			edit: func(c *Config) { c.CustomReports = []CustomReport{{Name: "many", Metrics: metricNames(11)}} },
			errs: []string{"custom_reports[0]: many: 11 metrics, needs 1 to 10"},
		},
		{
			name: "duplicate fields",
			edit: func(c *Config) {
				c.CustomReports = []CustomReport{{Name: "dup", Dimensions: []string{"country", "country"}, Metrics: []string{"sessions", "sessions"}}}
			},
			errs: []string{"dimension country is listed twice", "metric sessions is listed twice"},
		},
		{
			name: "record key collision",
			edit: func(c *Config) {
				c.CustomReports = []CustomReport{{Name: "clash", Dimensions: []string{"customEvent:a_b", "customEvent_a:b"}, Metrics: []string{"sessions"}}}
			},
			errs: []string{"custom report clash: customEvent:a_b and customEvent_a:b both become record key customEvent_a_b"},
		},
		{
			name: "collision with a data quality field",
			edit: func(c *Config) {
				c.CustomReports = []CustomReport{{Name: "clash", Metrics: []string{"sampled"}}}
			},
			errs: []string{"custom report clash: the data quality field sampled and sampled both become record key sampled"},
		},
		{
			name: "no collision without data quality fields",
			edit: func(c *Config) {
				c.DataQualityFields = boolp(false)
				c.CustomReports = []CustomReport{{Name: "clash", Metrics: []string{"sampled"}}}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := decodeConfig(t, testConfig(t, tt.edit)).validate()
			if len(tt.errs) == 0 {
				require.NoError(t, err)
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
			myReport,
			{Name: "date_later", Dimensions: []string{"country", "date"}, Metrics: []string{"sessions"}},
		}
	}))
	defs, err := reportDefs(cfg)
	require.NoError(t, err)
	require.Len(t, defs, len(builtinDefs)+2)
	assert.Equal(t, []string{"date", "customEvent:plan_tier", "country"}, defs[len(builtinDefs)].dimensions)
	assert.Equal(t, []string{"date", "country"}, defs[len(builtinDefs)+1].dimensions)
	for _, d := range defs {
		assert.Equal(t, dateDimension, d.dimensions[0], d.name)
	}
}
