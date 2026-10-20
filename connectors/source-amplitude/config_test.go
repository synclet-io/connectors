package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeConfig(t *testing.T, raw string) Config {
	t.Helper()
	var c Config
	require.NoError(t, json.Unmarshal([]byte(raw), &c))
	return c
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name, config, err string
	}{
		{name: "valid", config: testConfig},
		{name: "date without time", config: `{"api_key":"a","secret_key":"s","start_date":"2026-10-16"}`},
		{name: "eu region", config: `{"api_key":"a","secret_key":"s","start_date":"2026-10-16T00:00:00Z","data_region":"EU Residency Server"}`},
		{name: "missing keys", config: `{"start_date":"2026-10-16T00:00:00Z"}`, err: "api_key is required\nsecret_key is required"},
		{name: "missing start", config: `{"api_key":"a","secret_key":"s"}`, err: "start_date is required"},
		{name: "bad start", config: `{"api_key":"a","secret_key":"s","start_date":"16.10.2026"}`, err: `start_date "16.10.2026" is not a YYYY-MM-DDTHH:mm:ssZ time`},
		{name: "unknown region", config: `{"api_key":"a","secret_key":"s","start_date":"2026-10-16","data_region":"US"}`, err: `data_region "US" is not "Standard Server" or "EU Residency Server"`},
		{name: "zero range", config: `{"api_key":"a","secret_key":"s","start_date":"2026-10-16","request_time_range":0}`, err: "request_time_range must be between 1 and 8760 hours, got 0"},
		{name: "range too long", config: `{"api_key":"a","secret_key":"s","start_date":"2026-10-16","request_time_range":8761}`, err: "request_time_range must be between 1 and 8760 hours, got 8761"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := decodeConfig(t, tt.config).validate()
			if tt.err == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.err)
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	c := decodeConfig(t, `{"start_date":"2026-10-16T05:30:00+02:00"}`)
	assert.Equal(t, defaultRequestTimeRange, c.timeRange())
	assert.True(t, c.groupByCountry())
	assert.Equal(t, regionStandard, c.region())
	start, err := c.start()
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 16, 3, 30, 0, 0, time.UTC), start)
}

func TestRegionBaseURL(t *testing.T) {
	for region, want := range map[string]string{
		"":             "https://amplitude.com/api/",
		regionStandard: "https://amplitude.com/api/",
		regionEU:       "https://analytics.eu.amplitude.com/api/",
	} {
		assert.Equal(t, want, Source{}.apiClient(Config{DataRegion: region}).baseURL, region)
	}
}
