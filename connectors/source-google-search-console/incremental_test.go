package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const byDate = "search_analytics_by_date"

func byDateState(sites string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + byDate + `","namespace":"` + namespace + `"},` +
		`"stream_state":{"sites":` + sites + `}}}]`)
}

// days returns the requested day of each web query of site for
// search_analytics_by_date.
func days(h *hits, site string) []string {
	var out []string
	for _, q := range h.queries(site) {
		if q.Type == "web" {
			out = append(out, q.StartDate)
		}
	}
	return out
}

func TestIncremental(t *testing.T) {
	tests := []struct {
		name   string
		edit   func(*Config)
		state  json.RawMessage
		daysA  []string
		daysB  []string
		recsA  []string
		recsB  []string
		states []string
	}{
		{
			name:  "saved date minus the default lookback of 3 days, start_date without one",
			edit:  func(c *Config) { c.StartDate = "2026-10-06" },
			state: byDateState(`{"https://www.example.com/":{"date":"2026-10-10"}}`),
			daysA: []string{"2026-10-07", "2026-10-08", "2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			daysB: []string{"2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			recsA: []string{"2026-10-14", "2026-10-16"},
			recsB: []string{"2026-10-15"},
			states: []string{
				`{"sites":{"https://www.example.com/":{"date":"2026-10-16"},"sc-domain:example.org":{"date":"2026-10-16"}}}`,
			},
		},
		{
			name:  "lookback floored at start_date",
			edit:  func(c *Config) { c.StartDate = "2026-10-13" },
			state: byDateState(`{"https://www.example.com/":{"date":"2026-10-14"},"sc-domain:example.org":{"date":"2026-10-16"}}`),
			daysA: []string{"2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			daysB: []string{"2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			recsA: []string{"2026-10-14", "2026-10-16"},
			recsB: []string{"2026-10-15"},
		},
		{
			name:  "configured lookback and end_date",
			edit:  func(c *Config) { c.StartDate = "2026-10-01"; c.LookbackDays = intp(1); c.EndDate = "2026-10-15" },
			state: byDateState(`{"https://www.example.com/":{"date":"2026-10-15"},"sc-domain:example.org":{"date":"2026-10-12"}}`),
			daysA: []string{"2026-10-14", "2026-10-15"},
			daysB: []string{"2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15"},
			recsA: []string{"2026-10-14"},
			recsB: []string{"2026-10-15"},
		},
		{
			name:  "default start 16 months back",
			edit:  func(c *Config) { c.StartDate = "" },
			state: byDateState(`{"https://www.example.com/":{"date":"2026-10-16"},"sc-domain:example.org":{"date":"2026-10-16"}}`),
			daysA: []string{"2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			daysB: []string{"2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			recsA: []string{"2026-10-14", "2026-10-16"},
			recsB: []string{"2026-10-15"},
		},
		{
			name:  "saved date past end_date reads nothing",
			edit:  func(c *Config) { c.StartDate = "2026-10-01"; c.EndDate = "2026-10-10" },
			state: byDateState(`{"https://www.example.com/":{"date":"2026-10-16"},"sc-domain:example.org":{"date":"2026-10-16"}}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src, _ := testSource(srv)
			cfg := testConfig(t, tt.edit)
			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, byDate), tt.state)
			require.NoError(t, out.Err)
			assert.Equal(t, tt.daysA, days(h, testSiteA))
			assert.Equal(t, tt.daysB, days(h, testSiteB))
			var recsA, recsB []string
			for _, r := range records(t, out, byDate) {
				if r["search_type"] != "web" {
					continue
				}
				if r["site_url"] == testSiteA {
					recsA = append(recsA, r["date"].(string))
				} else {
					recsB = append(recsB, r["date"].(string))
				}
			}
			assert.Equal(t, tt.recsA, recsA)
			assert.Equal(t, tt.recsB, recsB)
			states := streamStates(t, out)
			assert.Len(t, states, len(tt.daysA)+len(tt.daysB), "one state per site-day")
			if tt.states != nil {
				assert.Equal(t, tt.states, states[len(states)-1:])
			}
		})
	}
}

func TestIncrementalStateAfterEachDay(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, func(c *Config) { c.StartDate = "2026-10-15" })
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, byDate), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		`{"sites":{"https://www.example.com/":{"date":"2026-10-15"}}}`,
		`{"sites":{"https://www.example.com/":{"date":"2026-10-16"}}}`,
		`{"sites":{"https://www.example.com/":{"date":"2026-10-16"},"sc-domain:example.org":{"date":"2026-10-15"}}}`,
		`{"sites":{"https://www.example.com/":{"date":"2026-10-16"},"sc-domain:example.org":{"date":"2026-10-16"}}}`,
	}, streamStates(t, out))
}

func TestIncrementalBadState(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, byDate),
		byDateState(`{"https://www.example.com/":{"date":"16/10/2026"}}`))
	require.Error(t, out.Err)
	assert.Equal(t, `search_analytics_by_date: site https://www.example.com/: saved date "16/10/2026" is not a YYYY-MM-DD date`, out.Err.Error())
}
