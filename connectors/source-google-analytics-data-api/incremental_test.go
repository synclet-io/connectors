package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func activeUsersState(properties string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"daily_active_users","namespace":"google_analytics"},` +
		`"stream_state":{"properties":` + properties + `}}}]`)
}

// days returns the requested day of each runReport of prop for
// daily_active_users.
func days(h *hits, prop string) []string {
	var out []string
	for _, r := range h.reports(prop, "active1DayUsers") {
		out = append(out, r.DateRanges[0].StartDate)
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
			name: "saved date minus the default lookback of 3 days, start_date without one",
			edit: func(c *Config) { c.StartDate = "2026-10-06" },
			// The prefixed id of a saved state counts for its property.
			state:  activeUsersState(`{"properties/123456789":{"date":"2026-10-12"}}`),
			daysA:  []string{"2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			daysB:  []string{"2026-10-06", "2026-10-07", "2026-10-08", "2026-10-09", "2026-10-10", "2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			recsA:  []string{"2026-10-10", "2026-10-14", "2026-10-16"},
			recsB:  []string{"2026-10-06", "2026-10-14"},
			states: []string{`{"properties":{"123456789":{"date":"2026-10-16"},"987654321":{"date":"2026-10-16"}}}`},
		},
		{
			name:  "lookback floored at start_date",
			edit:  func(c *Config) { c.StartDate = "2026-10-11" },
			state: activeUsersState(`{"123456789":{"date":"2026-10-12"},"987654321":{"date":"2026-10-15"}}`),
			daysA: []string{"2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			daysB: []string{"2026-10-12", "2026-10-13", "2026-10-14", "2026-10-15", "2026-10-16"},
			recsA: []string{"2026-10-14", "2026-10-16"},
			recsB: []string{"2026-10-14"},
		},
		{
			name:  "configured lookback and end_date",
			edit:  func(c *Config) { c.LookbackDays = intp(0); c.EndDate = "2026-10-14" },
			state: activeUsersState(`{"123456789":{"date":"2026-10-14"},"987654321":{"date":"2026-10-13"}}`),
			daysA: []string{"2026-10-14"},
			daysB: []string{"2026-10-13", "2026-10-14"},
			recsA: []string{"2026-10-14"},
			recsB: []string{"2026-10-14"},
		},
		{
			name:  "saved date past end_date reads nothing",
			edit:  func(c *Config) { c.EndDate = "2026-10-10" },
			state: activeUsersState(`{"123456789":{"date":"2026-10-16"},"987654321":{"date":"2026-10-16"}}`),
			daysA: nil,
			daysB: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, nil)
			src, _ := testSource(srv)
			cfg := testConfig(t, tt.edit)
			out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, "daily_active_users"), tt.state)
			require.NoError(t, out.Err)
			assert.Equal(t, tt.daysA, days(h, testPropA))
			assert.Equal(t, tt.daysB, days(h, testPropB))
			var recsA, recsB []string
			for _, r := range records(t, out, "daily_active_users") {
				if r["property_id"] == testPropA {
					recsA = append(recsA, r["date"].(string))
				} else {
					recsB = append(recsB, r["date"].(string))
				}
			}
			assert.Equal(t, tt.recsA, recsA)
			assert.Equal(t, tt.recsB, recsB)
			states := streamStates(t, out)
			assert.Len(t, states, len(tt.daysA)+len(tt.daysB), "one state per day read")
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
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, "daily_active_users"), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{
		`{"properties":{"123456789":{"date":"2026-10-15"}}}`,
		`{"properties":{"123456789":{"date":"2026-10-16"}}}`,
		`{"properties":{"123456789":{"date":"2026-10-16"},"987654321":{"date":"2026-10-15"}}}`,
		`{"properties":{"123456789":{"date":"2026-10-16"},"987654321":{"date":"2026-10-16"}}}`,
	}, streamStates(t, out))
}

func TestIncrementalBadState(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src, _ := testSource(srv)
	cfg := testConfig(t, nil)
	out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeIncremental, "daily_active_users"),
		activeUsersState(`{"123456789":{"date":"16/10/2026"}}`))
	require.Error(t, out.Err)
	assert.Equal(t, `daily_active_users: property 123456789: saved date "16/10/2026" is not a YYYY-MM-DD date`, out.Err.Error())
}
