package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

const (
	checkAdvertiser = "report/advertiser/2026-10-17.json"
	checkMax        = "maxReport/2026-10-17.json"
)

var forbidden = fixture{status: http.StatusForbidden, file: "errors/report_unavailable.json"}

func runCheck(t *testing.T, src Source, key string) *testkit.Output {
	t.Helper()
	cfg, err := json.Marshal(Config{APIKey: key, StartDate: "2026-10-14"})
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out
}

func warnings(out *testkit.Output) []string {
	var ws []string
	for _, l := range out.Logs() {
		if l.Level == synclet.LogLevelWarn {
			ws = append(ws, l.Message)
		}
	}
	return ws
}

func TestCheckErrors(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		override map[string]fixture
		message  string
		paths    []string
	}{
		{
			name: "invalid key", key: "wrong-report-key",
			message: "invalid report key (HTTP 401)",
			paths:   []string{checkAdvertiser},
		},
		{
			name: "no report type available", key: testReportKey,
			override: map[string]fixture{"report/advertiser": forbidden, "maxReport": forbidden},
			message:  "report types advertiser and MAX revenue are not available to the report key (HTTP 403)",
			paths:    []string{checkAdvertiser, checkMax},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.override)
			status := runCheck(t, testSource(srv), tt.key).ConnectionStatus()
			assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
			assert.Contains(t, status.Message, tt.message)
			assert.NotContains(t, status.Message, tt.key)
			assert.Equal(t, tt.paths, h.paths(), "an error status is not retried")
		})
	}
}

func TestCheckOneReportTypeUnavailable(t *testing.T) {
	for _, tt := range []struct{ dir, typeName, stream string }{
		{"report/advertiser", "advertiser", streamAdvertiserReport},
		{"maxReport", "MAX revenue", streamMaxRevenueReport},
	} {
		t.Run(tt.typeName, func(t *testing.T) {
			srv, _ := fixtureServer(t, map[string]fixture{tt.dir: forbidden})
			out := runCheck(t, testSource(srv), testReportKey)
			assert.Equal(t, synclet.ConnectionStatusSucceeded, out.ConnectionStatus().Status)
			ws := warnings(out)
			require.Len(t, ws, 1)
			assert.Contains(t, ws[0], "report type "+tt.typeName+" is not available to the report key; reading "+tt.stream+" will fail")
		})
	}
}

func TestCheckSucceeds(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	out := runCheck(t, testSource(srv), testReportKey)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, out.ConnectionStatus().Status)
	assert.Empty(t, warnings(out))
	hs := h.all()
	require.Len(t, hs, 2)
	assert.Equal(t, "advertiser", hs[0].query.Get("report_type"))
	assert.False(t, hs[1].query.Has("report_type"))
	for _, x := range hs {
		assert.Equal(t, "day,impressions", x.query.Get("columns"))
		assert.Equal(t, "2026-10-17", x.query.Get("start"))
		assert.Equal(t, "2026-10-17", x.query.Get("end"))
		assert.Equal(t, "1", x.query.Get("limit"))
	}
}
