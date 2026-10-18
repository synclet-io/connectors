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

func runCheck(t *testing.T, src Source, cfg Config) *synclet.ConnectionStatus {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: raw})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out.ConnectionStatus()
}

func TestCheck(t *testing.T) {
	valid := Config{AccessToken: testToken, StartDate: "2026-10-01"}
	tests := []struct {
		name      string
		cfg       Config
		overrides map[string]http.HandlerFunc
		want      []string
	}{
		{name: "valid token", cfg: valid},
		{
			name: "wrong token", cfg: Config{AccessToken: "wrong-private-app-token"},
			want: []string{"invalid private-app token (HTTP 401): Authentication credentials not found."},
		},
		{
			name: "deals scope missing", cfg: valid,
			overrides: map[string]http.HandlerFunc{
				"crm/v3/objects/deals.json":   respond(t, http.StatusForbidden, "errors/missing_scopes_deals.json"),
				"crm/v3/pipelines/deals.json": respond(t, http.StatusForbidden, "errors/missing_scopes_deals.json"),
			},
			want: []string{
				"missing scope for stream deals: crm.objects.deals.read",
				"missing scope for stream deal_pipelines: crm.objects.deals.read",
			},
		},
		{
			name: "schemas scope missing", cfg: valid,
			overrides: map[string]http.HandlerFunc{
				"crm/v3/schemas.json": respond(t, http.StatusForbidden, "errors/missing_scopes_schemas.json"),
			},
			want: []string{"missing scope for custom objects: crm.schemas.custom.read"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, tt.overrides)
			st := runCheck(t, testSource(srv), tt.cfg)
			if tt.want == nil {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
				return
			}
			assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
			for _, w := range tt.want {
				assert.Contains(t, st.Message, w)
			}
		})
	}
}

// TestCheckProbes asserts check reaches every stream's endpoint,
// including the custom objects the schemas API lists.
func TestCheckProbes(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	st := runCheck(t, testSource(srv), Config{AccessToken: testToken})
	require.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
	assert.Equal(t, []string{
		"GET crm/v3/objects/contacts?limit=1",
		"GET crm/v3/objects/companies?limit=1",
		"GET crm/v3/objects/deals?limit=1",
		"GET crm/v3/objects/tickets?limit=1",
		"GET crm/v3/objects/line_items?limit=1",
		"GET crm/v3/objects/products?limit=1",
		"GET crm/v3/owners?limit=1",
		"GET crm/v3/pipelines/deals",
		"GET crm/v3/pipelines/tickets",
		"GET crm/v3/schemas",
		"GET crm/v3/objects/2-1001?limit=1",
	}, h.lines())
}

func TestCheckConfig(t *testing.T) {
	st := runCheck(t, Source{}, Config{StartDate: "yesterday"})
	assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
	assert.Contains(t, st.Message, "access_token is required")
	assert.Contains(t, st.Message, `start_date "yesterday"`)
}
