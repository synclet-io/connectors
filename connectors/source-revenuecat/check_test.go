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

func runCheck(t *testing.T, src Source, cfg string) *synclet.ConnectionStatus {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(cfg)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out.ConnectionStatus()
}

func TestCheckSucceeds(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	st := runCheck(t, testSource(srv, newClock()), testConfig)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
	p := "projects/" + testProj + "/"
	assert.Equal(t, []string{
		"projects?limit=100",
		"projects?limit=100&starting_after=proj_test1",
		p + "apps?limit=1",
		p + "products?limit=1",
		p + "entitlements?limit=1",
		p + "offerings?limit=1",
		p + "customers?limit=1",
		p + "metrics/overview",
		cust + "cust_test_001/subscriptions?limit=1",
		cust + "cust_test_001/purchases?limit=1",
		cust + "cust_test_001/active_entitlements?limit=1",
	}, h.list())
}

func TestCheckCauses(t *testing.T) {
	forbidden := fixture{status: http.StatusForbidden, file: "errors/forbidden.json"}
	tests := []struct {
		name      string
		config    string
		overrides map[string]fixture
		want      []string
	}{
		{
			name:   "invalid key",
			config: `{"api_key":"wrong-key","project_id":"proj_test1"}`,
			want:   []string{"invalid API key (HTTP 401): authentication_error: Invalid API key.", "API v1 keys do not work with REST API v2"},
		},
		{
			name:   "v1 key",
			config: `{"api_key":"` + testV1Key + `","project_id":"proj_test1"}`,
			want:   []string{"API v1 key used for REST API v2"},
		},
		{
			name:      "key lacks permissions",
			config:    testConfig,
			overrides: map[string]fixture{cust + "cust_test_001/purchases": forbidden, "projects/" + testProj + "/metrics/overview": forbidden},
			want:      []string{"key lacks permission for streams: metrics_overview (charts_metrics:overview:read), customer_purchases (customer_information:purchases:read)"},
		},
		{
			name:   "unknown project",
			config: `{"api_key":"` + testKey + `","project_id":"proj_nosuch"}`,
			want:   []string{`unknown project "proj_nosuch": the key can see projects proj_test1, proj_test2`},
		},
		{
			name:      "unknown project when the key cannot list projects",
			config:    `{"api_key":"` + testKey + `","project_id":"proj_nosuch"}`,
			overrides: map[string]fixture{"projects": forbidden},
			want:      []string{`unknown project "proj_nosuch"`, "HTTP 404"},
		},
		{
			name:   "several projects without project_id",
			config: `{"api_key":"` + testKey + `"}`,
			want:   []string{"project_id is empty and the key can see 2 projects (proj_test1, proj_test2); set project_id to one of them"},
		},
		{
			name:      "no project_id and no permission to list projects",
			config:    `{"api_key":"` + testKey + `"}`,
			overrides: map[string]fixture{"projects": forbidden},
			want:      []string{"project_id is empty and the key cannot list projects (needs project_configuration:projects:read)"},
		},
		{
			name:   "unknown environment",
			config: `{"api_key":"` + testKey + `","environment":"staging"}`,
			want:   []string{`environment "staging" must be sandbox, production or empty`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, tt.overrides)
			st := runCheck(t, testSource(srv, newClock()), tt.config)
			assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
			for _, w := range tt.want {
				assert.Contains(t, st.Message, w)
			}
		})
	}
}

func TestCheckResolvesTheOnlyProject(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]fixture{"projects": {status: http.StatusOK, file: "single_project.json"}})
	st := runCheck(t, testSource(srv, newClock()), `{"api_key":"`+testKey+`"}`)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
}
