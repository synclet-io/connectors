package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestCheck(t *testing.T) {
	cases := []struct {
		name      string
		mod       func(*Config)
		overrides map[string]fixture
		want      string
	}{
		{name: "service account"},
		{name: "project secret", mod: projectSecret},
		{name: "wrong secret", mod: func(c *Config) { c.Credentials.Secret = "wrong" }, want: "invalid credentials"},
		{name: "other project", mod: func(c *Config) { c.Credentials.ProjectID = 999 }, want: "check the project_id and region"},
		{
			name: "no access", overrides: map[string]fixture{"cohorts/list": {status: http.StatusForbidden, file: "errors/forbidden.json"}},
			want: "a service account needs a project role",
		},
		{name: "missing project", mod: func(c *Config) { c.Credentials.ProjectID = 0 }, want: "credentials.project_id is required"},
		{name: "bad region", mod: func(c *Config) { c.Region = "APAC" }, want: `region "APAC"`},
		{name: "bad zone", mod: func(c *Config) { c.ProjectTimezone = "Mars/Base" }, want: `project_timezone "Mars/Base"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tc.overrides)
			var mods []func(*Config)
			if tc.mod != nil {
				mods = append(mods, tc.mod)
			}
			st := runCheck(t, testSource(srv, newClock()), testConfig(t, mods...))
			if tc.want == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
				assert.Equal(t, []string{"query cohorts/list"}, h.list())
				return
			}
			require.Equal(t, synclet.ConnectionStatusFailed, st.Status)
			assert.Contains(t, st.Message, tc.want)
			assert.LessOrEqual(t, len(h.list()), 1, "check fails without retrying")
		})
	}
}
