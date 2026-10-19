package main

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckNamesTheCause fails check for each cause it names: rejected
// credentials, an unreachable or missing SQL warehouse, a missing catalog,
// schema or volume, and no write privilege.
func TestCheckNamesTheCause(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, ws *fakeWorkspace, sql *fakeSQL, cfg *Config)
		// realDriver connects databricks-sql-go to the fake workspace
		// instead of using the recorded SQL warehouse.
		realDriver bool
		want       string
	}{
		{
			name:       "invalid token",
			realDriver: true,
			setup: func(_ *testing.T, ws *fakeWorkspace, _ *fakeSQL, cfg *Config) {
				ws.sqlStatus = http.StatusUnauthorized
				cfg.Authentication.PersonalAccessToken = "wrong"
			},
			want: "invalid token",
		},
		{
			name: "invalid client credentials",
			setup: func(_ *testing.T, _ *fakeWorkspace, _ *fakeSQL, cfg *Config) {
				cfg.Authentication = Auth{AuthType: authOAuth, ClientID: testClientID, Secret: "wrong"}
			},
			want: "invalid client credentials",
		},
		{
			name:       "warehouse unreachable",
			realDriver: true,
			setup: func(t *testing.T, _ *fakeWorkspace, _ *fakeSQL, cfg *Config) {
				cfg.Port = closedPort(t)
			},
			want: "SQL warehouse unreachable",
		},
		{
			name:       "warehouse not found",
			realDriver: true,
			setup: func(_ *testing.T, ws *fakeWorkspace, _ *fakeSQL, _ *Config) {
				ws.sqlStatus = http.StatusNotFound
			},
			want: `SQL warehouse not found at http_path "/sql/1.0/warehouses/abc123"`,
		},
		{
			name:  "catalog missing",
			setup: func(_ *testing.T, _ *fakeWorkspace, sql *fakeSQL, _ *Config) { sql.missing["catalog"] = true },
			want:  `catalog "main" does not exist`,
		},
		{
			name:  "schema missing",
			setup: func(_ *testing.T, _ *fakeWorkspace, sql *fakeSQL, _ *Config) { sql.missing["schema"] = true },
			want:  `schema "main.raw" does not exist`,
		},
		{
			name:  "volume missing",
			setup: func(_ *testing.T, _ *fakeWorkspace, sql *fakeSQL, _ *Config) { sql.missing["volume"] = true },
			want:  `volume "main.raw.stg" does not exist`,
		},
		{
			name: "no write privilege on the volume",
			setup: func(_ *testing.T, ws *fakeWorkspace, _ *fakeSQL, _ *Config) {
				ws.filesStatus = func(method, _ string) int {
					if method == http.MethodPut {
						return http.StatusForbidden
					}
					return 0
				}
			},
			want: "no write privilege on volume /Volumes/main/raw/stg",
		},
		{
			name: "no privilege to create tables",
			setup: func(_ *testing.T, _ *fakeWorkspace, sql *fakeSQL, _ *Config) {
				sql.fail = func(stmt string) error {
					if strings.HasPrefix(stmt, "CREATE TABLE") {
						return errDenied
					}
					return nil
				}
			},
			want: "no privilege to create tables in schema main.raw",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ws := newWorkspace(t)
			sql := newFakeSQL(ws)
			cfg := ws.config(t)
			tt.setup(t, ws, sql, &cfg)
			d := ws.destination(sql)
			if tt.realDriver {
				d = ws.destination(nil)
			}
			err := d.Check(t.Context(), cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
			assert.Zero(t, ws.fileCount(), "probe files are removed")
		})
	}
}

// TestCheckSucceeds probes the volume and the schema and leaves nothing
// behind.
func TestCheckSucceeds(t *testing.T) {
	for _, auth := range []Auth{
		{AuthType: authPAT, PersonalAccessToken: testPAT},
		{AuthType: authOAuth, ClientID: testClientID, Secret: testClientSecret},
	} {
		t.Run(auth.AuthType, func(t *testing.T) {
			ws := newWorkspace(t)
			sql := newFakeSQL(ws)
			cfg := ws.config(t)
			cfg.Authentication = auth
			require.NoError(t, ws.destination(sql).Check(t.Context(), cfg))

			ops := ws.opsList()
			require.Len(t, ops, 2)
			assert.Regexp(t, `^PUT /Volumes/main/raw/stg/_synclet_check_[0-9a-f-]{36}$`, ops[0])
			assert.Equal(t, "DELETE"+strings.TrimPrefix(ops[0], "PUT"), ops[1])
			created := sql.matching("CREATE TABLE")
			require.Len(t, created, 1)
			assert.Regexp(t, "^CREATE TABLE `main`.`raw`.`_synclet_check_[0-9a-f]{32}` \\(id INT, v VARIANT\\) USING DELTA$", created[0])
			dropped := sql.matching("DROP TABLE")
			require.Len(t, dropped, 1)
			assert.Equal(t, "DROP TABLE IF EXISTS "+strings.TrimSuffix(strings.TrimPrefix(created[0], "CREATE TABLE "), " (id INT, v VARIANT) USING DELTA"), dropped[0])
			assert.True(t, sql.closed)
		})
	}
}

func TestCheckRejectsInvalidConfig(t *testing.T) {
	err := NewDestination().Check(t.Context(), Config{Hostname: "https://x/y", Volume: "a.b", Authentication: Auth{AuthType: authOAuth}})
	require.Error(t, err)
	for _, want := range []string{"must be a bare host name", "http_path is required", "database is required", `volume "a.b" contains a period`, "OAUTH needs client_id and secret"} {
		assert.Contains(t, err.Error(), want)
	}
}
