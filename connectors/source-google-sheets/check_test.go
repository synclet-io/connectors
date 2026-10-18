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

func runCheck(t *testing.T, src Source, cfg json.RawMessage) (*synclet.ConnectionStatus, *testkit.Output) {
	t.Helper()
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: cfg})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	return status, out
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name      string
		edit      func(*Config)
		overrides map[string][]fixture
		// message is the start of the failure message; empty means success.
		message  string
		requests []string
		warning  string
	}{
		{
			name:     "service account succeeds",
			requests: []string{"POST token", spreadsheetKey(testSheetA)},
		},
		{name: "oauth succeeds", edit: withOAuth},
		{
			name:     "additional spreadsheets are read too",
			edit:     func(c *Config) { c.AdditionalSpreadsheetIDs = []string{testSheetB} },
			requests: []string{"POST token", spreadsheetKey(testSheetA), spreadsheetKey(testSheetB)},
		},
		{
			name:     "incremental reads modifiedTime",
			edit:     withIncremental,
			requests: []string{"POST token", spreadsheetKey(testSheetA), driveKey(testSheetA)},
		},
		{
			name:      "incremental without Drive access warns",
			edit:      withIncremental,
			overrides: map[string][]fixture{driveKey(testSheetA): {{status: http.StatusForbidden, file: "errors/drive_scope_insufficient"}}},
			warning:   "spreadsheet " + testSheetA + ": Google Drive does not return its modifiedTime, which incremental sync needs (grant the https://www.googleapis.com/auth/drive.metadata.readonly scope and access to the file): the credentials lack the https://www.googleapis.com/auth/drive.metadata.readonly scope",
		},
		{
			name:     "revoked service account key",
			edit:     func(c *Config) { c.Credentials.ServiceAccountInfo = serviceAccountKey(t, otherKey()) },
			message:  "service account key is invalid, disabled or deleted (token endpoint: HTTP 400 invalid_grant: Invalid JWT Signature.)",
			requests: []string{"POST token"},
		},
		{
			name: "malformed service account key",
			edit: func(c *Config) {
				c.Credentials.ServiceAccountInfo = `{"type":"service_account","client_email":"x@example.com","private_key":"nope"}`
			},
			message:  "service_account_info is not a valid service account key: private_key is not PEM encoded",
			requests: []string{},
		},
		{
			name:     "service_account_info is not JSON",
			edit:     func(c *Config) { c.Credentials.ServiceAccountInfo = "not json" },
			message:  "service_account_info is not a Google credential JSON",
			requests: []string{},
		},
		{
			name:     "revoked refresh token",
			edit:     func(c *Config) { withOAuth(c); c.Credentials.RefreshToken = "revoked-refresh-token" },
			message:  "refresh_token is invalid or revoked (token endpoint: HTTP 400 invalid_grant: Token has been expired or revoked.)",
			requests: []string{"POST token"},
		},
		{
			name:    "wrong client secret",
			edit:    func(c *Config) { withOAuth(c); c.Credentials.ClientSecret = "wrong-secret" },
			message: "client_id or client_secret is invalid",
		},
		{
			name:      "missing scope",
			overrides: map[string][]fixture{spreadsheetKey(testSheetA): {{status: http.StatusForbidden, file: "errors/scope_insufficient"}}},
			message:   "the credentials lack the https://www.googleapis.com/auth/spreadsheets.readonly scope: authorize them with it",
		},
		{
			name:      "spreadsheet not shared",
			overrides: map[string][]fixture{spreadsheetKey(testSheetA): {{status: http.StatusForbidden, file: "errors/permission_denied"}}},
			message:   "spreadsheet " + testSheetA + " is not shared with service account " + testClientEmail + ": share it with that account",
			requests:  []string{"POST token", spreadsheetKey(testSheetA)},
		},
		{
			name:      "spreadsheet not shared with the OAuth user",
			edit:      withOAuth,
			overrides: map[string][]fixture{spreadsheetKey(testSheetA): {{status: http.StatusForbidden, file: "errors/permission_denied"}}},
			message:   "spreadsheet " + testSheetA + " is not shared with the Google user of the refresh token",
		},
		{
			name:    "spreadsheet not found",
			edit:    func(c *Config) { c.SpreadsheetID = "1MissingSpreadsheet" },
			message: "spreadsheet 1MissingSpreadsheet not found: check the spreadsheet ID",
		},
		{
			name: "additional spreadsheet not found",
			edit: func(c *Config) {
				c.AdditionalSpreadsheetIDs = []string{"https://docs.google.com/spreadsheets/d/1MissingSpreadsheet/edit"}
			},
			message: "spreadsheet 1MissingSpreadsheet not found",
		},
		{
			name:      "Sheets API disabled",
			overrides: map[string][]fixture{spreadsheetKey(testSheetA): {{status: http.StatusForbidden, file: "errors/service_disabled"}}},
			message:   "Google Sheets API is disabled in project 123456789012: enable sheets.googleapis.com in that Google Cloud project",
		},
		{
			name:     "unknown auth_type",
			edit:     func(c *Config) { c.Credentials.AuthType = "Magic" },
			message:  `unsupported auth_type: "Magic"`,
			requests: []string{},
		},
		{
			name:     "invalid column_naming",
			edit:     func(c *Config) { c.ColumnNaming = "fancy" },
			message:  `column_naming must be legacy or stable, got "fancy"`,
			requests: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, tt.overrides)
			src, _ := testSource(srv)
			status, out := runCheck(t, src, testConfig(t, tt.edit))
			if tt.message == "" {
				assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
			} else {
				assert.Equal(t, synclet.ConnectionStatusFailed, status.Status)
				assert.Contains(t, status.Message, tt.message)
			}
			if tt.requests != nil {
				assert.Equal(t, tt.requests, append([]string{}, h.keys()...))
			}
			if tt.warning != "" {
				assert.Len(t, warnings(out), 1)
				for _, w := range warnings(out) {
					assert.Contains(t, w, tt.warning)
				}
			} else {
				assert.Empty(t, warnings(out))
			}
		})
	}
}

func TestCheckRefreshesExpiredToken(t *testing.T) {
	srv, h := fixtureServer(t, map[string][]fixture{
		spreadsheetKey(testSheetA): {{status: http.StatusUnauthorized, file: "errors/unauthenticated"}, {}},
	})
	src, _ := testSource(srv)
	status, _ := runCheck(t, src, testConfig(t, nil))
	assert.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)
	assert.Equal(t, []string{"POST token", spreadsheetKey(testSheetA), "POST token", spreadsheetKey(testSheetA)}, h.keys())
}
