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

func runCheck(t *testing.T, src Source, cfg Config) (*synclet.ConnectionStatus, *testkit.Output) {
	t.Helper()
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandCheck, Config: raw})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.NotNil(t, out.ConnectionStatus())
	return out.ConnectionStatus(), out
}

func scopesBody(handles ...string) string {
	body := `{"data":{"shop":{"name":"Example Store"},"currentAppInstallation":{"accessScopes":[`
	for i, h := range handles {
		if i > 0 {
			body += ","
		}
		body += `{"handle":"` + h + `"}`
	}
	return body + `]}}}`
}

func TestCheck(t *testing.T) {
	token := Config{Shop: testShop, AccessToken: testToken}
	grant := Config{Shop: testShop, ClientID: testClientID, ClientSecret: testClientSecret}
	tests := []struct {
		name      string
		cfg       Config
		overrides map[string]http.HandlerFunc
		want      []string
	}{
		{name: "valid token", cfg: token},
		{name: "valid client credentials", cfg: grant},
		{name: "write scopes grant read", cfg: token, overrides: map[string]http.HandlerFunc{
			"CheckShop": jsonBody(http.StatusOK, scopesBody("write_products", "read_customers", "write_orders", "read_all_orders", "write_inventory", "read_locations")),
		}},
		{
			name: "invalid access token", cfg: Config{Shop: testShop, AccessToken: "wrong-admin-token"},
			want: []string{"access token is invalid or revoked (HTTP 401): [API] Invalid API key or access token"},
		},
		{
			name: "invalid client secret", cfg: Config{Shop: testShop, ClientID: testClientID, ClientSecret: "wrong-secret"},
			want: []string{"client_id or client_secret is invalid, or the app is not installed on example.myshopify.com", "invalid_client: Client authentication failed"},
		},
		{
			name: "wrong shop domain", cfg: Config{Shop: "missing-shop.myshopify.com", AccessToken: testToken},
			want: []string{"shop missing-shop.myshopify.com not found; use the shop's myshopify.com domain"},
		},
		{
			name: "wrong shop domain with client credentials", cfg: Config{Shop: "missing-shop", ClientID: testClientID, ClientSecret: testClientSecret},
			want: []string{"shop missing-shop.myshopify.com not found; use the shop's myshopify.com domain"},
		},
		{
			name: "custom domain", cfg: Config{Shop: "shop.example.com", AccessToken: testToken},
			want: []string{`shop "shop.example.com" must be the store name or its myshopify.com domain`},
		},
		{
			name: "missing access scopes", cfg: token,
			overrides: map[string]http.HandlerFunc{
				"CheckShop": jsonBody(http.StatusOK, scopesBody("read_products", "read_customers", "read_inventory")),
			},
			want: []string{
				"missing access scope read_orders (orders, refunds, fulfillments, transactions)",
				"missing access scope read_locations (inventory_levels, locations)",
			},
		},
		{
			name: "frozen shop", cfg: token,
			overrides: map[string]http.HandlerFunc{"CheckShop": respond(t, http.StatusPaymentRequired, "errors/payment_required.json")},
			want:      []string{"shop example.myshopify.com is frozen (unpaid)"},
		},
		{
			name: "locked shop", cfg: token,
			overrides: map[string]http.HandlerFunc{"CheckShop": respond(t, http.StatusLocked, "errors/locked.json")},
			want:      []string{"shop example.myshopify.com is locked"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, tt.overrides)
			st, _ := runCheck(t, testSource(srv), tt.cfg)
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

// TestCheckAllOrders passes without read_all_orders and warns that only
// 60 days of orders are read.
func TestCheckAllOrders(t *testing.T) {
	srv, _ := fixtureServer(t, map[string]http.HandlerFunc{
		"CheckShop": jsonBody(http.StatusOK, scopesBody("read_products", "read_customers", "read_orders", "read_inventory", "read_locations")),
	})
	st, out := runCheck(t, testSource(srv), Config{Shop: testShop, AccessToken: testToken})
	assert.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
	var warned bool
	for _, m := range out.Messages {
		if m.Type == synclet.MessageTypeLog && m.Log.Level == synclet.LogLevelWarn {
			assert.Contains(t, m.Log.Message, "read_all_orders")
			warned = true
		}
	}
	assert.True(t, warned, "no WARN about read_all_orders")
}

func TestCheckConfig(t *testing.T) {
	tests := []struct {
		cfg  Config
		want []string
	}{
		{Config{StartDate: "yesterday", WindowDays: 400}, []string{
			"shop is required", "access_token, or client_id and client_secret, is required",
			`start_date "yesterday"`, "window_days 400 is outside 1-365",
		}},
		{Config{Shop: "example", AccessToken: testToken, ClientID: testClientID, ClientSecret: testClientSecret},
			[]string{"set either access_token or client_id and client_secret, not both"}},
		{Config{Shop: "https://example.myshopify.com", ClientID: testClientID},
			[]string{"custom domains and URLs are not accepted", "client_id and client_secret are both required"}},
	}
	for _, tt := range tests {
		st, _ := runCheck(t, Source{}, tt.cfg)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		for _, w := range tt.want {
			assert.Contains(t, st.Message, w)
		}
	}
}
