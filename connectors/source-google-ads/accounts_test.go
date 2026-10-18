package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// streamSearches returns, per customer, the first-page searches whose query
// is the query of stream d, with or without a date range.
func streamSearches(h *hits, d streamDef) map[string][]apiRequest {
	out := map[string][]apiRequest{}
	for _, r := range h.list() {
		if r.pageToken != "" {
			continue
		}
		if r.query != d.query.text && (!d.incremental() || !strings.HasPrefix(r.query, d.query.text+" WHERE ")) {
			continue
		}
		cid := r.path[len("customers/") : len(r.path)-len("/googleAds:search")]
		out[cid] = append(out[cid], r)
	}
	return out
}

func TestManagerAccounts(t *testing.T) {
	var all []string
	for _, d := range builtinDefs {
		all = append(all, d.name)
	}

	t.Run("every stream reads each enabled client of the manager", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, nil)
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, all...), nil)
		require.NoError(t, out.Err)

		clients := records(t, out, streamCustomerClient)
		var ids []any
		for _, c := range clients {
			assert.Equal(t, float64(1111111111), c["customer_id"])
			ids = append(ids, c["customer_client_id"])
		}
		assert.Equal(t, []any{float64(1111111111), float64(1234567890), float64(2345678901), float64(3333333333), float64(4444444444)}, ids,
			"customer_client lists the manager's accounts")

		for _, d := range builtinDefs {
			byCustomer := streamSearches(h, d)
			if d.name == streamCustomerClient {
				assert.Len(t, byCustomer, 1, d.name)
				assert.Len(t, byCustomer[testManager], 1, d.name)
				continue
			}
			assert.Len(t, byCustomer, 2, "%s reads the two enabled non-manager clients only", d.name)
			for _, cid := range []string{testClientA, testClientB} {
				if assert.Len(t, byCustomer[cid], 1, "%s %s", d.name, cid) {
					assert.Equal(t, testManager, byCustomer[cid][0].login, "%s %s", d.name, cid)
				}
			}
		}
		assert.Len(t, records(t, out, streamCustomer), 2)
		assert.Len(t, records(t, out, streamCampaignPerformance), 5, "3 rows of A in range, 2 of B")
	})

	t.Run("child_customer_ids narrows the clients", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, func(c *Config) { c.ChildCustomerIDs = []string{"234-567-8901"} })
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, streamCampaign, streamAdGroup), nil)
		require.NoError(t, out.Err)
		for _, name := range []string{streamCampaign, streamAdGroup} {
			d, _ := builtinByName(name)
			byCustomer := streamSearches(h, d)
			assert.Len(t, byCustomer, 1, name)
			assert.Len(t, byCustomer[testClientB], 1, name)
		}
		assert.Len(t, records(t, out, streamCampaign), 1)
	})

	t.Run("login_customer_id replaces the manager in the header", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, func(c *Config) { c.LoginCustomerID = "9999999999" })
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, streamAdGroup), nil)
		require.NoError(t, out.Err)
		d, _ := builtinByName(streamAdGroup)
		for cid, reqs := range streamSearches(h, d) {
			assert.Equal(t, "9999999999", reqs[0].login, cid)
		}
	})

	t.Run("a client account is read directly", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, func(c *Config) { c.CustomerIDs = []string{testClientA} })
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, streamAdGroup), nil)
		require.NoError(t, out.Err)
		d, _ := builtinByName(streamAdGroup)
		byCustomer := streamSearches(h, d)
		assert.Len(t, byCustomer, 1)
		require.Len(t, byCustomer[testClientA], 1)
		assert.Empty(t, byCustomer[testClientA][0].login)
	})

	t.Run("accounts are listed once per read", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src, _ := testSource(srv)
		cfg := testConfig(t, nil)
		out := runRead(t, src, cfg, configured(t, src, cfg, synclet.SyncModeFullRefresh, streamCampaign, streamAdGroup), nil)
		require.NoError(t, out.Err)
		assert.Len(t, h.searches("customer_client"), 1)
	})
}
