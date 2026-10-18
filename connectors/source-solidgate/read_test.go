package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func savedCursor(stream, cursor string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + stream + `"},"stream_state":{"accounts":{"acme_test":{"cursor":"` + cursor + `"}}}}}]`)
}

func runReadConfig(t *testing.T, src Source, config string, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	raw, err := json.Marshal(catalog)
	require.NoError(t, err)
	out, err := testkit.RunSource(t.Context(), src, synclet.Request{
		Command: synclet.CommandRead, Config: json.RawMessage(config), Catalog: raw, State: state,
	})
	require.NoError(t, err)
	return out
}

func runRead(t *testing.T, src Source, catalog *synclet.ConfiguredCatalog, state json.RawMessage) *testkit.Output {
	t.Helper()
	out := runReadConfig(t, src, testConfig, catalog, state)
	require.NoError(t, out.Err)
	return out
}

func recordField(t *testing.T, out *testkit.Output, stream, name string) []string {
	t.Helper()
	var vals []string
	for _, r := range out.Records(synclet.StreamDescriptor{Name: stream}) {
		var rec map[string]any
		require.NoError(t, json.Unmarshal(r.Data, &rec))
		s, _ := rec[name].(string)
		vals = append(vals, s)
	}
	return vals
}

func TestReadResumeFromCursorMinusLookback(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := `{"accounts":[{"name":"acme_test","public_key":"api_pk_test","secret_key":"api_sk_test"}],"start_date":"2026-10-01"}`
	out := runReadConfig(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), savedCursor(streamAPMOrders, "2026-10-17T07:45:00+00:00"))
	require.NoError(t, out.Err)

	assert.Equal(t, []string{"apm-orders 2026-10-15", "apm-orders 2026-10-16", "apm-orders 2026-10-17", "apm-orders 2026-10-18"}, h.list())
	states := out.States()
	require.Len(t, states, 4)
	assert.JSONEq(t, `{"accounts":{"acme_test":{"cursor":"2026-10-17T07:45:00+00:00"}}}`, string(states[3].Stream.StreamState))
}

func TestReadResumeLookbackOption(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := `{"accounts":[{"name":"acme_test","public_key":"api_pk_test","secret_key":"api_sk_test"}],"start_date":"2026-10-01","lookback_days":0}`
	out := runReadConfig(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), savedCursor(streamAPMOrders, "2026-10-17T07:45:00+00:00"))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"apm-orders 2026-10-17", "apm-orders 2026-10-18"}, h.list())
}

func TestReadResumeNotBeforeStartDate(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	runRead(t, src, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), savedCursor(streamAPMOrders, "2026-10-16T10:15:00+00:00"))
	assert.Equal(t, []string{"apm-orders 2026-10-16", "apm-orders 2026-10-17", "apm-orders 2026-10-18"}, h.list())
}

func TestReadWindowIsOneUTCDay(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	runRead(t, src, configured(t, src, synclet.SyncModeFullRefresh, streamPayPalDisputes), nil)

	assert.Equal(t, []reportRequest{
		{DateFrom: "2026-10-16 00:00:00", DateTo: "2026-10-17 00:00:00", Filter: "updated_at", Limit: reportLimit},
		{DateFrom: "2026-10-17 00:00:00", DateTo: "2026-10-18 00:00:00", Filter: "updated_at", Limit: reportLimit},
		{DateFrom: "2026-10-18 00:00:00", DateTo: "2026-10-19 00:00:00", Filter: "updated_at", Limit: reportLimit},
	}, h.requests())
}

func TestReadPagination(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, configured(t, src, synclet.SyncModeIncremental, streamCardOrders), nil)

	assert.Equal(t, []string{
		"card-orders 2026-10-16", "card-orders 2026-10-16 page-2", "card-orders 2026-10-16 page-3",
		"card-orders 2026-10-17", "card-orders 2026-10-18",
	}, h.list())
	assert.Equal(t, []string{"ord-0001", "ord-0002", "ord-0003", "ord-0004", "ord-0005"}, recordField(t, out, streamCardOrders, "id"))
	reqs := h.requests()
	assert.Equal(t, "page-2", reqs[1].NextPageIterator)
	assert.Equal(t, "2026-10-16 00:00:00", reqs[2].DateFrom)
	states := out.States()
	require.Len(t, states, 3)
	assert.JSONEq(t, `{"accounts":{"acme_test":{"cursor":"2026-10-16T13:00:00+00:00"}}}`, string(states[0].Stream.StreamState))
	assert.JSONEq(t, `{"accounts":{"acme_test":{"cursor":"2026-10-17T08:30:00+00:00"}}}`, string(states[2].Stream.StreamState))
}

func TestReadRecords(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, configured(t, src, synclet.SyncModeFullRefresh,
		streamSubscriptions, streamSubscriptionInvoices, streamChargebacks, streamCardFraudAlerts, streamProducts, streamCardOrders), nil)

	assert.Equal(t, []string{"sub-0002", "sub-0001", "sub-0003"}, recordField(t, out, streamSubscriptions, "id"))
	sub := out.Records(synclet.StreamDescriptor{Name: streamSubscriptions})[0]
	assert.JSONEq(t, `{
		"id":"sub-0002","solid_account":"acme_test","status":"active","updated_at":"2026-10-16T12:00:00+00:00",
		"started_at":"2026-10-16T09:00:00+00:00","expired_at":"2026-11-16T09:00:00+00:00","next_charge_at":"2026-11-16T09:00:00+00:00",
		"payment_type":"card","trial":false,"cancelled_at":null,"cancel_code":null,"cancel_message":null,
		"customer_account_id":"cust-0002","customer_email":"customer2@example.com","product_id":"prod-0001",
		"product_name":"Premium monthly","product_payment_action":"auth_settle","product_amount":9.99,
		"product_currency":"USD","product_trial":true,"product_trial_period":7,"pause_from_date":null,"pause_to_date":null
	}`, string(sub.Data))

	assert.Equal(t, []string{"inv-0002-b", "inv-0002-a", "inv-0001-b", "inv-0001-a", "inv-0003-b", "inv-0003-a"}, recordField(t, out, streamSubscriptionInvoices, "id"),
		"every invoice of a subscription is emitted, also one that changed before the window")
	inv := out.Records(synclet.StreamDescriptor{Name: streamSubscriptionInvoices})
	assert.JSONEq(t, `{
		"id":"inv-0002-b","solid_account":"acme_test","subscription_id":"sub-0002","status":"paid",
		"created_at":"2026-10-16T09:00:00+00:00","updated_at":"2026-10-16T12:00:00+00:00","amount":9.99,
		"billing_period_started_at":"2026-10-16T09:00:00+00:00","billing_period_ended_at":"2026-11-16T09:00:00+00:00",
		"subscription_term_number":2,
		"orders":[{"id":"ord-0002","status":"success","created_at":"2026-10-16 09:00:00","processed_at":"2026-10-16 09:00:05","amount":999,"operation":"recurring"}]
	}`, string(inv[0].Data))
	assert.Contains(t, string(inv[1].Data), `"orders":[]`)
	assert.Contains(t, string(inv[1].Data), `"updated_at":"2026-09-16T09:01:00+00:00"`)

	cb := out.Records(synclet.StreamDescriptor{Name: streamChargebacks})
	require.Len(t, cb, 2)
	assert.JSONEq(t, `{
		"id":"cb-0001","solid_account":"acme_test","order_id":"ord-0001","created_at":"2026-10-16T06:00:00+00:00",
		"updated_at":"2026-10-16T14:00:00+00:00","dispute_date":"2026-10-16T05:00:00+00:00","settlement_date":null,
		"amount":1000,"currency":"USD","type":"1st_chb","status":"won","reason_group":"fraud","reason_code":"10.4",
		"reason_description":"Other fraud","flows":[{"id":"flow-0001","type":"1st_chb","status":"won","amount":1000,"currency":"USD"}]
	}`, string(cb[0].Data))

	fa := out.Records(synclet.StreamDescriptor{Name: streamCardFraudAlerts})
	require.Len(t, fa, 2)
	assert.Contains(t, string(fa[0].Data), `"id":1`)

	prods := out.Records(synclet.StreamDescriptor{Name: streamProducts})
	require.Len(t, prods, 2)
	assert.JSONEq(t, `{
		"id":"prod-0001","solid_account":"acme_test","name":"Plan 1","type":"subscription","description":"Monthly access",
		"public_description":null,"status":"active","created_at":"2026-01-01T00:00:00+00:00","updated_at":"2026-01-02T00:00:00+00:00",
		"payment_action":"auth_settle","settle_interval":0,"tax_profile_id":"tax-profile-1","tax_category_id":"tax-cat-1","tax_is_included":false
	}`, string(prods[0].Data))
	assert.Contains(t, string(prods[1].Data), `"tax_profile_id":null`)

	order := out.Records(synclet.StreamDescriptor{Name: streamCardOrders})[0]
	var rec map[string]any
	require.NoError(t, json.Unmarshal(order.Data, &rec))
	assert.Equal(t, map[string]any{"cascade_steps": []any{map[string]any{"mid": "mid-test", "route_id": "route-1", "cascade_number": float64(1)}}}, rec["routing"])
	assert.Equal(t, map[string]any{"campaign": "test"}, rec["order_metadata"])
	assert.NotContains(t, rec, "chargebacks")
}

func TestReadProductsPaging(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	runRead(t, src, configured(t, src, synclet.SyncModeFullRefresh, streamProducts), nil)
	assert.Equal(t, []string{"products 0", "products 2"}, h.list())
}

func TestReadTagsEveryAccount(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := `{"accounts":[{"name":"acme_test","public_key":"api_pk_test","secret_key":"api_sk_test"},{"name":"acme_eu_test","public_key":"api_pk_test","secret_key":"api_sk_test"}],"start_date":"2026-10-17"}`
	out := runReadConfig(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), nil)
	require.NoError(t, out.Err)

	assert.Equal(t, []string{"acme_test", "acme_eu_test"}, recordField(t, out, streamAPMOrders, "solid_account"))
	assert.Len(t, h.list(), 4)
	states := out.States()
	assert.JSONEq(t, `{"accounts":{"acme_test":{"cursor":"2026-10-17T07:45:00+00:00"},"acme_eu_test":{"cursor":"2026-10-17T07:45:00+00:00"}}}`, string(states[len(states)-1].Stream.StreamState))
}

func TestReadFailsOnBadTimestamp(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{"apm-orders 2026-10-16": {{status: 200, body: `{"orders":[{"order_id":"apm-9","created_at":"16.10.2026","updated_at":"2026-10-16 10:00:00"}]}`}}})
	src := testSource(srv)
	out := runReadConfig(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), nil)
	require.ErrorContains(t, out.Err, `apm_orders: account "acme_test", day 2026-10-16: field created_at: value does not match YYYY-MM-DD HH:MM:SS`)
	assert.Empty(t, out.States())
}

func TestReadInvalidState(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	out := runReadConfig(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamCardOrders), savedCursor(streamCardOrders, "yesterday"))
	require.ErrorContains(t, out.Err, `state of card_orders: cursor of account "acme_test" is not a YYYY-MM-DDTHH:MM:SS timestamp`)
}
