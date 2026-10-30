package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func savedDay(stream, day string) json.RawMessage {
	return json.RawMessage(`[{"type":"STREAM","stream":{"stream_descriptor":{"name":"` + stream + `"},"stream_state":{"accounts":{"acme_test":{"day":"` + day + `"}}}}}]`)
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

func decodeRecord(t *testing.T, data json.RawMessage) map[string]any {
	t.Helper()
	var rec map[string]any
	require.NoError(t, json.Unmarshal(data, &rec))
	return rec
}

func TestReadResumeFromDayMinusLookback(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := `{"accounts":[{"name":"acme_test","public_key":"api_pk_test","secret_key":"api_sk_test"}],"start_date":"2026-10-01"}`
	out := runReadConfig(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), savedDay(streamAPMOrders, "2026-10-17"))
	require.NoError(t, out.Err)

	assert.Equal(t, []string{"apm-orders 2026-10-15", "apm-orders 2026-10-16", "apm-orders 2026-10-17", "apm-orders 2026-10-18"}, h.list())
	states := out.States()
	require.Len(t, states, 4)
	assert.JSONEq(t, `{"accounts":{"acme_test":{"day":"2026-10-18"}}}`, string(states[3].Stream.StreamState))
}

func TestReadResumeLookbackOption(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := `{"accounts":[{"name":"acme_test","public_key":"api_pk_test","secret_key":"api_sk_test"}],"start_date":"2026-10-01","lookback_days":0}`
	out := runReadConfig(t, src, cfg, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), savedDay(streamAPMOrders, "2026-10-17"))
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"apm-orders 2026-10-17", "apm-orders 2026-10-18"}, h.list())
}

func TestReadResumeNotBeforeStartDate(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	runRead(t, src, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), savedDay(streamAPMOrders, "2026-10-16"))
	assert.Equal(t, []string{"apm-orders 2026-10-16", "apm-orders 2026-10-17", "apm-orders 2026-10-18"}, h.list())
}

// TestReadSavesDaysWithoutRecords: a day the report has no record for is
// saved too, so the next read does not request it again; the only records
// are from 2026-10-16 and 2026-10-17.
func TestReadSavesDaysWithoutRecords(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := `{"accounts":[{"name":"acme_test","public_key":"api_pk_test","secret_key":"api_sk_test"}],"start_date":"2026-10-01"}`
	cat := configured(t, src, synclet.SyncModeIncremental, streamAPMOrders)
	out := runReadConfig(t, src, cfg, cat, nil)
	require.NoError(t, out.Err)

	states := out.States()
	require.Len(t, states, 18)
	assert.JSONEq(t, `{"accounts":{"acme_test":{"day":"2026-10-01"}}}`, string(states[0].Stream.StreamState))
	last, err := json.Marshal([]synclet.StateMessage{states[17]})
	require.NoError(t, err)

	srv, h := fixtureServer(t, nil)
	src = testSource(srv)
	require.NoError(t, runReadConfig(t, src, cfg, cat, last).Err)
	assert.Equal(t, []string{"apm-orders 2026-10-16", "apm-orders 2026-10-17", "apm-orders 2026-10-18"}, h.list())
}

// TestReadFailedDayNotSaved: the state stays at the last day read whole, so
// the next read requests the failed day again.
func TestReadFailedDayNotSaved(t *testing.T) {
	srv, _ := fixtureServer(t, map[string][]fixture{
		"apm-orders 2026-10-17": {{status: http.StatusBadRequest, body: `{"error":{"code":"2.01","messages":["Invalid request"]}}`}},
	})
	src := testSource(srv)
	out := runReadConfig(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamAPMOrders), nil)
	require.ErrorContains(t, out.Err, `apm_orders: account "acme_test", day 2026-10-17`)

	states := out.States()
	require.Len(t, states, 1)
	assert.JSONEq(t, `{"accounts":{"acme_test":{"day":"2026-10-16"}}}`, string(states[0].Stream.StreamState))
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
	assert.JSONEq(t, `{"accounts":{"acme_test":{"day":"2026-10-16"}}}`, string(states[0].Stream.StreamState))
	assert.JSONEq(t, `{"accounts":{"acme_test":{"day":"2026-10-18"}}}`, string(states[2].Stream.StreamState))
}

func TestReadRecords(t *testing.T) {
	srv, _ := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, configured(t, src, synclet.SyncModeFullRefresh,
		streamSubscriptions, streamSubscriptionInvoices, streamChargebacks, streamCardFraudAlerts, streamProducts, streamCardOrders,
		streamAPMOrders, streamPayPalDisputes), nil)

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
		"term_length":12,"payment_action":"auth_settle","settle_interval":48,"billing_period":{"unit":"month","value":1},
		"trial":{"billing_period":{"unit":"day","value":7},"payment_action":"auth_void","settle_interval":0},
		"retry_mode":"smart","retry_strategy_id":"retry-strategy-1",
		"tax_profile_id":null,"tax_category_id":"tax-cat-1","tax_is_included":null,"tax":{"category_id":"tax-cat-1","behavior":"excluded"},
		"metadata":{"tier":"premium"}
	}`, string(prods[0].Data))
	// A product with the undocumented tax fields profile_id and is_included
	// keeps them in the flat tax_* fields.
	prod := decodeRecord(t, prods[1].Data)
	assert.Equal(t, "tax-profile-1", prod["tax_profile_id"])
	assert.Equal(t, "tax-cat-2", prod["tax_category_id"])
	assert.Equal(t, false, prod["tax_is_included"])

	order := out.Records(synclet.StreamDescriptor{Name: streamCardOrders})[0]
	var rec map[string]any
	require.NoError(t, json.Unmarshal(order.Data, &rec))
	assert.Equal(t, map[string]any{"cascade_steps": []any{map[string]any{"mid": "mid-test", "route_id": "route-1", "cascade_number": float64(1)}}}, rec["routing"])
	assert.Equal(t, map[string]any{"campaign": "test"}, rec["order_metadata"])
	assert.NotContains(t, rec, "chargebacks")
	for name, want := range map[string]any{
		"product_id": "prod-0001", "product_name": "Plan 1", "product_type": "recurring", "subscription_id": "sub-0001",
		"settled_amount": float64(1000), "settled_currency": "USD", "refunded_amount": float64(300), "refunded_currency": "USD",
		"refund": "partial", "payment_method": "apple-pay", "traffic_source": "facebook", "fraudulent": false,
	} {
		assert.Equal(t, want, rec[name], name)
	}

	apm := decodeRecord(t, out.Records(synclet.StreamDescriptor{Name: streamAPMOrders})[0].Data)
	for name, want := range map[string]any{
		"product_id": "prod-0002", "product_name": "Plan 2", "product_type": "one_time", "subscription_id": nil,
		"settled_amount": float64(500), "settled_currency": "EUR", "refunded_amount": nil, "refund": nil,
		"merchant_account_id": "merchant-paypal-1",
	} {
		assert.Equal(t, want, apm[name], name)
	}

	assert.Contains(t, string(out.Records(synclet.StreamDescriptor{Name: streamPayPalDisputes})[0].Data), `"represented_by":"merchant"`)
}

// TestReadMissingFieldsAreNull: a field Solidgate does not send is null in the
// record, never "", 0 or false.
func TestReadMissingFieldsAreNull(t *testing.T) {
	const day = "2026-10-16"
	body := func(s string) []fixture { return []fixture{{status: http.StatusOK, body: s}} }
	srv, _ := fixtureServer(t, map[string][]fixture{
		"subscriptions " + day: body(`{"subscriptions":{"sub-min":{"id":"sub-min","updated_at":"2026-10-16 10:00:00",` +
			`"invoices":{"inv-min":{"id":"inv-min","updated_at":"2026-10-16 10:00:00"}}}}}`),
		"card-orders " + day: body(`{"orders":[{"order_id":"ord-min","updated_at":"2026-10-16 10:00:00"}]}`),
		"apm-orders " + day:  body(`{"orders":[{"order_id":"apm-min","updated_at":"2026-10-16 10:00:00"}]}`),
		"card-orders/chargebacks " + day: body(`{"orders":[{"order_id":"ord-min",` +
			`"chargebacks":[{"id":"cb-min","updated_at":"2026-10-16 10:00:00"}]}]}`),
		"apm-orders/paypal-disputes " + day: body(`{"disputes":[{"dispute_id":"PP-D-min","updated_at":"2026-10-16 10:00:00"}]}`),
		"products 0":                        body(`{"data":[{"id":"prod-min"}]}`),
	})
	src := testSource(srv)
	out := runRead(t, src, configured(t, src, synclet.SyncModeFullRefresh, streamSubscriptions, streamSubscriptionInvoices,
		streamCardOrders, streamAPMOrders, streamChargebacks, streamPayPalDisputes, streamProducts), nil)

	const updated = "2026-10-16T10:00:00+00:00"
	sent := map[string]map[string]any{
		streamSubscriptions:        {"id": "sub-min", "updated_at": updated},
		streamSubscriptionInvoices: {"id": "inv-min", "updated_at": updated, "subscription_id": "sub-min", "orders": []any{}},
		streamCardOrders:           {"id": "ord-min", "updated_at": updated},
		streamAPMOrders:            {"id": "apm-min", "updated_at": updated},
		streamChargebacks:          {"id": "cb-min", "updated_at": updated, "order_id": "ord-min"},
		streamPayPalDisputes:       {"id": "PP-D-min", "updated_at": updated},
		streamProducts:             {"id": "prod-min"},
	}
	for stream, want := range sent {
		recs := out.Records(synclet.StreamDescriptor{Name: stream})
		require.NotEmpty(t, recs, stream)
		rec := decodeRecord(t, recs[0].Data)
		d, ok := streamDefByName(stream)
		require.True(t, ok, stream)
		assert.Len(t, rec, len(d.fields), stream)
		for name, v := range rec {
			if name == accountField {
				continue
			}
			if w, ok := want[name]; ok {
				assert.Equal(t, w, v, "%s.%s", stream, name)
			} else {
				assert.Nil(t, v, "%s.%s", stream, name)
			}
		}
	}
}

func TestReadProductsPaging(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	runRead(t, src, configured(t, src, synclet.SyncModeFullRefresh, streamProducts), nil)
	assert.Equal(t, []string{"products 0", "products 2"}, h.list())
}

func TestReadProductsStopsOnRepeatedPage(t *testing.T) {
	srv, h := fixtureServer(t, map[string][]fixture{"products 2": {{status: 200, file: "products/offset_0.json"}}})
	src := testSource(srv)
	out := runReadConfig(t, src, testConfig, configured(t, src, synclet.SyncModeFullRefresh, streamProducts), nil)
	require.ErrorContains(t, out.Err, `products: account "acme_test": the page at offset 2 repeats the previous page; the API ignores pagination[offset]`)
	assert.Equal(t, []string{"products 0", "products 2"}, h.list())
	assert.Len(t, out.Records(synclet.StreamDescriptor{Name: streamProducts}), 2, "the repeated page is not emitted")
}

func TestReadProductsPageCap(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"data":[{"id":"prod-` + r.URL.Query().Get("pagination[offset]") + `"}]}`))
	}))
	t.Cleanup(srv.Close)
	src := testSource(srv)
	products, ok := streamDefByName(streamProducts)
	require.True(t, ok)
	cat := &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
		Stream: products.stream(), SyncMode: synclet.SyncModeFullRefresh, DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
	}}}
	out := runReadConfig(t, src, testConfig, cat, nil)
	require.ErrorContains(t, out.Err, `products: account "acme_test": still listing after 1000 pages`)
	assert.Equal(t, int32(maxProductPages), calls.Load())
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
	assert.JSONEq(t, `{"accounts":{"acme_test":{"day":"2026-10-18"},"acme_eu_test":{"day":"2026-10-18"}}}`, string(states[len(states)-1].Stream.StreamState))
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
	out := runReadConfig(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamCardOrders), savedDay(streamCardOrders, "yesterday"))
	require.ErrorContains(t, out.Err, `state of card_orders: day of account "acme_test" is not a YYYY-MM-DD date`)
}
