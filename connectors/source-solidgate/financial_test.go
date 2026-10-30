package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestFinancialEntriesRead(t *testing.T) {
	srv, h := fixtureServer(t, map[string][]fixture{
		"poll 2026-10-16": {{status: http.StatusNoContent}, {status: http.StatusNoContent}, {status: http.StatusFound, location: "/files/2026-10-16.csv"}},
	})
	src := testSource(srv)
	out := runRead(t, src, configured(t, src, synclet.SyncModeIncremental, streamFinancialEntries), nil)

	assert.Equal(t, []string{
		"finance/financial_entries 2026-10-16", "poll 2026-10-16", "poll 2026-10-16", "poll 2026-10-16", "file 2026-10-16",
		"finance/financial_entries 2026-10-17", "poll 2026-10-17", "file 2026-10-17",
		"finance/financial_entries 2026-10-18", "poll 2026-10-18", "file 2026-10-18",
	}, h.list())
	assert.Equal(t, reportRequest{DateFrom: "2026-10-16 00:00:00", DateTo: "2026-10-17 00:00:00", Filter: "created_at"}, h.requests()[0])

	recs := out.Records(synclet.StreamDescriptor{Name: streamFinancialEntries})
	require.Len(t, recs, 3)
	assert.JSONEq(t, `{
		"id":"fe-0001","solid_account":"acme_test","order_id":"ord-0001","external_psp_order_id":null,
		"transaction_id":"txn-0001","chargeback_id":null,"order_description":"Premium plan",
		"created_at":"2026-10-16T10:00:00","transaction_datetime_provider":"2026-10-16T10:00:00",
		"transaction_datetime_utc":"2026-10-16T10:00:00+00:00","accounting_date":"2026-10-16",
		"amount":"1000","amount_in_major_units":"10.00","currency":"USD","currency_minor_units":"2",
		"payout_amount":"970","payout_amount_in_major_units":"9.70","payout_currency":"USD",
		"payout_currency_minor_units":"2","record_type_key":"sale","provider":"acquirer-test",
		"payment_method":"card","card_brand":"VISA","geo_country":"USA","issuing_country":"USA",
		"legal_entity":"Example Ltd","product_id":"prod-0001","product_name":"Premium monthly"
	}`, string(recs[0].Data))
	states := out.States()
	require.Len(t, states, 3)
	assert.JSONEq(t, `{"accounts":{"acme_test":{"day":"2026-10-16"}}}`, string(states[0].Stream.StreamState))
	assert.JSONEq(t, `{"accounts":{"acme_test":{"day":"2026-10-18"}}}`, string(states[2].Stream.StreamState))
}

func TestFinancialEntriesReportErrors(t *testing.T) {
	tests := []struct {
		name      string
		overrides map[string][]fixture
		message   string
	}{
		{
			name:      "404",
			overrides: map[string][]fixture{"poll 2026-10-16": {{status: http.StatusNotFound}}},
			message:   "financial entries report not found (HTTP 404)",
		},
		{
			name:      "410",
			overrides: map[string][]fixture{"poll 2026-10-16": {{status: http.StatusNoContent}, {status: http.StatusGone}}},
			message:   "financial entries report expired (HTTP 410)",
		},
		{
			name:      "never ready",
			overrides: map[string][]fixture{"poll 2026-10-16": {{status: http.StatusNoContent}}},
			message:   "financial entries report was not ready after 1s",
		},
		{
			name: "report_url on another host",
			overrides: map[string][]fixture{"finance/financial_entries 2026-10-16": {{
				status: http.StatusOK, body: `{"report_url":"https://files.example.com/report/1"}`,
			}}},
			message: "financial entries report: report_url is not on the reports API host",
		},
		{
			name: "malformed date",
			overrides: map[string][]fixture{"file 2026-10-16": {{
				status: http.StatusOK, file: "financial_entries/bad_date.csv",
			}}},
			message: `financial_entries: account "acme_test", day 2026-10-16: financial entries report: entry fe-0005: field created_at: value does not match YYYY-MM-DD HH:MM:SS`,
		},
		{
			name: "row shorter than header",
			overrides: map[string][]fixture{"file 2026-10-16": {{
				status: http.StatusOK, file: "financial_entries/short_row.csv",
			}}},
			message: "wrong number of fields",
		},
		{
			name:      "file download fails",
			overrides: map[string][]fixture{"file 2026-10-16": {{status: http.StatusForbidden}}},
			message:   "financial entries report: file download failed (HTTP 403)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, _ := fixtureServer(t, tt.overrides)
			src := testSource(srv)
			out := runReadConfig(t, src, testConfig, configured(t, src, synclet.SyncModeIncremental, streamFinancialEntries), savedDay(streamFinancialEntries, "2026-10-17"))
			require.Error(t, out.Err)
			assert.Contains(t, out.Err.Error(), tt.message)
			assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: streamFinancialEntries}), "no record of a day whose file is not parsed whole")
			assert.Empty(t, out.States(), "the day is not marked handled")
		})
	}
}
