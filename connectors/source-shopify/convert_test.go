package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func orderConverter(t *testing.T) converter {
	t.Helper()
	st, ok := findStream("orders")
	require.True(t, ok)
	c, err := newConverter(synclet.ObjectSchema(st.schema()))
	require.NoError(t, err)
	return c
}

func TestConvertMoney(t *testing.T) {
	c := orderConverter(t)
	obj := map[string]json.RawMessage{
		"id":            json.RawMessage(`"gid://shopify/Order/1"`),
		"totalPriceSet": json.RawMessage(`{"shopMoney":{"amount":"12345678901234567890.123456789","currencyCode":"USD"},"presentmentMoney":{"amount":"0.10","currencyCode":"EUR"}}`),
		"lineItems":     json.RawMessage(`[{"id":"gid://shopify/LineItem/1","quantity":2,"originalUnitPriceSet":{"shopMoney":{"amount":"-1.50","currencyCode":"USD"},"presentmentMoney":null}}]`),
		"note":          json.RawMessage(`"12.50"`),
		"__parentId":    json.RawMessage(`"x"`),
	}
	rec, err := c.record(obj)
	require.NoError(t, err)
	raw, err := json.Marshal(rec)
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"id":"gid://shopify/Order/1",
		"totalPriceSet":{"shopMoney":{"amount":12345678901234567890.123456789,"currencyCode":"USD"},"presentmentMoney":{"amount":0.10,"currencyCode":"EUR"}},
		"lineItems":[{"id":"gid://shopify/LineItem/1","quantity":2,"originalUnitPriceSet":{"shopMoney":{"amount":-1.50,"currencyCode":"USD"},"presentmentMoney":null}}],
		"note":"12.50"
	}`, string(raw))
	assert.Contains(t, string(raw), `"amount":12345678901234567890.123456789`, "the digits are kept, not rounded through a float")
	assert.Contains(t, string(raw), `"amount":0.10`)
}

func TestConvertBadNumber(t *testing.T) {
	c := orderConverter(t)
	for _, v := range []string{`"12,50"`, `"1e"`, `"NaN"`, `""`, `"0x10"`, `" 1"`} {
		_, err := c.record(map[string]json.RawMessage{
			"totalTaxSet": json.RawMessage(`{"shopMoney":{"amount":` + v + `,"currencyCode":"USD"}}`),
		})
		require.Error(t, err, v)
		assert.Contains(t, err.Error(), "totalTaxSet.shopMoney.amount: value")
		assert.Contains(t, err.Error(), "is not a decimal number")
	}
	_, err := c.record(map[string]json.RawMessage{"lineItems": json.RawMessage(`[{"quantity":"two"}]`)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `lineItems[0].quantity: value "two" is not an integer`)
}
