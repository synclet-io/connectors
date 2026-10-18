package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestReadRecords(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	out := runRead(t, src, catalogOf(t, src, synclet.SyncModeFullRefresh,
		"shop", "orders", "refunds", "transactions", "inventory_levels", "collections", "product_variants"), nil)

	orders := recordMaps(t, out, "orders")
	require.Len(t, orders, 2)
	assert.Equal(t, "gid://shopify/Order/6001", orders[0]["id"])
	items := orders[0]["lineItems"].([]any)
	require.Len(t, items, 2)
	assert.Equal(t, "gid://shopify/LineItem/7001", items[0].(map[string]any)["id"])
	assert.NotContains(t, items[0], "__parentId")
	assert.Len(t, orders[1]["lineItems"], 1)

	assert.Contains(t, records(out, "orders")[0], `"totalPriceSet":{"presentmentMoney":{"amount":25.10,"currencyCode":"USD"},"shopMoney":{"amount":25.10,"currencyCode":"USD"}}`)
	assert.Contains(t, records(out, "product_variants")[1], `"price":0.10`)

	refunds := recordMaps(t, out, "refunds")
	require.Len(t, refunds, 1)
	assert.Equal(t, "gid://shopify/Order/6001", refunds[0]["order_id"])
	assert.Equal(t, "2026-10-02T09:00:00Z", refunds[0]["order_updated_at"])
	assert.Equal(t, []string{"gid://shopify/OrderTransaction/8201", "gid://shopify/OrderTransaction/8202", "gid://shopify/OrderTransaction/8203"},
		recordIDs(t, out, "transactions"))

	levels := recordMaps(t, out, "inventory_levels")
	require.Len(t, levels, 2)
	assert.Equal(t, "gid://shopify/Location/9001", levels[0]["location_id"])

	assert.Equal(t, []string{"gid://shopify/Collection/9301", "gid://shopify/Collection/9302"}, recordIDs(t, out, "collections"))
	assert.Equal(t, []string{"gid://shopify/Shop/1"}, recordIDs(t, out, "shop"))
	assert.Empty(t, states(out), "a full refresh saves no state")

	assert.Equal(t, []string{
		"ShopPlan",
		"Shop",
		"ShopCreatedAt",
		"SearchProbe", "BulkRun-orders-20261001T000000Z", "BulkPoll-1004",
		"SearchProbe", "BulkRun-refunds-20261001T000000Z", "BulkPoll-1005",
		"SearchProbe", "BulkRun-transactions-20261001T000000Z", "BulkPoll-1007",
		"BulkRun-inventory_levels", "BulkPoll-1009",
		"Collections", "Collections-cursor-collections-2",
		"SearchProbe", "BulkRun-product_variants-20261001T000000Z", "BulkPoll-1002",
	}, h.keys())
}

// TestReadStartFromShop reads from the shop's creation when start_date is
// empty.
func TestReadStartFromShop(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src := testSource(srv)
	cfg := `{"shop":"example.myshopify.com","access_token":"` + testToken + `"}`
	out := readWith(t, src, cfg, catalogOf(t, src, synclet.SyncModeIncremental, "companies"), nil)
	require.NoError(t, out.Err)
	assert.Equal(t, []string{"ShopPlan", "ShopCreatedAt", "Companies"}, h.keys())
	assert.Equal(t, "updated_at:>='2026-09-20T08:15:30Z' AND updated_at:<'2026-10-18T11:59:00Z'", h.ops("Companies")[0].vars["query"])
	assert.Equal(t, []string{`{"cursor":"2026-10-18T11:59:00Z"}`}, states(out))
}
