package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestDiscover(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	got := discoverStreams(t, testSource(srv, newClock()))
	assert.Empty(t, h.list(), "discover requests nothing")

	wantPK := map[string][][]string{
		streamProjects: {{"id"}}, streamApps: {{"id"}}, streamProducts: {{"id"}}, streamEntitlements: {{"id"}},
		streamOfferings: {{"id"}}, streamCustomers: {{"id"}}, streamCustomerSubscriptions: {{"id"}},
		streamCustomerPurchases: {{"id"}}, streamCustomerActiveEntitlements: {{"customer_id"}, {"entitlement_id"}},
		streamMetricsOverview: {{"id"}},
	}
	require.Len(t, got, len(wantPK))
	for name, pk := range wantPK {
		s, ok := got[name]
		require.True(t, ok, name)
		assert.Equal(t, pk, s.SourceDefinedPrimaryKey, name)
		assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, s.SupportedSyncModes, name)
		var schema struct {
			AdditionalProperties bool                       `json:"additionalProperties"`
			Properties           map[string]json.RawMessage `json:"properties"`
		}
		require.NoError(t, json.Unmarshal(s.JSONSchema, &schema), name)
		assert.True(t, schema.AdditionalProperties, name)
		for _, k := range pk {
			assert.Contains(t, schema.Properties, k[0], name)
		}
	}
}
