package main

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}

// schemaProps returns the properties of a stream's json_schema as JSON.
func schemaProps(t *testing.T, s synclet.Stream) map[string]string {
	t.Helper()
	var schema struct {
		Type       string                     `json:"type"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	require.NoError(t, json.Unmarshal(s.JSONSchema, &schema))
	assert.Equal(t, "object", schema.Type)
	props := map[string]string{}
	for k, v := range schema.Properties {
		props[k] = string(v)
	}
	return props
}

func TestDiscoverStreams(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	streams := discoverStreams(t, src, testCfg(o))

	names := make([]string, 0, len(streams))
	for n := range streams {
		names = append(names, n)
	}
	slices.Sort(names)
	// AccountChangeEvent is not queryable; ContentDocumentLink is on the
	// denylist of objects that need a filter.
	assert.Equal(t, []string{"Account", "Contact", "Invoice__c", "Region__mdt"}, names)
	assert.Equal(t, []string{
		"POST login/token", "GET sobjects",
		"GET sobjects/Account/describe", "GET sobjects/Contact/describe",
		"GET sobjects/Invoice__c/describe", "GET sobjects/Region__mdt/describe",
	}, o.keys())

	incremental := []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}
	for name, cursor := range map[string]string{"Account": "SystemModstamp", "Contact": "SystemModstamp", "Invoice__c": "LastModifiedDate"} {
		s := streams[name]
		assert.Equal(t, incremental, s.SupportedSyncModes, name)
		assert.True(t, s.SourceDefinedCursor, name)
		assert.Equal(t, []string{cursor}, s.DefaultCursorField, name)
		assert.Equal(t, [][]string{{"Id"}}, s.SourceDefinedPrimaryKey, name)
	}
	region := streams["Region__mdt"]
	assert.Equal(t, []synclet.SyncMode{synclet.SyncModeFullRefresh}, region.SupportedSyncModes)
	assert.False(t, region.SourceDefinedCursor)
	assert.Empty(t, region.DefaultCursorField)
	assert.Equal(t, [][]string{{"Id"}}, region.SourceDefinedPrimaryKey)
}

func TestDiscoverTypeMapping(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	streams := discoverStreams(t, src, testCfg(o))

	str := `{"type":["string","null"]}`
	num := `{"type":["number","null"]}`
	integer := `{"type":["integer","null"]}`
	boolType := `{"type":["boolean","null"]}`
	obj := `{"type":["object","null"]}`
	dateTime := `{"type":["string","null"],"airbyte_type":"timestamp_with_timezone","format":"date-time"}`
	date := `{"type":["string","null"],"format":"date"}`
	timeType := `{"type":["string","null"],"format":"time"}`

	account := schemaProps(t, streams["Account"])
	assert.Equal(t, map[string]string{
		"Id": str, "IsDeleted": boolType, "Name": str, "Type": str, "Industries__c": str,
		"AnnualRevenue": num, "NumberOfEmployees": integer, "BillingStreet": str, "BillingCity": str,
		"BillingAddress": obj, "Phone": str, "Website": str, "OwnerId": str, "LastActivityDate": date,
		"CreatedDate": dateTime, "LastModifiedDate": dateTime, "SystemModstamp": dateTime,
	}, account, "base64 Logo__c is left out")

	contact := schemaProps(t, streams["Contact"])
	for field, want := range map[string]string{
		"AccountId": str, "Email": str, "Birthdate": date, "MailingAddress": obj, "Home_Location__c": obj,
		"HasOptedOutOfEmail": boolType, "Engagement_Score__c": num, "Lifetime_Value__c": num, "Visits__c": integer,
		"Tax_Number__c": str, "Preferred_Channel__c": str, "Best_Call_Time__c": timeType, "Legacy_Value__c": `{}`,
	} {
		assert.JSONEq(t, want, contact[field], field)
	}
}

func TestDiscoverIncludeExclude(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	cfg.StreamsInclude = []string{"*__c", "Account", "ContentDocumentLink"}
	cfg.StreamsExclude = []string{"Invoice*"}
	streams := discoverStreams(t, src, cfg)
	assert.Len(t, streams, 1)
	assert.Contains(t, streams, "Account", "the denylist holds even when an include pattern names the object")
	assert.Equal(t, []string{"POST login/token", "GET sobjects", "GET sobjects/Account/describe"}, o.keys())
}

func TestDiscoverRejectsMalformedPattern(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	cfg.StreamsInclude = []string{"Account["}
	out := run(t, src, synclet.Request{Command: synclet.CommandDiscover, Config: rawConfig(t, cfg)})
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), `stream pattern "Account[" is malformed`)
	assert.Empty(t, o.keys())
}
