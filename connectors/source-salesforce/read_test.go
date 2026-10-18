package main

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const accountFields = "Id, AnnualRevenue, BillingAddress, BillingCity, BillingStreet, CreatedDate, Industries__c, IsDeleted, LastActivityDate, LastModifiedDate, Name, NumberOfEmployees, OwnerId, Phone, SystemModstamp, Type, Website"

func TestReadFullRefresh(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, "Account", "Contact", "Invoice__c", "Region__mdt")
	o = resetHits(t, o)
	out := runRead(t, src, cfg, catalog, nil)
	require.NoError(t, out.Err)

	assert.Equal(t, []string{"001000000000001AAA", "001000000000002AAA", "001000000000003AAA", "001000000000004AAA"}, recordIDs(t, out, "Account"))
	assert.Equal(t, []string{"003000000000001AAA", "003000000000002AAA", "003000000000003AAA", "003000000000004AAA"}, recordIDs(t, out, "Contact"))
	assert.Equal(t, []string{"a01000000000001AAA", "a01000000000002AAA", "a01000000000003AAA", "a01000000000004AAA"}, recordIDs(t, out, "Invoice__c"))
	assert.Equal(t, []string{"m00000000000001AAA", "m00000000000002AAA", "m00000000000003AAA"}, recordIDs(t, out, "Region__mdt"))
	assert.Empty(t, states(out), "a full refresh emits no state")

	// Objects with IsDeleted are read through queryAll; every stream follows
	// nextRecordsUrl to its second page.
	assert.Equal(t, []string{
		"POST login/token",
		"GET queryAll", "GET query/01gACC0000000001-2000",
		"GET queryAll", "GET query/01gCON0000000001-2000",
		"GET queryAll", "GET query/01gINV0000000001-2000",
		"GET query", "GET query/01gREG0000000001-2000",
	}, o.keys())
	assert.Equal(t, []string{
		"queryAll SELECT " + accountFields + " FROM Account WHERE SystemModstamp >= 2026-01-01T00:00:00.000Z",
		"queryAll SELECT Id, AccountId, Best_Call_Time__c, Birthdate, Email, Engagement_Score__c, FirstName, HasOptedOutOfEmail, Home_Location__c, IsDeleted, LastModifiedDate, LastName, Legacy_Value__c, Lifetime_Value__c, MailingAddress, Preferred_Channel__c, SystemModstamp, Tax_Number__c, Visits__c FROM Contact WHERE SystemModstamp >= 2026-01-01T00:00:00.000Z",
		"queryAll SELECT Id, Amount__c, IsDeleted, LastModifiedDate, Name, Notes__c, Paid_On__c, Status__c FROM Invoice__c WHERE LastModifiedDate >= 2026-01-01T00:00:00.000Z",
		"query SELECT Id, DeveloperName, Label, Quota__c FROM Region__mdt",
	}, o.queries())
}

func TestReadRecordValues(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	out := runRead(t, src, cfg, catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, "Account", "Contact"), nil)
	require.NoError(t, out.Err)

	raw := string(out.Records(synclet.StreamDescriptor{Name: "Account"})[0].Data)
	assert.NotContains(t, raw, "attributes")
	assert.Contains(t, raw, `"AnnualRevenue":1250000.50`, "currency keeps the API's literal")
	assert.Contains(t, raw, `"SystemModstamp":"2026-03-01T08:00:00.000Z"`, "datetimes are RFC 3339 UTC")
	assert.Contains(t, raw, `"LastActivityDate":"2026-05-20"`)
	assert.Contains(t, raw, `"BillingAddress":{"city":"Springfield"`)

	deleted := records(t, out, "Account")[1]
	assert.Equal(t, true, deleted["IsDeleted"], "soft-deleted rows arrive through queryAll")

	contact := string(out.Records(synclet.StreamDescriptor{Name: "Contact"})[2].Data)
	for _, want := range []string{
		`"Engagement_Score__c":87.50`, `"Lifetime_Value__c":1.10`, `"Home_Location__c":{"latitude":41.8781,"longitude":-87.6298}`,
		`"SystemModstamp":"2026-07-07T07:07:07.007Z"`, `"Best_Call_Time__c":"09:30:00.000Z"`, `"Legacy_Value__c":"legacy-3"`,
	} {
		assert.Contains(t, contact, want)
	}
}

func TestReadIncremental(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeIncremental, "Account", "Invoice__c")
	o = resetHits(t, o)
	out := runRead(t, src, cfg, catalog, nil)
	require.NoError(t, out.Err)

	assert.Equal(t, []string{
		"queryAll SELECT " + accountFields + " FROM Account WHERE SystemModstamp >= 2026-01-01T00:00:00.000Z ORDER BY SystemModstamp, Id",
		"queryAll SELECT Id, Amount__c, IsDeleted, LastModifiedDate, Name, Notes__c, Paid_On__c, Status__c FROM Invoice__c WHERE LastModifiedDate >= 2026-01-01T00:00:00.000Z ORDER BY LastModifiedDate, Id",
	}, o.queries())
	assert.Equal(t, []string{
		`{"cursor":"2026-04-15T10:30:00.000Z","id":"001000000000002AAA"}`,
		`{"cursor":"2026-06-01T00:00:00.000Z","id":"001000000000004AAA"}`,
		`{"cursor":"2026-03-10T10:00:00.000Z","id":"a01000000000002AAA"}`,
		`{"cursor":"2026-05-10T10:00:00.000Z","id":"a01000000000004AAA"}`,
	}, states(out), "state after every page")
}

func TestReadIncrementalFromState(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeIncremental, "Account")
	o = resetHits(t, o)
	out := runRead(t, src, cfg, catalog, streamState("Account", `{"cursor":"2026-06-01T00:00:00.000Z","id":"001000000000004AAA"}`))
	require.NoError(t, out.Err)

	assert.Equal(t, []string{
		"queryAll SELECT " + accountFields + " FROM Account WHERE (SystemModstamp > 2026-06-01T00:00:00.000Z OR (SystemModstamp = 2026-06-01T00:00:00.000Z AND Id > '001000000000004AAA')) AND SystemModstamp >= 2026-01-01T00:00:00.000Z ORDER BY SystemModstamp, Id",
	}, o.queries())
	assert.Equal(t, []string{"001000000000005AAA"}, recordIDs(t, out, "Account"))
	assert.Equal(t, []string{`{"cursor":"2026-10-10T09:15:30.250Z","id":"001000000000005AAA"}`}, states(out))
}

func TestReadIncrementalRejectsBadState(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeIncremental, "Account")
	o = resetHits(t, o)
	out := runRead(t, src, cfg, catalog, streamState("Account", `{"cursor":"2026-06-01T00:00:00.000Z","id":"x' OR Name != '"}`))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "state of Account")
	assert.Empty(t, o.queries())
}

func TestReadRejectsIncrementalWithoutCursor(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, "Region__mdt")
	catalog.Streams[0].SyncMode = synclet.SyncModeIncremental
	out := runRead(t, src, cfg, catalog, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "Region__mdt has no SystemModstamp or LastModifiedDate")
}

// chunkLimit makes the full Invoice__c SELECT (209 encoded characters) too
// long, so it is read as ids and then by two groups of ids and two field
// chunks.
const chunkLimit = 200

func TestReadChunkedSelect(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	src.maxSelectLen = chunkLimit
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeIncremental, "Invoice__c")
	o = resetHits(t, o)
	out := runRead(t, src, cfg, catalog, nil)
	require.NoError(t, out.Err)

	for _, q := range o.queries() {
		assert.LessOrEqual(t, len(encodeQuery(q[strings.Index(q, " ")+1:])), chunkLimit, q)
	}
	assert.Equal(t, []string{
		"queryAll SELECT Id, LastModifiedDate FROM Invoice__c WHERE LastModifiedDate >= 2026-01-01T00:00:00.000Z ORDER BY LastModifiedDate, Id",
		"queryAll SELECT Id, Amount__c, IsDeleted, LastModifiedDate, Name, Notes__c, Paid_On__c FROM Invoice__c WHERE Id IN ('a01000000000001AAA', 'a01000000000002AAA')",
		"queryAll SELECT Id, Status__c FROM Invoice__c WHERE Id IN ('a01000000000001AAA', 'a01000000000002AAA')",
		"queryAll SELECT Id, Amount__c, IsDeleted, LastModifiedDate, Name, Notes__c, Paid_On__c FROM Invoice__c WHERE Id IN ('a01000000000003AAA')",
		"queryAll SELECT Id, Status__c FROM Invoice__c WHERE Id IN ('a01000000000003AAA')",
	}, o.queries())
	recs := records(t, out, "Invoice__c")
	require.Len(t, recs, 3)
	assert.Equal(t, map[string]any{
		"Id": "a01000000000001AAA", "IsDeleted": false, "Name": "INV-0001", "Amount__c": json.Number("100.00"),
		"Status__c": "Paid", "Paid_On__c": "2026-05-01", "Notes__c": "Synthetic invoice 1", "LastModifiedDate": "2026-02-10T10:00:00.000Z",
	}, recs[0])
	assert.Equal(t, []string{`{"cursor":"2026-04-10T10:00:00.000Z","id":"a01000000000003AAA"}`}, states(out))
}

func TestReadChunkedSelectMissingID(t *testing.T) {
	o := fixtureServer(t, map[string][]fixture{
		"GET query/Invoice__c-in-a01000000000001AAA-Status__c": {{file: "query/Invoice__c-in-a01000000000001AAA-Status__c-missing"}},
	})
	src, _ := testSource(o)
	src.maxSelectLen = chunkLimit
	cfg := testCfg(o)
	out := runRead(t, src, cfg, catalogOf(t, src, cfg, synclet.SyncModeIncremental, "Invoice__c"), nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "Invoice__c record a01000000000002AAA is missing from the query of fields Status__c")
	assert.Empty(t, out.Records(synclet.StreamDescriptor{Name: "Invoice__c"}), "no partial record is emitted")
	assert.Empty(t, states(out))
}

func TestReadStopsAtAPIUsageThreshold(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	cfg.APIUsageThresholdPercent = 80
	catalog := catalogOf(t, src, cfg, synclet.SyncModeIncremental, "Account", "Contact")
	o = resetHits(t, o)
	o.setLimit("api-usage=12000/15000")
	out := runRead(t, src, cfg, catalog, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "the org's API usage 12000/15000 (80%) reached api_usage_threshold_percent 80")
	assert.Equal(t, []string{"001000000000001AAA", "001000000000002AAA"}, recordIDs(t, out, "Account"))
	assert.Equal(t, []string{`{"cursor":"2026-04-15T10:30:00.000Z","id":"001000000000002AAA"}`}, states(out), "the page's state is out before the read stops")
	assert.Equal(t, []string{"POST login/token", "GET queryAll"}, o.keys())
}

func TestReadRequestLimitExceeded(t *testing.T) {
	o := fixtureServer(t, map[string][]fixture{
		"GET query/Account": {{status: http.StatusForbidden, file: "errors/REQUEST_LIMIT_EXCEEDED"}},
	})
	src, w := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, "Account")
	o = resetHits(t, o)
	out := runRead(t, src, cfg, catalog, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "REQUEST_LIMIT_EXCEEDED")
	assert.Contains(t, out.Err.Error(), "daily API request limit")
	assert.Equal(t, []string{"POST login/token", "GET queryAll"}, o.keys(), "not retried")
	assert.Empty(t, w.list())
}

func TestReadRotatedRefreshToken(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, "Region__mdt")
	o.override("POST token", fixture{file: "token_rotated"})
	out := runRead(t, src, cfg, catalog, nil)
	require.NoError(t, out.Err)

	var controls []json.RawMessage
	for _, m := range out.Messages {
		if m.Type == synclet.MessageTypeControl {
			controls = append(controls, m.Control.ConnectorConfig.Config)
		}
	}
	require.Len(t, controls, 1, "one CONTROL message with the new refresh token")
	var got Config
	require.NoError(t, json.Unmarshal(controls[0], &got))
	want := cfg
	want.RefreshToken = testRotatedRefreshToken
	assert.Equal(t, want, got)
	assert.Len(t, recordIDs(t, out, "Region__mdt"), 3)
}

// resetHits returns o with its recorded requests cleared, so a test sees
// only the requests of the command under test.
func resetHits(t *testing.T, o *fakeOrg) *fakeOrg {
	t.Helper()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reqs = nil
	return o
}
