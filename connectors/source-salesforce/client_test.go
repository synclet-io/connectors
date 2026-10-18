package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func TestExpiredTokenRefreshedOnce(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, "Region__mdt")
	o = resetHits(t, o)
	o.expireNext(1)
	out := runRead(t, src, cfg, catalog, nil)
	require.NoError(t, out.Err)

	assert.Equal(t, []string{
		"POST login/token", "GET query",
		"POST login/token", "GET query",
		"GET query/01gREG0000000001-2000",
	}, o.keys(), "the rejected request is repeated with a new access token")
	q := o.queries()
	require.Len(t, q, 2)
	assert.Equal(t, q[0], q[1])
	assert.Len(t, recordIDs(t, out, "Region__mdt"), 3)
}

func TestExpiredTokenRepeatedOnlyOnce(t *testing.T) {
	o := fixtureServer(t, nil)
	src, _ := testSource(o)
	cfg := testCfg(o)
	catalog := catalogOf(t, src, cfg, synclet.SyncModeFullRefresh, "Region__mdt")
	o = resetHits(t, o)
	o.expireNext(10)
	out := runRead(t, src, cfg, catalog, nil)
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "INVALID_SESSION_ID")
	assert.Equal(t, []string{"POST login/token", "GET query", "POST login/token", "GET query"}, o.keys(), "no refresh loop")
}

func TestRetryServerError(t *testing.T) {
	o := fixtureServer(t, map[string][]fixture{
		"GET sobjects": {{status: http.StatusServiceUnavailable, file: "errors/SERVER_UNAVAILABLE"}, {status: http.StatusServiceUnavailable, file: "errors/SERVER_UNAVAILABLE"}, {}},
	})
	src, w := testSource(o)
	st := runCheck(t, src, testCfg(o))
	assert.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
	assert.Equal(t, []string{"POST login/token", "GET sobjects", "GET sobjects", "GET sobjects"}, o.keys())
	assert.Equal(t, []time.Duration{time.Second, 2 * time.Second}, w.list())
}

func TestRetryGivesUp(t *testing.T) {
	o := fixtureServer(t, map[string][]fixture{
		"GET sobjects": {{status: http.StatusServiceUnavailable, file: "errors/SERVER_UNAVAILABLE"}},
	})
	src, w := testSource(o)
	st := runCheck(t, src, testCfg(o))
	assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
	assert.Contains(t, st.Message, "SERVER_UNAVAILABLE")
	assert.Len(t, w.list(), attempts-1)
}

func TestParseLimitInfo(t *testing.T) {
	for in, want := range map[string][2]int{
		"api-usage=18/15000":                         {18, 15000},
		"api-usage=4500/5000, per-app-api-usage=1/2": {4500, 5000},
		"per-app-api-usage=1/2":                      {0, 0},
		"":                                           {0, 0},
		"api-usage=x/5000":                           {0, 0},
	} {
		used, max := parseLimitInfo(in)
		assert.Equal(t, want, [2]int{used, max}, in)
	}
}
