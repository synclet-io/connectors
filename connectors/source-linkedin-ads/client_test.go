package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testClient(t *testing.T, overrides map[string][]fixture) (*apiClient, *hits, *waits) {
	t.Helper()
	srv, h := fixtureServer(t, overrides)
	src, w := testSource(srv)
	return src.apiClient(decodeConfig(t, testConfig(t, nil))), h, w
}

func TestExpiredAccessTokenIsRefreshedAndRepeatedOnce(t *testing.T) {
	c, h, w := testClient(t, map[string][]fixture{
		"adAccounts/1001": {{status: http.StatusUnauthorized, file: "errors/401_expired"}, {}},
	})
	var acct map[string]any
	require.NoError(t, c.get(t.Context(), "adAccounts/1001", nil, &acct))
	assert.Equal(t, "Example Account A", acct["name"])

	reqs := h.of("adAccounts/1001")
	require.Len(t, reqs, 2)
	assert.Equal(t, "Bearer test-access-token-1", reqs[0].auth)
	assert.Equal(t, "Bearer test-access-token-2", reqs[1].auth, "the request is repeated with a new access token")
	assert.Len(t, h.of(tokenRoute), 2)
	assert.Empty(t, w.list(), "a refresh is not a retry and does not wait")
}

func TestSecondUnauthorizedFails(t *testing.T) {
	c, h, _ := testClient(t, map[string][]fixture{
		"adAccounts/1001": {{status: http.StatusUnauthorized, file: "errors/401_expired"}},
	})
	err := c.get(t.Context(), "adAccounts/1001", nil, &map[string]any{})
	require.EqualError(t, err, "linkedin GET adAccounts/1001: HTTP 401 EXPIRED_ACCESS_TOKEN (65601): The token used in the request has expired")
	assert.Len(t, h.of("adAccounts/1001"), 2, "repeated once, not more")
	assert.Len(t, h.of(tokenRoute), 2)
}

func TestAccessTokenIsReused(t *testing.T) {
	c, h, _ := testClient(t, nil)
	for range 3 {
		require.NoError(t, c.get(t.Context(), "adAccounts/1001", nil, &map[string]any{}))
	}
	assert.Len(t, h.of(tokenRoute), 1)
	form := h.of(tokenRoute)[0].form
	assert.Equal(t, "refresh_token", form.Get("grant_type"))
	assert.Equal(t, testRefreshToken, form.Get("refresh_token"))
}

func TestRetriesThrottlingAndServerErrors(t *testing.T) {
	c, h, w := testClient(t, map[string][]fixture{
		"adAccounts/1001": {{status: http.StatusTooManyRequests, file: "errors/429"}, {status: http.StatusInternalServerError, file: "errors/500"}, {}},
	})
	require.NoError(t, c.get(t.Context(), "adAccounts/1001", nil, &map[string]any{}))
	assert.Len(t, h.of("adAccounts/1001"), 3)
	assert.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond}, w.list())
}

func TestRetriesGiveUp(t *testing.T) {
	c, h, _ := testClient(t, map[string][]fixture{
		"adAccounts/1001": {{status: http.StatusInternalServerError, file: "errors/500"}},
	})
	err := c.get(t.Context(), "adAccounts/1001", nil, &map[string]any{})
	require.EqualError(t, err, "linkedin GET adAccounts/1001: HTTP 500 INTERNAL_SERVER_ERROR: Internal Server Error")
	assert.Len(t, h.of("adAccounts/1001"), retries+1)
}

func TestClientErrorIsNotRetried(t *testing.T) {
	c, h, w := testClient(t, map[string][]fixture{
		"adAccounts/1001": {{status: http.StatusBadRequest, file: "errors/400_fields"}},
	})
	err := c.get(t.Context(), "adAccounts/1001", nil, &map[string]any{})
	require.EqualError(t, err, "linkedin GET adAccounts/1001: HTTP 400 ILLEGAL_ARGUMENT (100): Invalid value for field; wrong type or other syntax error")
	assert.Len(t, h.of("adAccounts/1001"), 1)
	assert.Empty(t, w.list())
}

func TestListFollowsPageTokens(t *testing.T) {
	c, h, _ := testClient(t, nil)
	var names []any
	err := c.list(t.Context(), "adAccounts", query{{key: "q", value: "search"}}, func(obj map[string]any) error {
		names = append(names, obj["name"])
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []any{"Example Account A", "Example Account B"}, names)
	reqs := h.of("adAccounts")
	require.Len(t, reqs, 2)
	assert.Equal(t, "", reqs[0].params["pageToken"])
	assert.Equal(t, "p2", reqs[1].params["pageToken"])
	assert.Equal(t, "100", reqs[0].params["pageSize"])
}
