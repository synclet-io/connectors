package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func clientFor(t *testing.T, src Source) *apiClient {
	t.Helper()
	var cfg Config
	require.NoError(t, json.Unmarshal(testConfig(t, nil), &cfg))
	return src.apiClient(cfg)
}

// searchAll returns every row of a search and its error.
func searchAll(t *testing.T, c *apiClient, cid, login, query string) ([]map[string]any, error) {
	t.Helper()
	var rows []map[string]any
	err := c.search(t.Context(), cid, login, query, func(row map[string]any) error {
		rows = append(rows, row)
		return nil
	})
	return rows, err
}

const campaignQuery = "SELECT customer.id, campaign.id FROM campaign"

func TestClientPagination(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	rows, err := searchAll(t, clientFor(t, src), testClientA, testManager, campaignQuery)
	require.NoError(t, err)
	require.Len(t, rows, 2, "a page and the page its nextPageToken names")

	reqs := h.searches("campaign")
	require.Len(t, reqs, 2)
	assert.Equal(t, []string{"", "page2"}, []string{reqs[0].pageToken, reqs[1].pageToken})
	for _, r := range reqs {
		assert.Equal(t, "customers/"+testClientA+"/googleAds:search", r.path)
		assert.Equal(t, campaignQuery, r.query)
		assert.Equal(t, testManager, r.login)
		assert.Equal(t, testDeveloperToken, r.devToken)
	}
	assert.Equal(t, []string{"token", "customers/" + testClientA + "/googleAds:search", "customers/" + testClientA + "/googleAds:search"},
		pathsOf(h), "the access token is fetched once and reused")
}

func TestClientWithoutLogin(t *testing.T) {
	srv, h := fixtureServer(t, nil)
	src, _ := testSource(srv)
	_, err := searchAll(t, clientFor(t, src), testClientB, "", campaignQuery)
	require.NoError(t, err)
	require.Len(t, h.searches("campaign"), 1)
	assert.Empty(t, h.searches("campaign")[0].login, "no login-customer-id header without a manager")
}

func pathsOf(h *hits) []string {
	var out []string
	for _, r := range h.list() {
		out = append(out, r.path)
	}
	return out
}

func TestClientRetries(t *testing.T) {
	search := "POST customers/" + testClientB + "/googleAds:search"
	tempExhausted := fixture{status: http.StatusTooManyRequests, file: "errors/resource_temporarily_exhausted"}
	ok := fixture{}
	tests := []struct {
		name     string
		seq      []fixture
		requests int
		waits    []time.Duration
		err      string
	}{
		{
			name:     "temporarily exhausted, then ok",
			seq:      []fixture{tempExhausted, tempExhausted, ok},
			requests: 3,
			waits:    []time.Duration{time.Second + time.Millisecond, 2*time.Second + time.Millisecond},
		},
		{
			name:     "5xx, then ok",
			seq:      []fixture{{status: http.StatusInternalServerError, file: "errors/internal"}, ok},
			requests: 2,
			waits:    []time.Duration{time.Second + time.Millisecond},
		},
		{
			name:     "temporarily exhausted five times",
			seq:      []fixture{tempExhausted},
			requests: 5,
			waits: []time.Duration{
				time.Second + time.Millisecond, 2*time.Second + time.Millisecond,
				4*time.Second + time.Millisecond, 8*time.Second + time.Millisecond,
			},
			err: "HTTP 429 RESOURCE_TEMPORARILY_EXHAUSTED",
		},
		{
			name:     "daily quota exhausted is not retried",
			seq:      []fixture{{status: http.StatusTooManyRequests, file: "errors/resource_exhausted"}},
			requests: 1,
			err:      "HTTP 429 RESOURCE_EXHAUSTED: Too many requests. Retry in 86400 seconds.",
		},
		{
			name:     "403 is not retried",
			seq:      []fixture{{status: http.StatusForbidden, file: "errors/user_permission_denied"}},
			requests: 1,
			err:      "customer 2345678901 is not accessible",
		},
		{
			name:     "400 is not retried",
			seq:      []fixture{{status: http.StatusBadRequest, file: "errors/query_error"}},
			requests: 1,
			err:      "HTTP 400 UNRECOGNIZED_FIELD: Unrecognized field in the query.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, h := fixtureServer(t, map[string][]fixture{search: tt.seq})
			src, w := testSource(srv)
			_, err := searchAll(t, clientFor(t, src), testClientB, "", campaignQuery)
			if tt.err == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)
			}
			assert.Len(t, h.searches("campaign"), tt.requests)
			assert.Equal(t, tt.waits, w.list())
		})
	}
}

func TestClientRefreshesTokenOn401(t *testing.T) {
	search := "POST customers/" + testClientB + "/googleAds:search"
	srv, h := fixtureServer(t, map[string][]fixture{search: {{status: http.StatusUnauthorized, file: "errors/unauthenticated"}, {}}})
	src, w := testSource(srv)
	rows, err := searchAll(t, clientFor(t, src), testClientB, "", campaignQuery)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, []string{"token", "customers/" + testClientB + "/googleAds:search", "token", "customers/" + testClientB + "/googleAds:search"}, pathsOf(h))
	assert.Empty(t, w.list())

	t.Run("a second 401 fails", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string][]fixture{search: {{status: http.StatusUnauthorized, file: "errors/unauthenticated"}}})
		src, _ := testSource(srv)
		_, err := searchAll(t, clientFor(t, src), testClientB, "", campaignQuery)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "HTTP 401 UNAUTHENTICATED")
		assert.Len(t, h.searches("campaign"), 2)
	})
}
