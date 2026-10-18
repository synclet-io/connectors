package main

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// throttled is a THROTTLED response for a query of cost 502 with 10
// points available, restored at 50 per second.
const throttled = `{"errors":[{"message":"Throttled","extensions":{"code":"THROTTLED","documentation":"https://shopify.dev/api/usage/rate-limits"}}],` +
	`"extensions":{"cost":{"requestedQueryCost":502,"actualQueryCost":null,"throttleStatus":{"maximumAvailable":2000.0,"currentlyAvailable":10,"restoreRate":50.0}}}}`

// costly is a successful shop response for a query of cost 502 that left
// 10 points.
const costly = `{"data":{"shop":{"id":"gid://shopify/Shop/1","name":"Example Store"}},` +
	`"extensions":{"cost":{"requestedQueryCost":502,"actualQueryCost":490,"throttleStatus":{"maximumAvailable":2000.0,"currentlyAvailable":10,"restoreRate":50.0}}}}`

func shopRead(t *testing.T, overrides map[string]http.HandlerFunc) (*hits, []time.Duration, error) {
	t.Helper()
	srv, h := fixtureServer(t, overrides)
	src := testSource(srv)
	catalog := catalogOf(t, src, synclet.SyncModeFullRefresh, "shop", "locations")
	w := &waits{}
	src.wait = w.wait
	out := read(t, src, catalog, nil)
	return h, w.list(), out.Err
}

func TestThrottling(t *testing.T) {
	t.Run("throttled then success waits for the restore rate", func(t *testing.T) {
		h, ws, err := shopRead(t, map[string]http.HandlerFunc{
			"Shop":      sequence(jsonBody(http.StatusOK, throttled), respond(t, http.StatusOK, "graphql/Shop.json")),
			"Locations": respond(t, http.StatusOK, "graphql/Locations.json"),
		})
		require.NoError(t, err)
		assert.Len(t, h.ops("Shop"), 2)
		assert.Equal(t, []time.Duration{9840 * time.Millisecond}, ws)
	})

	t.Run("paces before a costly request", func(t *testing.T) {
		h, ws, err := shopRead(t, map[string]http.HandlerFunc{"Shop": jsonBody(http.StatusOK, costly)})
		require.NoError(t, err)
		assert.Len(t, h.ops("Shop"), 1)
		assert.Len(t, h.ops("Locations"), 1)
		assert.Equal(t, []time.Duration{9840 * time.Millisecond}, ws, "the request after a 502-point query waits until 502 points are back")
	})

	t.Run("429 waits Retry-After", func(t *testing.T) {
		tooMany := func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", "2.5")
			w.WriteHeader(http.StatusTooManyRequests)
		}
		h, ws, err := shopRead(t, map[string]http.HandlerFunc{"Shop": sequence(tooMany, respond(t, http.StatusOK, "graphql/Shop.json"))})
		require.NoError(t, err)
		assert.Len(t, h.ops("Shop"), 2)
		assert.Equal(t, []time.Duration{2500 * time.Millisecond}, ws)
	})

	t.Run("gives up at the eleventh throttle", func(t *testing.T) {
		h, ws, err := shopRead(t, map[string]http.HandlerFunc{"Shop": jsonBody(http.StatusOK, throttled)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Shop: THROTTLED: Throttled; gave up after 10 waits")
		assert.Len(t, h.ops("Shop"), 11)
		assert.Len(t, ws, 10)
	})

	t.Run("5xx is retried with a doubling delay", func(t *testing.T) {
		h, ws, err := shopRead(t, map[string]http.HandlerFunc{"Shop": sequence(
			jsonBody(http.StatusBadGateway, `{"errors":"Bad Gateway"}`), jsonBody(http.StatusServiceUnavailable, `{"errors":"Unavailable"}`),
			respond(t, http.StatusOK, "graphql/Shop.json"))})
		require.NoError(t, err)
		assert.Len(t, h.ops("Shop"), 3)
		assert.Equal(t, []time.Duration{time.Millisecond, 2 * time.Millisecond}, ws)
	})

	t.Run("access denied names the scope", func(t *testing.T) {
		h, _, err := shopRead(t, map[string]http.HandlerFunc{"Locations": jsonBody(http.StatusOK,
			`{"errors":[{"message":"Access denied for locations field. Required access: `+"`read_locations`"+` access scope.","extensions":{"code":"ACCESS_DENIED","requiredAccess":"`+"`read_locations`"+` access scope."}}]}`)})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "stream locations: all records: Locations: access denied, the app needs `read_locations` access scope.")
		assert.Len(t, h.ops("Locations"), 1, "access denied is not retried")
	})
}

func TestUnauthorized(t *testing.T) {
	t.Run("access token is requested once", func(t *testing.T) {
		srv, h := fixtureServer(t, nil)
		src := testSource(srv)
		catalog := catalogOf(t, src, synclet.SyncModeFullRefresh, "shop")
		out := readWith(t, src, `{"shop":"example","access_token":"wrong-admin-token"}`, catalog, nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "access token is invalid or revoked (HTTP 401)")
		assert.Len(t, h.ops("Shop"), 1)
	})

	t.Run("grant token is fetched again once", func(t *testing.T) {
		var revoked bool
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{
			"Shop": func(w http.ResponseWriter, r *http.Request) {
				if !revoked {
					revoked = true
					respond(t, http.StatusUnauthorized, "errors/invalid_token.json")(w, r)
					return
				}
				respond(t, http.StatusOK, "graphql/Shop.json")(w, r)
			},
		})
		src := testSource(srv)
		catalog := catalogOf(t, src, synclet.SyncModeFullRefresh, "shop")
		cfg := `{"shop":"example","client_id":"` + testClientID + `","client_secret":"` + testClientSecret + `"}`
		out := readWith(t, src, cfg, catalog, nil)
		require.NoError(t, out.Err)
		assert.Len(t, h.ops("Shop"), 2)
		var grants int
		for _, r := range h.list() {
			if strings.HasSuffix(r.path, "/oauth/access_token") {
				grants++
			}
		}
		assert.Equal(t, 2, grants)
	})

	t.Run("grant token rejected twice fails", func(t *testing.T) {
		srv, h := fixtureServer(t, map[string]http.HandlerFunc{"Shop": respond(t, http.StatusUnauthorized, "errors/invalid_token.json")})
		src := testSource(srv)
		catalog := catalogOf(t, src, synclet.SyncModeFullRefresh, "shop")
		cfg := `{"shop":"example","client_id":"` + testClientID + `","client_secret":"` + testClientSecret + `"}`
		out := readWith(t, src, cfg, catalog, nil)
		require.Error(t, out.Err)
		assert.Contains(t, out.Err.Error(), "access token of the client credentials grant was rejected (HTTP 401)")
		assert.Len(t, h.ops("Shop"), 2)
	})
}
