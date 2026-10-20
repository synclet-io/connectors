package engine

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

const itemsStream = `  - name: items
    path: items
    primary_key: [id]
    schema: ` + itemSchema + `
    records: {path: $.data}
`

func TestAuth(t *testing.T) {
	tests := []struct {
		name   string
		auth   string
		config string
		check  func(t *testing.T, r *http.Request)
	}{
		{"api_key header", `auth: {type: api_key, in: header, name: Authorization, value: "Token {{ config.api_key }}"}`, testConfig,
			func(t *testing.T, r *http.Request) { assert.Equal(t, "Token k1", r.Header.Get("Authorization")) }},
		{"api_key query", `auth: {type: api_key, in: query, name: key, value: "{{ config.api_key }}"}`, testConfig,
			func(t *testing.T, r *http.Request) {
				assert.Equal(t, "k1", r.URL.Query().Get("key"))
				assert.Empty(t, r.Header.Get("Authorization"))
			}},
		{"bearer", ``, testConfig,
			func(t *testing.T, r *http.Request) { assert.Equal(t, "Bearer k1", r.Header.Get("Authorization")) }},
		{"basic with empty password", `auth: {type: basic, username: "{{ config.api_key }}", password: ""}`, testConfig,
			func(t *testing.T, r *http.Request) {
				u, p, ok := r.BasicAuth()
				assert.True(t, ok)
				assert.Equal(t, "k1", u)
				assert.Empty(t, p)
			}},
		{"basic", `auth: {type: basic, username: "{{ config.user }}", password: "{{ config.api_key }}"}`, `{"api_key":"k1","user":"me@example.com"}`,
			func(t *testing.T, r *http.Request) {
				u, p, _ := r.BasicAuth()
				assert.Equal(t, "me@example.com", u)
				assert.Equal(t, "k1", p)
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t).json("/v1/items", `{"data":[{"id":"a"}]}`)
			src := newSource(t, manifestYAML(tt.auth+"\n", itemsStream), a, newClock())
			st := checkStatus(t, src, tt.config)
			require.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
			reqs := a.requests()
			require.Len(t, reqs, 1)
			tt.check(t, reqs[0])
			assert.Equal(t, "application/json", reqs[0].Header.Get("Accept"))
			assert.Equal(t, "synclet/source-test", reqs[0].Header.Get("User-Agent"))
		})
	}
}

func TestAuthQueryKeepsKeyOutOfErrors(t *testing.T) {
	a := newAPI(t).on("/v1/items", reply{status: 500, body: `{"message":"boom"}`})
	src := newSource(t, manifestYAML(`auth: {type: api_key, in: query, name: key, value: "{{ config.api_key }}"}`+"\n", itemsStream), a, newClock())
	out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
	msg, _ := traceMessage(out)
	assert.Contains(t, msg, "GET /v1/items (HTTP 500)")
	assert.NotContains(t, msg, "k1")
}

const oauthTop = `auth:
  type: oauth2_refresh
  token_url: TOKEN_URL
  client_id: "{{ config.client_id }}"
  client_secret: "{{ config.client_secret }}"
  refresh_token: "{{ config.refresh_token }}"
  scopes: [read, write]
`

const oauthConfig = `{"api_key":"unused","client_id":"cid","client_secret":"cs","refresh_token":"rt1"}`

func oauthSource(t *testing.T, a *fakeAPI, c *clock, streams string) synclet.Source[json.RawMessage] {
	t.Helper()
	return newSource(t, manifestYAML(strings.Replace(oauthTop, "TOKEN_URL", a.srv.URL+"/oauth/token", 1), streams), a, c)
}

func TestOAuthRefresh(t *testing.T) {
	t.Run("before the first request", func(t *testing.T) {
		a := newAPI(t).
			json("/oauth/token", `{"access_token":"at1","expires_in":3600}`).
			json("/v1/items", `{"data":[{"id":"a"}]}`)
		st := checkStatus(t, oauthSource(t, a, newClock(), itemsStream), oauthConfig)
		require.Equal(t, synclet.ConnectionStatusSucceeded, st.Status, st.Message)
		reqs := a.requests()
		require.Len(t, reqs, 2)
		assert.Equal(t, http.MethodPost, reqs[0].Method)
		assert.Equal(t, "client_id=cid&client_secret=cs&grant_type=refresh_token&refresh_token=rt1&scope=read+write", a.bodies[0])
		assert.Equal(t, "Bearer at1", reqs[1].Header.Get("Authorization"))
	})

	t.Run("on expiry", func(t *testing.T) {
		a := newAPI(t).
			on("/oauth/token", reply{status: 200, body: `{"access_token":"at1","expires_in":120}`}, reply{status: 200, body: `{"access_token":"at2","expires_in":120}`}).
			json("/v1/items", `{"data":[{"id":"a"}],"next":"p2"}`).
			on("/v1/items?after=p2", reply{status: 429, headers: map[string]string{"Retry-After": "90"}}, reply{status: 200, body: `{"data":[{"id":"b"}]}`})
		c := newClock()
		stream := strings.Replace(itemsStream, "records: {path: $.data}", "records: {path: $.data}\n    pagination: {type: cursor, cursor_from: $.next, cursor_param: after}", 1)
		src := oauthSource(t, a, c, stream)
		// The 429 of the second page waits past the token's expiry.
		out := read(t, src, oauthConfig, catalog(t, src, oauthConfig, synclet.SyncModeFullRefresh, "items"), "")
		require.NoError(t, out.Err)
		auths := []string{}
		for _, r := range a.requests() {
			auths = append(auths, r.URL.Path+" "+r.Header.Get("Authorization"))
		}
		assert.Equal(t, []string{"/oauth/token ", "/v1/items Bearer at1", "/v1/items Bearer at1", "/oauth/token ", "/v1/items Bearer at2"}, auths)
	})

	t.Run("once after a 401", func(t *testing.T) {
		a := newAPI(t).
			on("/oauth/token", reply{status: 200, body: `{"access_token":"at1","expires_in":3600}`}, reply{status: 200, body: `{"access_token":"at2","expires_in":3600,"refresh_token":"rt2"}`}).
			on("/v1/items", reply{status: 401, body: `{"message":"token expired"}`}, reply{status: 200, body: `{"data":[{"id":"a"}]}`})
		src := oauthSource(t, a, newClock(), itemsStream)
		out := read(t, src, oauthConfig, catalog(t, src, oauthConfig, synclet.SyncModeFullRefresh, "items"), "")
		require.NoError(t, out.Err)
		assert.Equal(t, []string{`"a"`}, values(out, "items", "id"))
		var warned bool
		for _, l := range out.Logs() {
			if l.Level == synclet.LogLevelWarn && strings.Contains(l.Message, "new refresh_token") {
				warned = true
				assert.NotContains(t, l.Message, "rt2")
			}
		}
		assert.True(t, warned, "no WARN about the rotated refresh token")
	})

	t.Run("a second 401 is a config error", func(t *testing.T) {
		a := newAPI(t).
			json("/oauth/token", `{"access_token":"at1","expires_in":3600}`).
			on("/v1/items", reply{status: 401, body: `{"message":"revoked"}`})
		st := checkStatus(t, oauthSource(t, a, newClock(), itemsStream), oauthConfig)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "revoked")
		assert.Len(t, a.keys(), 4)
	})

	t.Run("a rejected refresh token is a config error", func(t *testing.T) {
		a := newAPI(t).on("/oauth/token", reply{status: 400, body: `{"error":"invalid_grant"}`})
		st := checkStatus(t, oauthSource(t, a, newClock(), itemsStream), oauthConfig)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "invalid_grant")
	})
}

func readItems(t *testing.T, top string, a *fakeAPI, c *clock) (string, synclet.FailureType, []string) {
	t.Helper()
	src := newSource(t, manifestYAML(top, itemsStream), a, c)
	out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
	msg, ft := traceMessage(out)
	return msg, ft, values(out, "items", "id")
}

func TestRetry(t *testing.T) {
	ok := reply{status: 200, body: `{"data":[{"id":"a"}]}`}
	epoch := newClock().Now().Add(7 * time.Second).Unix()
	tests := []struct {
		name    string
		top     string
		replies []reply
		waits   []time.Duration
		err     string
	}{
		{"500 then 200", "", []reply{{status: 500, body: `{}`}, ok}, []time.Duration{time.Second}, ""},
		{"transient doubling until attempts", "errors: {message: $.message}\nretry: {transient: {statuses: [503], attempts: 3, backoff: 2s}}\n",
			[]reply{{status: 503, body: `{"message":"down"}`}}, []time.Duration{2 * time.Second, 4 * time.Second}, "GET /v1/items (HTTP 503): down; gave up after 3 attempts"},
		{"429 Retry-After seconds", "", []reply{{status: 429, headers: map[string]string{"Retry-After": "2"}}, ok}, []time.Duration{2 * time.Second}, ""},
		{"429 Retry-After date", "", []reply{{status: 429, headers: map[string]string{"Retry-After": newClock().Now().Add(30 * time.Second).Format(http.TimeFormat)}}, ok}, []time.Duration{30 * time.Second}, ""},
		{"429 reset epoch", "retry: {rate_limit: {wait: [\"header:Retry-After\", \"header-epoch:x-ratelimit-reset\", 60s]}}\n",
			[]reply{{status: 429, headers: map[string]string{"x-ratelimit-reset": fmt.Sprint(epoch)}}, ok}, []time.Duration{7 * time.Second}, ""},
		{"429 body backoff", "retry: {rate_limit: {wait: [\"header:Retry-After\", \"body:$.backoff_ms\", 60s]}}\n",
			[]reply{{status: 429, body: `{"backoff_ms":1500}`}, ok}, []time.Duration{1500 * time.Millisecond}, ""},
		{"429 fallback", "", []reply{{status: 429, body: `{}`}, ok}, []time.Duration{time.Minute}, ""},
		{"403 as a rate limit", "retry: {rate_limit: {also_when: {status: 403, header: x-ratelimit-remaining, equals: \"0\"}, wait: [\"header-epoch:x-ratelimit-reset\", 60s]}}\n",
			[]reply{{status: 403, headers: map[string]string{"x-ratelimit-remaining": "0", "x-ratelimit-reset": fmt.Sprint(epoch)}}, ok}, []time.Duration{7 * time.Second}, ""},
		{"five waits then failure", "errors: {message: $.message}\n", []reply{{status: 429, headers: map[string]string{"Retry-After": "1"}, body: `{"message":"slow down"}`}},
			[]time.Duration{time.Second, time.Second, time.Second, time.Second, time.Second}, "GET /v1/items (HTTP 429): slow down; gave up after 5 waits"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t).on("/v1/items", tt.replies...)
			c := newClock()
			msg, _, ids := readItems(t, tt.top, a, c)
			assert.Equal(t, tt.waits, c.waits())
			if tt.err == "" {
				assert.Empty(t, msg)
				assert.Equal(t, []string{`"a"`}, ids)
				return
			}
			assert.Contains(t, msg, tt.err)
		})
	}
}

func TestErrorClassification(t *testing.T) {
	tests := []struct {
		name   string
		top    string
		status int
		body   string
		ft     synclet.FailureType
		msg    string
	}{
		{"401 config error with the API text", "errors: {message: $.error.message}\n", 401, `{"error":{"message":"Invalid API key"}}`, synclet.FailureTypeConfigError, "GET /v1/items (HTTP 401): Invalid API key"},
		{"403 config error", "", 403, `forbidden here`, synclet.FailureTypeConfigError, "(HTTP 403): forbidden here"},
		{"other status fails the read", "", 400, strings.Repeat("x", 300), synclet.FailureTypeSystemError, "(HTTP 400): " + strings.Repeat("x", 200)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t).on("/v1/items", reply{status: tt.status, body: tt.body})
			c := newClock()
			msg, ft, _ := readItems(t, tt.top, a, c)
			assert.Equal(t, tt.ft, ft)
			assert.Contains(t, msg, tt.msg)
			assert.NotContains(t, msg, strings.Repeat("x", 201))
			assert.Empty(t, c.waits())
			assert.Len(t, a.keys(), 1)
		})
	}

	t.Run("check fails with the API text", func(t *testing.T) {
		a := newAPI(t).on("/v1/items", reply{status: 401, body: `{"message":"bad key"}`})
		src := newSource(t, manifestYAML("errors: {message: $.message}\n", itemsStream), a, newClock())
		st := checkStatus(t, src, testConfig)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "bad key")
	})
}

func TestOn404Skip(t *testing.T) {
	a := newAPI(t).on("/v1/items", reply{status: 404, body: `{"message":"gone"}`})
	src := newSource(t, manifestYAML("", strings.Replace(itemsStream, "    records:", "    on_404: skip\n    records:", 1)), a, newClock())
	out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
	require.NoError(t, out.Err)
	var warned bool
	for _, l := range out.Logs() {
		warned = warned || (l.Level == synclet.LogLevelWarn && strings.Contains(l.Message, "404"))
	}
	assert.True(t, warned)

	a = newAPI(t).on("/v1/items", reply{status: 404, body: `{"message":"gone"}`})
	msg, _, _ := readItems(t, "", a, newClock())
	assert.Contains(t, msg, "(HTTP 404): ")
}

func TestHeadersAndParams(t *testing.T) {
	a := newAPI(t).json("/v1/items", `{"data":[]}`)
	top := "headers: {Accept: application/vnd.test+json, X-Version: \"2026-01-01\"}\nparams: {limit: 100, region: \"{{ config.region }}\", types: [a, b]}\n"
	stream := strings.Replace(itemsStream, "    records:", "    params: {limit: 50, empty: \"{{ config.region }}\"}\n    headers: {X-Stream: s}\n    records:", 1)
	src := newSource(t, manifestYAML(top, stream), a, newClock())
	out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
	require.NoError(t, out.Err)
	r := a.requests()[0]
	assert.Equal(t, "limit=50&types=a&types=b", r.URL.RawQuery)
	assert.Equal(t, "application/vnd.test+json", r.Header.Get("Accept"))
	assert.Equal(t, "2026-01-01", r.Header.Get("X-Version"))
	assert.Equal(t, "s", r.Header.Get("X-Stream"))
}

func TestPathEscaping(t *testing.T) {
	a := newAPI(t).json("/v1/regions/eu%20west/1/items", `{"data":[]}`).
		json("/v1/regions/a%3Fb%23c%25d/items", `{"data":[]}`)
	stream := strings.Replace(itemsStream, "path: items", `path: "regions/{{ config.region }}/items"`, 1)
	for _, tt := range []struct{ region, want string }{
		{"eu west/1", "/v1/regions/eu%20west/1/items"},
		{"a?b#c%d", "/v1/regions/a%3Fb%23c%25d/items"},
	} {
		src := newSource(t, manifestYAML("", stream), a, newClock())
		cfg := `{"api_key":"k1","region":"` + tt.region + `"}`
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeFullRefresh, "items"), "")
		require.NoError(t, out.Err, tt.region)
		assert.Equal(t, tt.want, a.requests()[len(a.requests())-1].URL.EscapedPath())
	}

	for _, region := range []string{"..", "x/../y", ".", "x/./y", "a//b", "x/"} {
		src := newSource(t, manifestYAML("", stream), a, newClock())
		cfg := `{"api_key":"k1","region":"` + region + `"}`
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeFullRefresh, "items"), "")
		msg, _ := traceMessage(out)
		assert.Contains(t, msg, "path segment", region)
	}
	assert.Len(t, a.requests(), 2)

	cfg := `{"api_key":"k1","region":""}`
	stream = strings.Replace(itemsStream, "path: items", `path: "{{ config.region }}"`, 1)
	src := newSource(t, manifestYAML("", stream), a, newClock())
	out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeFullRefresh, "items"), "")
	msg, _ := traceMessage(out)
	assert.Contains(t, msg, "path renders empty")
}

func TestBodyErrors(t *testing.T) {
	ok := reply{status: 200, body: `{"ok":true,"data":[{"id":"a"}]}`}
	slackLike := "errors:\n  message: $.error\n  when:\n" +
		"    - {path: $.error, equals: [invalid_auth, token_revoked], action: config_error}\n" +
		"    - {path: $.error, equals: busy, action: retry}\n" +
		"    - {path: $.error, equals: ratelimited, action: rate_limit}\n" +
		"    - {path: $.error, equals: nothing_here, action: ignore}\n" +
		"    - {path: $.ok, equals: false}\n"
	tests := []struct {
		name    string
		top     string
		replies []reply
		ft      synclet.FailureType
		msg     string
		waits   []time.Duration
		ids     []string
	}{
		{"fail by default with the body's message", slackLike, []reply{{status: 200, body: `{"ok":false,"error":"channel_not_found"}`}},
			synclet.FailureTypeSystemError, "GET /v1/items (HTTP 200): channel_not_found", nil, nil},
		{"config error, one of a list", slackLike, []reply{{status: 200, body: `{"ok":false,"error":"token_revoked"}`}},
			synclet.FailureTypeConfigError, "GET /v1/items (HTTP 200): token_revoked", nil, nil},
		{"retry like a transient status", slackLike, []reply{{status: 200, body: `{"ok":false,"error":"busy"}`}, ok},
			"", "", []time.Duration{time.Second}, []string{`"a"`}},
		{"retry gives up after the attempts", slackLike, []reply{{status: 200, body: `{"ok":false,"error":"busy"}`}},
			synclet.FailureTypeSystemError, "GET /v1/items (HTTP 200): busy; gave up after 3 attempts", []time.Duration{time.Second, 2 * time.Second}, nil},
		{"rate_limit waits for the API", slackLike, []reply{{status: 200, body: `{"ok":false,"error":"ratelimited"}`, headers: map[string]string{"Retry-After": "3"}}, ok},
			"", "", []time.Duration{3 * time.Second}, []string{`"a"`}},
		{"ignore reads no records", slackLike, []reply{{status: 200, body: `{"ok":false,"error":"nothing_here"}`}},
			"", "", nil, nil},
		{"no rule matches", slackLike, []reply{ok}, "", "", nil, []string{`"a"`}},
		{"numbers compare by value", "errors: {when: [{path: $.code, equals: 7}]}\n", []reply{{status: 200, body: `{"code":7.0,"data":[]}`}},
			synclet.FailureTypeSystemError, "GET /v1/items (HTTP 200): ", nil, nil},
		{"a rule never sees a non-2xx response", slackLike, []reply{{status: 400, body: `{"ok":false,"error":"token_revoked"}`}},
			synclet.FailureTypeSystemError, "GET /v1/items (HTTP 400): token_revoked", nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAPI(t).on("/v1/items", tt.replies...)
			c := newClock()
			msg, ft, ids := readItems(t, tt.top, a, c)
			assert.Equal(t, tt.waits, c.waits())
			assert.Equal(t, tt.ids, ids)
			if tt.msg == "" {
				assert.Empty(t, msg)
				return
			}
			assert.Equal(t, tt.ft, ft)
			assert.Contains(t, msg, tt.msg)
		})
	}

	t.Run("ignore logs a warning", func(t *testing.T) {
		a := newAPI(t).on("/v1/items", reply{status: 200, body: `{"ok":false,"error":"nothing_here"}`})
		src := newSource(t, manifestYAML(slackLike, itemsStream), a, newClock())
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items"), "")
		require.NoError(t, out.Err)
		var warned bool
		for _, l := range out.Logs() {
			warned = warned || (l.Level == synclet.LogLevelWarn && strings.Contains(l.Message, "nothing_here"))
		}
		assert.True(t, warned)
	})

	t.Run("check fails on a matching 200", func(t *testing.T) {
		a := newAPI(t).on("/v1/items", reply{status: 200, body: `{"ok":false,"error":"invalid_auth"}`})
		src := newSource(t, manifestYAML(slackLike, itemsStream), a, newClock())
		st := checkStatus(t, src, testConfig)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "invalid_auth")
	})

	t.Run("stream rules come first and stay in their stream", func(t *testing.T) {
		streams := strings.Replace(itemsStream, "    records:", "    errors: {when: [{path: $.error, equals: gone, action: ignore}]}\n    records:", 1) +
			strings.ReplaceAll(itemsStream, "items", "others")
		a := newAPI(t).
			on("/v1/items", reply{status: 200, body: `{"ok":false,"error":"gone"}`}).
			on("/v1/others", reply{status: 200, body: `{"ok":false,"error":"gone"}`})
		src := newSource(t, manifestYAML(slackLike, streams), a, newClock())
		out := read(t, src, testConfig, catalog(t, src, testConfig, synclet.SyncModeFullRefresh, "items", "others"), "")
		msg, _ := traceMessage(out)
		assert.Contains(t, msg, "GET /v1/others (HTTP 200): gone")
		assert.Equal(t, []string{"/v1/items", "/v1/others"}, a.keys())
	})
}
