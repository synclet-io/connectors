package engine

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func dump(t *testing.T, out *testkit.Output) string {
	t.Helper()
	b, err := json.Marshal(out.Messages)
	require.NoError(t, err)
	s := string(b)
	if out.Err != nil {
		s += "\n" + out.Err.Error()
	}
	return s
}

func TestConfigErrorsHideSecrets(t *testing.T) {
	const secret = "leak-ME-9f8e7d"
	yaml := strings.Replace(manifestYAML("", itemsStream),
		"api_key: {type: string, secret: true}", `api_key: {type: string, secret: true, pattern: "^sk_"}`, 1)
	cfg := fmt.Sprintf(`{"api_key":%q}`, secret)

	t.Run("read", func(t *testing.T) {
		src := newSource(t, yaml, newAPI(t), newClock())
		out := read(t, src, cfg, &synclet.ConfiguredCatalog{}, "")
		require.Error(t, out.Err)
		s := dump(t, out)
		assert.Contains(t, s, "config.api_key")
		assert.NotContains(t, s, secret)
	})
	t.Run("check", func(t *testing.T) {
		src := newSource(t, yaml, newAPI(t), newClock())
		s := dump(t, runCmd(t, src, synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(cfg)}))
		assert.Contains(t, s, "config.api_key")
		assert.NotContains(t, s, secret)
	})
	t.Run("infer", func(t *testing.T) {
		out := infer(t, generic(newAPI(t)), InferRequest{Config: json.RawMessage(withManifest(t, cfg, yaml))})
		require.Error(t, out.err)
		assert.Contains(t, out.raw, `config.api_key: the secret value does not satisfy \"pattern\"`)
		assert.NotContains(t, out.raw, secret)
	})
}

func TestOn404SkipWarningHidesSecrets(t *testing.T) {
	const secret = "k404-SECRET-77"
	cfg := fmt.Sprintf(`{"api_key":%q}`, secret)
	body := fmt.Sprintf(`{"message":"no items for key %s"}`, secret)
	yaml := manifestYAML("", strings.Replace(itemsStream, "    records:", "    on_404: skip\n    records:", 1))

	warned := func(t *testing.T, logs []string) {
		t.Helper()
		var found bool
		for _, l := range logs {
			if strings.Contains(l, "on_404") {
				found = true
				assert.Contains(t, l, "***")
				assert.NotContains(t, l, secret)
			}
		}
		assert.True(t, found, "no on_404 warning in %v", logs)
	}
	t.Run("read", func(t *testing.T) {
		src := newSource(t, yaml, newAPI(t).on("/v1/items", reply{status: 404, body: body}), newClock())
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeFullRefresh, "items"), "")
		require.NoError(t, out.Err)
		var logs []string
		for _, l := range out.Logs() {
			logs = append(logs, l.Message)
		}
		warned(t, logs)
		assert.NotContains(t, dump(t, out), secret)
	})
	t.Run("infer", func(t *testing.T) {
		out := infer(t, generic(newAPI(t).on("/v1/items", reply{status: 404, body: body})), InferRequest{Config: json.RawMessage(withManifest(t, cfg, yaml))})
		require.NoError(t, out.err)
		warned(t, out.logs(synclet.LogLevelWarn))
		assert.NotContains(t, out.raw, secret)
	})
}

func TestInferRedactsResponseBody(t *testing.T) {
	tests := []struct {
		name, prop, value, echo string
	}{
		{name: "string secret", prop: "api_key", value: `"sk-ECHO-123abc"`, echo: "sk-ECHO-123abc"},
		{name: "number secret", prop: "pin", value: "987654321", echo: "987654321"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml := strings.Replace(manifestYAML("", itemsStream),
				"    user: {type: string}\n", "    user: {type: string}\n    pin: {type: integer, secret: true}\n", 1)
			cfg := fmt.Sprintf(`{"api_key":"k1",%q:%s}`, tt.prop, tt.value)
			if tt.prop == "api_key" {
				cfg = fmt.Sprintf(`{"api_key":%s}`, tt.value)
			}
			a := newAPI(t).on("/v1/items", reply{status: 401, body: fmt.Sprintf(`{"error":"bad credential %s"}`, tt.echo)})
			out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, cfg, yaml))})
			require.NoError(t, out.err)
			tr := out.traces()
			require.Len(t, tr, 1)
			assert.Contains(t, tr[0].Message, "bad credential ***")
			assert.Equal(t, synclet.FailureTypeConfigError, tr[0].FailureType)
			assert.NotContains(t, out.raw, tt.echo)
		})
	}
}

// texts joins the human-readable text of msgs: log, connection status and
// error trace messages.
func texts[M synclet.Message | *synclet.Message](msgs []M) string {
	var b strings.Builder
	for _, v := range msgs {
		m := asMessage(v)
		switch {
		case m.Log != nil:
			b.WriteString(m.Log.Message)
		case m.ConnectionStatus != nil:
			b.WriteString(m.ConnectionStatus.Message)
		case m.Trace != nil && m.Trace.Error != nil:
			b.WriteString(m.Trace.Error.Message)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func asMessage[M synclet.Message | *synclet.Message](v M) *synclet.Message {
	switch m := any(v).(type) {
	case *synclet.Message:
		return m
	case synclet.Message:
		return &m
	}
	return nil
}

func TestRedactsEncodedForms(t *testing.T) {
	const user, secret = "svc-user", `pa"ss\wo rd/+<&>`
	jsonForm := func(html bool) string {
		var b strings.Builder
		enc := json.NewEncoder(&b)
		enc.SetEscapeHTML(html)
		require.NoError(t, enc.Encode(secret))
		return strings.TrimSuffix(strings.TrimSpace(b.String()), `"`)[1:]
	}
	forms := map[string]string{
		"raw":       secret,
		"json":      jsonForm(false),
		"json html": jsonForm(true),
		"query":     url.QueryEscape(secret),
		"path":      url.PathEscape(secret),
		"basic":     base64.StdEncoding.EncodeToString([]byte(user + ":" + secret)),
	}
	var body strings.Builder
	for _, k := range sortedKeys(forms) {
		fmt.Fprintf(&body, "%s=%s ", k, forms[k])
	}
	yaml := manifestYAML(`auth: {type: basic, username: "{{ config.user }}", password: "{{ config.api_key }}"}`+"\n", itemsStream)
	cfgB, err := json.Marshal(map[string]string{"api_key": secret, "user": user})
	require.NoError(t, err)
	cfg := string(cfgB)
	api := func() *fakeAPI { return newAPI(t).on("/v1/items", reply{status: 401, body: body.String()}) }

	masked := func(t *testing.T, s string) {
		t.Helper()
		for _, k := range sortedKeys(forms) {
			assert.Contains(t, s, k+"=***", "form %s", k)
			assert.NotContains(t, s, forms[k], "form %s", k)
		}
	}
	t.Run("read", func(t *testing.T) {
		src := newSource(t, yaml, api(), newClock())
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeFullRefresh, "items"), "")
		require.Error(t, out.Err)
		masked(t, texts(out.Messages))
	})
	t.Run("check", func(t *testing.T) {
		src := newSource(t, yaml, api(), newClock())
		st := checkStatus(t, src, cfg)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		masked(t, st.Message)
	})
	t.Run("infer", func(t *testing.T) {
		out := infer(t, generic(api()), InferRequest{Config: json.RawMessage(withManifest(t, cfg, yaml))})
		require.NoError(t, out.err)
		require.Len(t, out.traces(), 1)
		masked(t, texts(out.msgs))
	})
}

func TestRedactorShortSecret(t *testing.T) {
	red := redactor{}
	red.add("k1")
	red.add("long-secret-1")
	tests := []struct{ in, want string }{
		{"bad key k1", "bad key ***"},
		{"k1", "***"},
		{"key=k1&x=1", "key=***&x=1"},
		{`{"key":"k1"}`, `{"key":"***"}`},
		{"k1abc bk1 ak12 k1_x", "k1abc bk1 ak12 k1_x"},
		{"echo xlong-secret-1y", "echo x***y"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, red.apply(tt.in), tt.in)
	}
}

func TestRedactingLogger(t *testing.T) {
	var buf strings.Builder
	red := redactor{}
	red.add("long-secret-1")
	log := slog.New(redactHandler{h: slog.NewTextHandler(&buf, nil), red: &red})
	log.With("k", "long-secret-1").Warn("echo long-secret-1", "v", "x long-secret-1")
	assert.NotContains(t, buf.String(), "long-secret-1")
	assert.Contains(t, buf.String(), "echo ***")
}

func TestRedactsRuntimeOAuthTokens(t *testing.T) {
	const access, rotated = "at-RUNTIME-7f3e9c1d", "rt-ROTATED-4b2a8e6f"
	api := func() *fakeAPI {
		return newAPI(t).
			json("/oauth/token", fmt.Sprintf(`{"access_token":%q,"refresh_token":%q,"expires_in":3600}`, access, rotated)).
			on("/v1/items", reply{status: 401, body: fmt.Sprintf(`{"error":"bad token %s, refresh %s"}`, access, rotated)})
	}
	masked := func(t *testing.T, s string) {
		t.Helper()
		assert.Contains(t, s, "bad token ***, refresh ***")
		assert.NotContains(t, s, access)
		assert.NotContains(t, s, rotated)
	}
	t.Run("read", func(t *testing.T) {
		a := api()
		src := oauthSource(t, a, newClock(), itemsStream)
		out := read(t, src, oauthConfig, catalog(t, src, oauthConfig, synclet.SyncModeFullRefresh, "items"), "")
		require.Error(t, out.Err)
		masked(t, texts(out.Messages))
	})
	t.Run("check", func(t *testing.T) {
		a := api()
		st := checkStatus(t, oauthSource(t, a, newClock(), itemsStream), oauthConfig)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		masked(t, st.Message)
	})
	t.Run("infer", func(t *testing.T) {
		a := api()
		yaml := manifestYAML(strings.Replace(oauthTop, "TOKEN_URL", a.srv.URL+"/oauth/token", 1), itemsStream)
		out := infer(t, generic(a), InferRequest{Config: json.RawMessage(withManifest(t, oauthConfig, yaml))})
		require.NoError(t, out.err)
		require.Len(t, out.traces(), 1)
		masked(t, texts(out.msgs))
	})
}

func TestRedactsAPIErrorMessage(t *testing.T) {
	const secret = "sk-FULL-ECHO-5a6b7c"
	cfg := fmt.Sprintf(`{"api_key":%q}`, secret)
	yaml := manifestYAML("errors: {message: $.error.message}\n", itemsStream)
	api := func() *fakeAPI {
		return newAPI(t).on("/v1/items", reply{status: 401, body: fmt.Sprintf(`{"error":{"message":"Invalid API Key provided: %s"}}`, secret)})
	}
	t.Run("check", func(t *testing.T) {
		st := checkStatus(t, newSource(t, yaml, api(), newClock()), cfg)
		assert.Equal(t, synclet.ConnectionStatusFailed, st.Status)
		assert.Contains(t, st.Message, "Invalid API Key provided: ***")
		assert.NotContains(t, st.Message, secret)
	})
	t.Run("read", func(t *testing.T) {
		src := newSource(t, yaml, api(), newClock())
		out := read(t, src, cfg, catalog(t, src, cfg, synclet.SyncModeFullRefresh, "items"), "")
		require.Error(t, out.Err)
		assert.Contains(t, texts(out.Messages), "Invalid API Key provided: ***")
		assert.NotContains(t, dump(t, out), secret)
	})
}

func TestRedactorForms(t *testing.T) {
	const user, secret = "svc-user", `pa"ss\wo rd/+<&>'?~`
	yaml := manifestYAML(`auth: {type: basic, username: "{{ config.user }}", password: "{{ config.api_key }}"}`+"\n", itemsStream)
	red := newRedactor(load(t, yaml), map[string]any{"api_key": secret, "user": user})
	b64 := func(s string) map[string]string {
		return map[string]string{
			"std": base64.StdEncoding.EncodeToString([]byte(s)), "raw": base64.RawStdEncoding.EncodeToString([]byte(s)),
			"url": base64.URLEncoding.EncodeToString([]byte(s)), "rawurl": base64.RawURLEncoding.EncodeToString([]byte(s)),
		}
	}
	u := func(hex ...string) string {
		var b strings.Builder
		for _, h := range hex {
			b.WriteString(`\` + "u00" + h)
		}
		return b.String()
	}
	forms := map[string]string{
		"json slash":      `pa\"ss\\wo rd\/+<&>'?~`,
		"json html slash": `pa\"ss\\wo rd\/+` + u("3c", "26", "3e") + `'?~`,
		"json upper":      `pa\"ss\\wo rd/+` + u("3C", "26", "3E") + `'?~`,
		"query lower":     lowerPercent(url.QueryEscape(secret)),
		"path lower":      lowerPercent(url.PathEscape(secret)),
		"html":            `pa&quot;ss\wo rd/+&lt;&amp;&gt;&#39;?~`,
	}
	for k, v := range b64(secret) {
		forms["password base64 "+k] = v
	}
	for k, v := range b64(user + ":" + secret) {
		forms["basic base64 "+k] = v
	}
	for _, k := range sortedKeys(forms) {
		assert.Equal(t, "x *** y", red.apply("x "+forms[k]+" y"), "form %s: %s", k, forms[k])
	}
}

func TestRedactorOverlappingSecrets(t *testing.T) {
	red := redactor{}
	red.add("AAAABBBB")
	red.add("BBBBCCCC")
	for _, in := range []string{"AAAABBBBCCCC", "x AAAABBBBCCCC y", "BBBBCCCCAAAABBBB"} {
		out := red.apply(in)
		for _, part := range []string{"AAAA", "BBBB", "CCCC"} {
			assert.NotContains(t, out, part, in)
		}
	}
	assert.Equal(t, "x *** y", red.apply("x AAAABBBBCCCC y"))
}

func TestRedactingLoggerGroups(t *testing.T) {
	var buf strings.Builder
	red := redactor{}
	red.add("long-secret-1")
	log := slog.New(redactHandler{h: slog.NewTextHandler(&buf, nil), red: &red})
	log.Warn("m", slog.Group("g", "k", "long-secret-1", slog.Group("h", "k", "x long-secret-1")))
	log.With(slog.Group("w", "k", "long-secret-1")).Warn("m")
	assert.NotContains(t, buf.String(), "long-secret-1")
	assert.Contains(t, buf.String(), "g.k=***")
	assert.Contains(t, buf.String(), `g.h.k="x ***"`)
	assert.Contains(t, buf.String(), "w.k=***")
}

type panicTransport struct{}

func (panicTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	panic("transport broke on " + req.Header.Get("Authorization"))
}

func TestRedactsPanics(t *testing.T) {
	const secret = "sk-PANIC-ECHO-1a2b3c"
	cfg := fmt.Sprintf(`{"api_key":%q}`, secret)
	src := func() synclet.Source[json.RawMessage] {
		o := newAPI(t).options(newClock())
		o.Client = &http.Client{Transport: panicTransport{}}
		return New(load(t, manifestYAML("", itemsStream)), o)
	}
	t.Run("check", func(t *testing.T) {
		out := runCmd(t, src(), synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(cfg)})
		s := dump(t, out)
		assert.Contains(t, s, "transport broke on Bearer ***")
		assert.NotContains(t, s, secret)
	})
	t.Run("read", func(t *testing.T) {
		s := src()
		out := read(t, s, cfg, catalog(t, s, cfg, synclet.SyncModeFullRefresh, "items"), "")
		require.Error(t, out.Err)
		d := dump(t, out)
		assert.Contains(t, d, "transport broke on Bearer ***")
		assert.NotContains(t, d, secret)
	})
}

func TestRedactPanicValue(t *testing.T) {
	red := redactor{}
	red.add("long-secret-1")
	assert.PanicsWithValue(t, "boom ***", func() { defer red.repanic(); panic("boom long-secret-1") })
	assert.PanicsWithError(t, "boom ***", func() { defer red.repanic(); panic(fmt.Errorf("boom %s", "long-secret-1")) })
	var nilRed *redactor
	assert.PanicsWithValue(t, "boom", func() { defer nilRed.repanic(); panic("boom") })
}
