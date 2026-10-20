package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"

	"source-declarative/internal/manifest"
)

const (
	defaultTimeout    = 60 * time.Second
	defaultBackoff    = time.Second
	defaultAttempts   = 3
	defaultWaits      = 5
	defaultRateWait   = 60 * time.Second
	maxRateWait       = 10 * time.Minute
	refreshMargin     = 60 * time.Second
	errorSnippetBytes = 200
)

var defaultTransient = []int{500, 502, 503, 504}

// run is one command of one manifest with one config. Requests are
// sequential, so it needs no lock.
type run struct {
	m   *manifest.Manifest
	cfg map[string]any
	o   Options
	em  *synclet.Emitter
	red *redactor

	base     *url.URL
	baseErr  error
	token    string
	tokenSet bool
	expiry   time.Time
}

func newRun(m *manifest.Manifest, cfg map[string]any, o Options, em *synclet.Emitter) *run {
	if o.Client != nil {
		c := *o.Client
		c.CheckRedirect = sameHostRedirect
		o.Client = &c
	}
	r := &run{m: m, cfg: cfg, o: o, em: em, red: newRedactor(m, cfg)}
	base := o.BaseURL
	if base == "" {
		var err error
		if base, err = m.BaseURL.Render(r.scope(), nil); err != nil {
			r.baseErr = synclet.ConfigError(fmt.Errorf("base_url: %w", err))
			return r
		}
	}
	u, err := url.Parse(base)
	if err != nil || !u.IsAbs() {
		r.baseErr = synclet.ConfigError(fmt.Errorf("base_url %q is not an absolute URL", base))
		return r
	}
	r.base = u
	return r
}

func (r *run) scope() manifest.Scope { return manifest.Scope{Config: r.cfg} }

// response is a 2xx response with its body decoded with exact numbers.
// ignored is set when an ignore rule of errors.when matched it.
type response struct {
	header  http.Header
	body    []byte
	doc     any
	desc    string
	ignored *httpError
}

// httpError is a non-2xx response that is not retried.
type httpError struct {
	status int
	desc   string
	detail string
}

func (e *httpError) Error() string {
	return fmt.Sprintf("%s (HTTP %d): %s", e.desc, e.status, e.detail)
}

// describe names a request in errors: method and path, with the query
// unless the credential travels in it.
func (r *run) describe(method string, u *url.URL) string {
	d := method + " " + u.EscapedPath()
	if u.RawQuery != "" && !r.keyInQuery() {
		d += "?" + u.RawQuery
	}
	return d
}

func (r *run) keyInQuery() bool {
	return r.m.Auth.Type == manifest.AuthAPIKey && r.m.Auth.In == "query"
}

// get sends a GET of u with the stream's headers and the manifest's auth.
func (r *run) get(ctx context.Context, st *manifest.Stream, u *url.URL, scope manifest.Scope) (*response, error) {
	header, err := r.headers(st, scope)
	if err != nil {
		return nil, err
	}
	if r.keyInQuery() {
		v, err := r.m.Auth.Value.Render(r.scope(), nil)
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("auth.value: %w", err))
		}
		q := u.Query()
		q.Set(r.m.Auth.Name, v)
		cp := *u
		cp.RawQuery = q.Encode()
		u = &cp
	}
	desc := r.describe(http.MethodGet, u)
	if u.Scheme != r.base.Scheme || u.Host != r.base.Host {
		return nil, fmt.Errorf("%s: %s is not on the base URL's scheme and host", desc, u.Scheme+"://"+u.Host)
	}
	rules := slices.Concat(st.Errors.When, r.m.Errors.When)
	return r.send(ctx, desc, rules, true, func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		req.Header = header.Clone()
		return req, nil
	})
}

// headers merges the default, top-level and stream headers.
func (r *run) headers(st *manifest.Stream, scope manifest.Scope) (http.Header, error) {
	h := http.Header{}
	h.Set("Accept", "application/json")
	h.Set("User-Agent", "synclet/"+r.m.Connector.Name)
	for _, set := range []struct {
		at string
		ts map[string]manifest.Template
		s  manifest.Scope
	}{{"headers", r.m.Headers, r.scope()}, {"streams." + st.Name + ".headers", st.Headers, scope}} {
		for _, k := range sortedKeys(set.ts) {
			t := set.ts[k]
			v, err := t.Render(set.s, nil)
			if err != nil {
				return nil, fmt.Errorf("%s.%s: %w", set.at, k, err)
			}
			if strings.ContainsAny(v, "\r\n") {
				return nil, fmt.Errorf("%s.%s renders a line break", set.at, k)
			}
			if v == "" {
				h.Del(k)
				continue
			}
			h.Set(k, v)
		}
	}
	switch a := r.m.Auth; a.Type {
	case manifest.AuthAPIKey:
		if a.In == "header" {
			v, err := a.Value.Render(r.scope(), nil)
			if err != nil {
				return nil, synclet.ConfigError(fmt.Errorf("auth.value: %w", err))
			}
			h.Set(a.Name, v)
		}
	case manifest.AuthBearer:
		v, err := a.Token.Render(r.scope(), nil)
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("auth.token: %w", err))
		}
		h.Set("Authorization", "Bearer "+v)
	case manifest.AuthBasic:
		u, err := a.Username.Render(r.scope(), nil)
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("auth.username: %w", err))
		}
		p, err := a.Password.Render(r.scope(), nil)
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("auth.password: %w", err))
		}
		h.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(u+":"+p)))
	}
	return h, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// send sends the request build makes until it succeeds or fails for good:
// transient statuses, network errors and timeouts are retried with a
// doubling backoff, rate limits wait for what the API says, and an OAuth
// token is refreshed before it expires and once after a 401. The first of
// rules that matches a 2xx body handles that response as an error.
func (r *run) send(ctx context.Context, desc string, rules []manifest.BodyRule, authed bool, build func(context.Context) (*http.Request, error)) (*response, error) {
	tr := r.m.Retry.Transient
	statuses, attempts, delay := tr.Statuses, tr.Attempts, time.Duration(tr.Backoff)
	if statuses == nil {
		statuses = defaultTransient
	}
	if attempts == 0 {
		attempts = defaultAttempts
	}
	if delay == 0 {
		delay = defaultBackoff
	}
	rl := r.m.Retry.RateLimit
	maxWaits := rl.Attempts
	if maxWaits == 0 {
		maxWaits = defaultWaits
	}
	oauth := authed && r.m.Auth.Type == manifest.AuthOAuth2Refresh
	var failures, waits int
	refreshed := false
	for {
		if oauth && (!r.tokenSet || (!r.expiry.IsZero() && !r.o.Now().Before(r.expiry.Add(-refreshMargin)))) {
			if err := r.refresh(ctx); err != nil {
				return nil, err
			}
		}
		status, header, body, err := r.attempt(ctx, build, oauth)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			var fe fatal
			if errors.As(err, &fe) {
				return nil, fe.err
			}
			failures++
			if failures >= attempts {
				return nil, fmt.Errorf("%s: %w; gave up after %d attempts", desc, err, attempts)
			}
			if err := r.o.Sleep(ctx, delay); err != nil {
				return nil, err
			}
			delay *= 2
			continue
		}
		action := ""
		if status >= 200 && status < 300 {
			resp := &response{header: header, body: body, desc: desc}
			if err := decodeBody(body, &resp.doc); err != nil {
				return nil, fmt.Errorf("%s: response is not JSON: %w", desc, err)
			}
			for i := range rules {
				if rules[i].Matches(resp.doc) {
					action = rules[i].Action
					break
				}
			}
			if action == "" {
				return resp, nil
			}
			if action == manifest.ActionIgnore {
				resp.ignored = &httpError{status: status, desc: desc, detail: r.errorMessage(body)}
				return resp, nil
			}
		}
		he := &httpError{status: status, desc: desc, detail: r.errorMessage(body)}
		switch {
		case action == manifest.ActionFail:
			return nil, he
		case action == manifest.ActionConfigError:
			return nil, synclet.ConfigError(he)
		case status == http.StatusUnauthorized && oauth && !refreshed:
			refreshed = true
			if err := r.refresh(ctx); err != nil {
				return nil, err
			}
			continue
		case action == manifest.ActionRateLimit || (action == "" && r.rateLimited(status, header)):
			waits++
			if waits > maxWaits {
				return nil, fmt.Errorf("%w; gave up after %d waits", he, maxWaits)
			}
			wait := r.rateWait(header, body)
			if wait > maxRateWait {
				return nil, fmt.Errorf("%w; the API asks to wait %s, more than the %s limit", he, wait, maxRateWait)
			}
			if err := r.o.Sleep(ctx, wait); err != nil {
				return nil, err
			}
			continue
		case action == "" && (status == http.StatusUnauthorized || status == http.StatusForbidden):
			return nil, synclet.ConfigError(he)
		case action == manifest.ActionRetry || (action == "" && slices.Contains(statuses, status)):
			failures++
			if failures >= attempts {
				return nil, fmt.Errorf("%w; gave up after %d attempts", he, attempts)
			}
			if err := r.o.Sleep(ctx, delay); err != nil {
				return nil, err
			}
			delay *= 2
			continue
		}
		return nil, he
	}
}

// fatal is a failure of attempt that no retry can fix.
type fatal struct{ err error }

func (f fatal) Error() string { return f.err.Error() }

// attempt sends one request with a per-request timeout and reads its body.
func (r *run) attempt(ctx context.Context, build func(context.Context) (*http.Request, error), oauth bool) (int, http.Header, []byte, error) {
	timeout := defaultTimeout
	if r.m.Timeout != nil {
		timeout = time.Duration(*r.m.Timeout)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := build(ctx)
	if err != nil {
		return 0, nil, nil, fatal{err}
	}
	if oauth {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	resp, err := r.o.Client.Do(req)
	if err != nil {
		var re *redirectError
		if errors.As(err, &re) || errors.Is(err, errTooManyRedirects) {
			return 0, nil, nil, fatal{fmt.Errorf("%s %s: %w", req.Method, req.URL.EscapedPath(), errors.Unwrap(err))}
		}
		return 0, nil, nil, transportError(err)
	}
	body, rerr := io.ReadAll(resp.Body)
	if cerr := resp.Body.Close(); rerr == nil {
		rerr = cerr
	}
	if rerr != nil {
		return 0, nil, nil, fmt.Errorf("read body: %w", rerr)
	}
	return resp.StatusCode, resp.Header, body, nil
}

var errTooManyRedirects = errors.New("stopped after 10 redirects")

// redirectError is a redirect that would leave the host of the request,
// carrying the credential of the query or headers with it.
type redirectError struct{ target string }

func (e *redirectError) Error() string {
	return fmt.Sprintf("redirect to %s is not on the base URL's scheme and host", e.target)
}

// sameHostRedirect follows a redirect only to the scheme and host of the
// first request, at most 10 times.
func sameHostRedirect(req *http.Request, via []*http.Request) error {
	first := via[0].URL
	if req.URL.Scheme != first.Scheme || req.URL.Host != first.Host {
		return &redirectError{target: req.URL.Scheme + "://" + req.URL.Host}
	}
	if len(via) >= 10 {
		return errTooManyRedirects
	}
	return nil
}

// transportError describes a failed round trip without the request URL,
// which may carry the credential in its query.
func transportError(err error) error {
	var ue *url.Error
	if !errors.As(err, &ue) {
		return err
	}
	if ue.Timeout() {
		return errors.New("request timed out")
	}
	var dns *net.DNSError
	if errors.As(ue.Err, &dns) {
		return fmt.Errorf("DNS lookup of %s failed", dns.Name)
	}
	var op *net.OpError
	if errors.As(ue.Err, &op) {
		if op.Addr != nil {
			return fmt.Errorf("%s %s: %v", op.Op, op.Addr, op.Err)
		}
		return fmt.Errorf("%s: %v", op.Op, op.Err)
	}
	return fmt.Errorf("request failed (%T)", ue.Err)
}

func decodeBody(b []byte, out *any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		return err
	}
	if d.More() {
		return errors.New("more than one JSON value")
	}
	return nil
}

// errorMessage is the API's error text: the errors.message path of the
// body, else its first 200 bytes.
func (r *run) errorMessage(body []byte) string {
	if p := r.m.Errors.Message; p != nil {
		var doc any
		if decodeBody(body, &doc) == nil {
			if v, ok := p.Get(doc); ok {
				if s, ok := v.(string); ok && s != "" {
					return s
				}
				if v != nil {
					b, _ := json.Marshal(v)
					return string(b)
				}
			}
		}
	}
	s := strings.TrimSpace(r.red.apply(string(body)))
	if len(s) > errorSnippetBytes {
		s = s[:errorSnippetBytes]
	}
	return s
}

func (r *run) rateLimited(status int, h http.Header) bool {
	rl := r.m.Retry.RateLimit
	statuses := rl.Statuses
	if statuses == nil {
		statuses = []int{http.StatusTooManyRequests}
	}
	if slices.Contains(statuses, status) {
		return true
	}
	aw := rl.AlsoWhen
	return aw != nil && aw.Status == status && h.Get(aw.Header) == aw.Equals
}

// rateWait is the first wait of rate_limit.wait the response answers.
func (r *run) rateWait(h http.Header, body []byte) time.Duration {
	waits := r.m.Retry.RateLimit.Wait
	if waits == nil {
		waits = []manifest.Wait{{Kind: manifest.WaitHeader, Name: "Retry-After"}, {Kind: manifest.WaitFixed, Fixed: defaultRateWait}}
	}
	now := r.o.Now()
	for _, w := range waits {
		switch w.Kind {
		case manifest.WaitHeader:
			v := strings.TrimSpace(h.Get(w.Name))
			if v == "" {
				continue
			}
			if n, err := strconv.ParseFloat(v, 64); err == nil && n >= 0 {
				return time.Duration(n * float64(time.Second))
			}
			if t, err := http.ParseTime(v); err == nil {
				return max(t.Sub(now), 0)
			}
		case manifest.WaitHeaderEpoch:
			if n, err := strconv.ParseInt(strings.TrimSpace(h.Get(w.Name)), 10, 64); err == nil {
				return max(time.Unix(n, 0).Sub(now), 0)
			}
		case manifest.WaitBody:
			var doc any
			if decodeBody(body, &doc) != nil {
				continue
			}
			v, ok := w.Path.Get(doc)
			if !ok {
				continue
			}
			if n, ok := v.(json.Number); ok {
				if ms, err := n.Float64(); err == nil && ms >= 0 {
					return time.Duration(ms * float64(time.Millisecond))
				}
			}
		case manifest.WaitFixed:
			return w.Fixed
		}
	}
	return defaultRateWait
}

// refresh gets a new access token with the refresh token.
func (r *run) refresh(ctx context.Context) error {
	a := r.m.Auth
	vals := map[string]string{"grant_type": "refresh_token"}
	for key, t := range map[string]manifest.Template{"client_id": a.ClientID, "client_secret": a.ClientSecret, "refresh_token": a.RefreshToken} {
		v, err := t.Render(r.scope(), nil)
		if err != nil {
			return synclet.ConfigError(fmt.Errorf("auth.%s: %w", key, err))
		}
		vals[key] = v
	}
	if len(a.Scopes) > 0 {
		vals["scope"] = strings.Join(a.Scopes, " ")
	}
	tokenURL, err := a.TokenURL.Render(r.scope(), nil)
	if err != nil {
		return synclet.ConfigError(fmt.Errorf("auth.token_url: %w", err))
	}
	u, err := url.Parse(tokenURL)
	if err != nil {
		return synclet.ConfigError(fmt.Errorf("auth.token_url: %w", err))
	}
	var body []byte
	contentType := "application/x-www-form-urlencoded"
	if a.Body == "json" {
		contentType = "application/json"
		if body, err = json.Marshal(vals); err != nil {
			return err
		}
	} else {
		form := url.Values{}
		for k, v := range vals {
			form.Set(k, v)
		}
		body = []byte(form.Encode())
	}
	desc := "POST " + u.EscapedPath()
	resp, err := r.send(ctx, desc, nil, false, func(ctx context.Context) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "synclet/"+r.m.Connector.Name)
		return req, nil
	})
	if err != nil {
		var he *httpError
		if errors.As(err, &he) && he.status >= 400 && he.status < 500 {
			return synclet.ConfigError(fmt.Errorf("oauth2 refresh: %w", err))
		}
		return fmt.Errorf("oauth2 refresh: %w", err)
	}
	atPath, expPath := a.AccessTokenPath, a.ExpiresInPath
	if atPath == nil {
		atPath, _ = manifest.ParsePath("$.access_token")
	}
	if expPath == nil {
		expPath, _ = manifest.ParsePath("$.expires_in")
	}
	v, _ := atPath.Get(resp.doc)
	token, ok := v.(string)
	if !ok || token == "" {
		return fmt.Errorf("oauth2 refresh: %s: no access token at %s", desc, atPath.Raw)
	}
	r.red.register(token)
	r.token, r.tokenSet, r.expiry = token, true, time.Time{}
	if v, ok := expPath.Get(resp.doc); ok && v != nil {
		n, ok := v.(json.Number)
		secs, okRat := new(big.Rat).SetString(string(n))
		if !ok || !okRat {
			return fmt.Errorf("oauth2 refresh: %s: expires_in %v is not a number", desc, v)
		}
		f, _ := secs.Float64()
		r.expiry = r.o.Now().Add(time.Duration(f * float64(time.Second)))
	}
	if rt, _ := resp.doc.(map[string]any)["refresh_token"].(string); rt != "" && rt != vals["refresh_token"] {
		r.red.register(rt)
		r.logger(ctx).Warn("the token endpoint returned a new refresh_token; it is ignored, the configured one stays in use (APIs that rotate refresh tokens need a Go connector)")
	}
	return nil
}

func (r *run) logger(ctx context.Context) *slog.Logger { return r.red.logger(ctx) }
