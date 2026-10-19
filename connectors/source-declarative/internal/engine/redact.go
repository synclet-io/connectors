package engine

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
	"log/slog"
	"net/url"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"

	"source-declarative/internal/manifest"
)

// minSecretLen is the length from which a secret form is masked wherever it
// occurs; a shorter one is masked only as a whole token, so that a short
// value does not blank out the words and numbers that contain it.
const minSecretLen = 8

// redactor masks the values of the config's secret properties, the
// basic-auth password and credential, and their encoded forms: JSON-escaped
// (with or without HTML escaping, escaped slashes, upper-case hex escapes),
// URL query- and path-escaped (either hex case), HTML-escaped and base64 in
// every alphabet with and without padding. A run's goroutine owns its
// redactor, so register needs no lock.
type redactor struct{ secrets []string }

func newRedactor(m *manifest.Manifest, cfg map[string]any) *redactor {
	r := &redactor{}
	for name, p := range m.Spec.Properties {
		if !p.Secret {
			continue
		}
		switch v := cfg[name].(type) {
		case string:
			r.add(v)
		case json.Number:
			r.add(v.String())
		}
	}
	if a := m.Auth; a.Type == manifest.AuthBasic {
		sc := manifest.Scope{Config: cfg}
		u, uerr := a.Username.Render(sc, nil)
		p, perr := a.Password.Render(sc, nil)
		if uerr == nil && perr == nil {
			r.add(p)
			r.addForms(base64Forms(u + ":" + p))
		}
	}
	return r
}

// register adds a secret obtained at run time, such as an OAuth token.
func (r *redactor) register(v string) { r.add(v) }

// add registers v and its encoded forms.
func (r *redactor) add(v string) {
	if v == "" {
		return
	}
	var forms []string
	for _, j := range []string{jsonEscape(v, true), jsonEscape(v, false)} {
		slash := strings.ReplaceAll(j, "/", `\/`)
		forms = append(forms, j, slash, upperHexEscapes(j), upperHexEscapes(slash))
	}
	for _, e := range []string{url.QueryEscape(v), url.PathEscape(v)} {
		forms = append(forms, e, lowerPercent(e))
	}
	forms = append(forms, v, htmlEscaper.Replace(v), html.EscapeString(v))
	r.addForms(append(forms, base64Forms(v)...))
}

func (r *redactor) addForms(forms []string) {
	for _, f := range forms {
		if f != "" && !slices.Contains(r.secrets, f) {
			r.secrets = append(r.secrets, f)
		}
	}
}

var htmlEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", `"`, "&quot;", "'", "&#39;")

func base64Forms(v string) []string {
	b := []byte(v)
	return []string{
		base64.StdEncoding.EncodeToString(b), base64.RawStdEncoding.EncodeToString(b),
		base64.URLEncoding.EncodeToString(b), base64.RawURLEncoding.EncodeToString(b),
	}
}

func jsonEscape(v string, html bool) string {
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(html)
	if err := enc.Encode(v); err != nil {
		return v
	}
	s := strings.TrimSuffix(b.String(), "\n")
	return s[1 : len(s)-1]
}

// upperHexEscapes upper-cases the hex digits of the four-digit escapes of a
// JSON string body.
func upperHexEscapes(s string) string {
	b := []byte(s)
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' || i+1 >= len(b) {
			continue
		}
		if b[i+1] == 'u' && i+5 < len(b) {
			copy(b[i+2:i+6], bytes.ToUpper(b[i+2:i+6]))
		}
		i++
	}
	return string(b)
}

// lowerPercent lower-cases the hex digits of the percent escapes of s.
func lowerPercent(s string) string {
	b := []byte(s)
	for i := 0; i+2 < len(b); i++ {
		if b[i] == '%' {
			copy(b[i+1:i+3], bytes.ToLower(b[i+1:i+3]))
			i += 2
		}
	}
	return string(b)
}

// apply replaces each maximal run of s that secret forms cover, overlapping
// or adjacent, with "***". A form of minSecretLen bytes or more is masked
// wherever it occurs; a shorter one only where no unmasked word byte adjoins
// it.
func (r *redactor) apply(s string) string {
	var masked []bool
	mark := func(short bool) {
		for _, v := range r.secrets {
			if (len(v) < minSecretLen) != short {
				continue
			}
			for i := 0; i+len(v) <= len(s); {
				j := strings.Index(s[i:], v)
				if j < 0 {
					break
				}
				start, end := i+j, i+j+len(v)
				i = start + 1
				if short && (wordAt(s, masked, start-1) || wordAt(s, masked, end)) {
					continue
				}
				if masked == nil {
					masked = make([]bool, len(s))
				}
				for k := start; k < end; k++ {
					masked[k] = true
				}
			}
		}
	}
	mark(false)
	mark(true)
	if masked == nil {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if !masked[i] {
			b.WriteByte(s[i])
			continue
		}
		b.WriteString("***")
		for i+1 < len(s) && masked[i+1] {
			i++
		}
	}
	return b.String()
}

// wordAt reports whether s has an unmasked word byte at i.
func wordAt(s string, masked []bool, i int) bool {
	return i >= 0 && i < len(s) && (masked == nil || !masked[i]) && isWordByte(s[i])
}

// isWordByte reports whether c is a letter, a digit, '_' or a byte of a
// multibyte UTF-8 character.
func isWordByte(c byte) bool {
	return c == '_' || c >= 0x80 || ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

// wrap returns err with its message redacted; errors.As and errors.Is see
// err through it.
func (r *redactor) wrap(err error) error {
	if err == nil {
		return nil
	}
	return redactedError{err: err, red: r}
}

type redactedError struct {
	err error
	red *redactor
}

func (e redactedError) Error() string { return e.red.apply(e.err.Error()) }
func (e redactedError) Unwrap() error { return e.err }

// logger is the connector's logger with messages and attribute values redacted.
func (r *redactor) logger(ctx context.Context) *slog.Logger {
	return slog.New(redactHandler{h: synclet.Logger(ctx).Handler(), red: r})
}

type redactHandler struct {
	h   slog.Handler
	red *redactor
}

func (h redactHandler) Enabled(ctx context.Context, l slog.Level) bool { return h.h.Enabled(ctx, l) }

func (h redactHandler) Handle(ctx context.Context, rec slog.Record) error {
	out := slog.NewRecord(rec.Time, rec.Level, h.red.apply(rec.Message), rec.PC)
	rec.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(h.attr(a))
		return true
	})
	return h.h.Handle(ctx, out)
}

func (h redactHandler) WithAttrs(as []slog.Attr) slog.Handler {
	out := make([]slog.Attr, len(as))
	for i, a := range as {
		out[i] = h.attr(a)
	}
	return redactHandler{h: h.h.WithAttrs(out), red: h.red}
}

func (h redactHandler) WithGroup(name string) slog.Handler {
	return redactHandler{h: h.h.WithGroup(name), red: h.red}
}

func (h redactHandler) attr(a slog.Attr) slog.Attr {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		g := a.Value.Group()
		out := make([]any, len(g))
		for i, x := range g {
			out[i] = h.attr(x)
		}
		return slog.Group(a.Key, out...)
	}
	s := a.Value.String()
	if r := h.red.apply(s); r != s {
		a.Value = slog.StringValue(r)
	}
	return a
}

// repanic, deferred, re-panics a recovered panic with its value redacted; a
// nil redactor re-panics it unchanged.
func (r *redactor) repanic() {
	v := recover()
	if v == nil {
		return
	}
	if r == nil {
		panic(v)
	}
	if err, ok := v.(error); ok {
		panic(r.wrap(err))
	}
	panic(r.apply(fmt.Sprint(v)))
}
