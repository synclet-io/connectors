package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

// redactor masks the values of the config's secret properties, their
// JSON-escaped, URL query-escaped and path-escaped forms, and the basic-auth
// credential, longest form first.
type redactor struct{ secrets []string }

func newRedactor(m *manifest.Manifest, cfg map[string]any) redactor {
	var r redactor
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
			r.add(base64.StdEncoding.EncodeToString([]byte(u + ":" + p)))
		}
	}
	r.finish()
	return r
}

// add registers v and its encoded forms.
func (r *redactor) add(v string) {
	if v == "" {
		return
	}
	for _, f := range []string{v, jsonEscape(v, true), jsonEscape(v, false), url.QueryEscape(v), url.PathEscape(v)} {
		if !slices.Contains(r.secrets, f) {
			r.secrets = append(r.secrets, f)
		}
	}
}

// finish orders the forms longest first, so a form is masked before any
// shorter form it contains.
func (r *redactor) finish() {
	slices.SortStableFunc(r.secrets, func(a, b string) int { return len(b) - len(a) })
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

func (r redactor) apply(s string) string {
	for _, v := range r.secrets {
		if len(v) >= minSecretLen {
			s = strings.ReplaceAll(s, v, "***")
		} else {
			s = replaceTokens(s, v)
		}
	}
	return s
}

// replaceTokens masks the occurrences of v that no word byte adjoins.
func replaceTokens(s, v string) string {
	var b strings.Builder
	for {
		i := strings.Index(s, v)
		if i < 0 {
			break
		}
		end := i + len(v)
		if (i > 0 && isWordByte(s[i-1])) || (end < len(s) && isWordByte(s[end])) {
			b.WriteString(s[:i+1])
			s = s[i+1:]
			continue
		}
		b.WriteString(s[:i])
		b.WriteString("***")
		s = s[end:]
	}
	b.WriteString(s)
	return b.String()
}

// isWordByte reports whether c is a letter, a digit, '_' or a byte of a
// multibyte UTF-8 character.
func isWordByte(c byte) bool {
	return c == '_' || c >= 0x80 || ('0' <= c && c <= '9') || ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z')
}

// wrap returns err with its message redacted; errors.As and errors.Is see
// err through it.
func (r redactor) wrap(err error) error {
	if err == nil {
		return nil
	}
	return redactedError{err: err, red: r}
}

type redactedError struct {
	err error
	red redactor
}

func (e redactedError) Error() string { return e.red.apply(e.err.Error()) }
func (e redactedError) Unwrap() error { return e.err }

// logger is the connector's logger with messages and attribute values redacted.
func (r redactor) logger(ctx context.Context) *slog.Logger {
	return slog.New(redactHandler{h: synclet.Logger(ctx).Handler(), red: r})
}

type redactHandler struct {
	h   slog.Handler
	red redactor
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
