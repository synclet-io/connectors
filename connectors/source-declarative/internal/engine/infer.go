package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"

	"source-declarative/internal/draft"
	"source-declarative/internal/manifest"
)

// InferRequest is one infer command of the builder: a sample read of the
// manifest in Config's __manifest. Streams selects streams, all when empty;
// Limit caps the records per stream and Pages the pages per stream,
// partition value and parent.
type InferRequest struct {
	Config  json.RawMessage
	Streams []string
	Limit   int
	Pages   int
}

// Infer runs one infer command of src and writes the protocol stream to out:
// RECORD messages, a LOG per request, a TRACE error per failed stream and
// LOG warnings, then one CATALOG whose streams carry draft schemas. It
// returns a non-nil error, already emitted as a TRACE error, only when the
// command itself fails: a catalog image, an invalid config or manifest.
func Infer(ctx context.Context, src synclet.Source[json.RawMessage], req InferRequest, out io.Writer) error {
	w := &lockedWriter{w: out}
	inf := &inferer{req: req, em: synclet.NewEmitter(w)}
	switch s := src.(type) {
	case *source:
		inf.s = s
	case failed:
		inf.err = s.err
	default:
		inf.err = fmt.Errorf("infer: unsupported source %T", src)
	}
	return synclet.ServeSource(ctx, inf, synclet.Request{Command: synclet.CommandDiscover, Config: req.Config}, w)
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// inferer serves infer as the SDK serves discover, so the SDK writes the
// final CATALOG or the command's TRACE error; records, request logs and
// stream errors go through em on the same writer before it.
type inferer struct {
	s   *source
	err error
	req InferRequest
	em  *synclet.Emitter
}

var errNotInfer = errors.New("infer serves discover only")

func (*inferer) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	return nil, errNotInfer
}
func (*inferer) Check(context.Context, json.RawMessage) error { return errNotInfer }
func (*inferer) Read(context.Context, json.RawMessage, *synclet.ConfiguredCatalog, *synclet.InputState, *synclet.Emitter) error {
	return errNotInfer
}

var (
	errLimit = errors.New("record limit reached")
	errPages = errors.New("page limit reached")
)

func (inf *inferer) Discover(ctx context.Context, raw json.RawMessage) (*synclet.Catalog, error) {
	if inf.err != nil {
		return nil, inf.err
	}
	if inf.s.m != nil {
		return nil, synclet.ConfigError(errors.New("infer runs on the generic source-declarative image only; this connector's manifest is built into its image"))
	}
	m, cfg, err := inf.s.resolve(raw, true)
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for _, name := range inf.req.Streams {
		if !slices.ContainsFunc(m.Streams, func(st manifest.Stream) bool { return st.Name == name }) {
			return nil, synclet.ConfigError(fmt.Errorf("unknown stream %q", name))
		}
		selected[name] = true
	}
	o := inf.s.o
	base := http.DefaultTransport
	if o.Client.Transport != nil {
		base = o.Client.Transport
	}
	c := *o.Client
	c.Transport = &requestLog{base: base, queryKey: m.Auth.Type == manifest.AuthAPIKey && m.Auth.In == "query", red: newRedactor(m, cfg)}
	o.Client = &c
	r := newRun(m, cfg, o, inf.em)
	red := r.red
	log := r.logger(ctx)

	cat := &synclet.Catalog{}
	for i := range m.Streams {
		st := &m.Streams[i]
		if len(selected) > 0 && !selected[st.Name] {
			continue
		}
		desc := synclet.StreamDescriptor{Name: st.Name}
		recs, err := r.sample(ctx, st, desc, inf.req.Limit, inf.req.Pages)
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if err != nil {
			ft := synclet.FailureTypeSystemError
			if isConfigError(err) {
				ft = synclet.FailureTypeConfigError
			}
			trace := synclet.TraceMessage{Type: synclet.TraceTypeError, EmittedAt: float64(time.Now().UnixMilli()), Error: &synclet.ErrorTrace{
				Message:          red.apply(fmt.Sprintf("stream %s: %v", st.Name, err)),
				FailureType:      ft,
				StreamDescriptor: &desc,
			}}
			if err := inf.em.Trace(trace); err != nil {
				return nil, err
			}
			continue
		}
		schema := draft.Schema(recs)
		if len(recs) == 0 {
			log.Warn(fmt.Sprintf("%s: no records sampled; the draft schema is empty", st.Name))
		}
		if st.Schema == nil {
			for _, e := range manifest.SchemaFieldErrors(m, i, schema) {
				log.Warn(red.apply(fmt.Sprintf("draft schema of %s: %s.%s: %s", st.Name, ManifestKey, e.Path, e.Message)))
			}
		}
		cat.Streams = append(cat.Streams, catalogStream(st, schema))
	}
	return cat, nil
}

// sample reads st as a full refresh from start, the first window only: a
// partition.values stream for its first value, a child over its parent's
// records in order; at most pages pages per scope and limit records, which
// are emitted as read emits them and returned. A child samples its first
// parents up to limit, whatever their children return.
func (r *run) sample(ctx context.Context, st *manifest.Stream, desc synclet.StreamDescriptor, limit, pages int) ([]map[string]any, error) {
	timed := st
	parent := parentOf(st)
	if parent != "" {
		timed = r.stream(parent)
	}
	tm, err := r.timing(timed.Incremental)
	if err != nil {
		return nil, err
	}
	scope := r.scope()
	if tm != nil {
		w := window{start: tm.start, end: tm.now, set: true}
		if wins := tm.windows(tm.start); len(wins) > 0 {
			w = wins[0]
		} else if win := tm.inc.Window; win != nil {
			w.end = tm.start.Add(time.Duration(win.Step))
		}
		scope = bounds(scope, w, tm.sinceFormat(timed.Incremental))
	}
	if p := st.Partition; p != nil && p.Values != nil {
		vals, err := r.partitionValues(st)
		if err != nil {
			return nil, err
		}
		if len(vals) == 0 {
			return nil, synclet.ConfigError(errors.New("no partition value to sample"))
		}
		scope.Partition = &vals[0]
	}
	t := r.newTracker(&member{st: st, desc: desc}, tm, nil)
	var recs []map[string]any
	take := func(pg page) error {
		for _, rec := range pg.records {
			if t != nil {
				ok, err := t.accept(rec, pg.resp.desc)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
			}
			if err := r.em.Record(desc, rec); err != nil {
				return err
			}
			recs = append(recs, rec)
			if len(recs) >= limit {
				return errLimit
			}
		}
		return nil
	}
	if parent == "" {
		err = r.pages(ctx, st, scope, false, capPages(pages, take))
		return recs, sampleErr(err)
	}
	ps := r.stream(parent)
	pt := r.newTracker(&member{st: ps, desc: synclet.StreamDescriptor{Name: ps.Name}}, tm, nil)
	sampled := 0
	err = r.pages(ctx, ps, scope, false, capPages(pages, func(pg page) error {
		for _, rec := range pg.records {
			if pt != nil {
				ok, err := pt.accept(rec, pg.resp.desc)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
			}
			cs := scope
			cs.Parent = rec
			if err := sampleErr(r.pages(ctx, st, cs, false, capPages(pages, take))); err != nil {
				return err
			}
			sampled++
			if len(recs) >= limit || sampled >= limit {
				return errLimit
			}
		}
		return nil
	}))
	return recs, sampleErr(err)
}

// capPages stops a stream's pages after n pages.
func capPages(n int, fn func(page) error) func(page) error {
	seen := 0
	return func(pg page) error {
		if err := fn(pg); err != nil {
			return err
		}
		seen++
		if seen >= n {
			return errPages
		}
		return nil
	}
}

// sampleErr drops the ends of a sample that are not failures.
func sampleErr(err error) error {
	if errors.Is(err, errLimit) || errors.Is(err, errPages) || errors.Is(err, errSkipped) {
		return nil
	}
	return err
}

var configErrorType = reflect.TypeOf(synclet.ConfigError(errors.New("")))

func isConfigError(err error) bool {
	return errors.As(err, reflect.New(configErrorType).Interface())
}

// requestLog logs every request as method and path, with the query unless
// the credential travels in it.
type requestLog struct {
	base     http.RoundTripper
	queryKey bool
	red      redactor
}

func (l *requestLog) RoundTrip(req *http.Request) (*http.Response, error) {
	d := req.Method + " " + req.URL.EscapedPath()
	if req.URL.RawQuery != "" && !l.queryKey {
		d += "?" + req.URL.RawQuery
	}
	synclet.Logger(req.Context()).Info(l.red.apply("request: " + d))
	return l.base.RoundTrip(req)
}

// redactor masks the values of the config's secret properties.
type redactor struct{ secrets []string }

func newRedactor(m *manifest.Manifest, cfg map[string]any) redactor {
	var out []string
	for name, p := range m.Spec.Properties {
		if !p.Secret {
			continue
		}
		switch v := cfg[name].(type) {
		case string:
			if v != "" {
				out = append(out, v)
			}
		case json.Number:
			out = append(out, v.String())
		}
	}
	sort.Slice(out, func(i, j int) bool { return len(out[i]) > len(out[j]) })
	return redactor{secrets: out}
}

func (r redactor) apply(s string) string {
	for _, v := range r.secrets {
		s = strings.ReplaceAll(s, v, "***")
	}
	return s
}
