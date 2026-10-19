package main

import (
	"bytes"
	"compress/gzip"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Result formats the fake answers queries in.
const (
	formatJSON  = "json"
	formatArrow = "arrow"
)

// resultFormats are the formats a test of result decoding runs in.
var resultFormats = []string{formatJSON, formatArrow}

// fakeQRMK is the result master key the fake hands out with chunked
// results and expects back on every chunk download.
const fakeQRMK = "fake-qrmk"

// fakeColumn is one column of a fake result: its name and the driver's
// type of it (text, fixed, real).
type fakeColumn struct {
	name string
	typ  string
}

// fakeQuery answers a query whose SQL text and bound values equal sql and
// bindings, with rows or with a Snowflake error. Rows of chunks follow rows,
// each chunk downloaded from its own URL; failChunk, when set, is the
// 1-based chunk that answers 403.
type fakeQuery struct {
	sql       string
	bindings  []string
	columns   []fakeColumn
	rows      [][]*string
	chunks    [][][]*string
	failChunk int
	errCode   string
	errMsg    string
}

// fakeChunk is a result chunk the fake serves at /chunk/N.
type fakeChunk struct {
	body   []byte
	status int
}

// fakeSnowflake speaks the part of Snowflake's HTTP protocol gosnowflake
// uses: login, query and session close. It fails the test on any request
// or query no route matches.
type fakeSnowflake struct {
	t *testing.T

	user     string
	password string
	key      *rsa.PublicKey
	// loginStatus, when set, is the HTTP status of every login.
	loginStatus int
	// loginErr, when set, is the code and message every login fails with.
	loginErr [2]string

	// format is the result format of every query: formatJSON or
	// formatArrow.
	format string

	mu      sync.Mutex
	url     string
	queries []fakeQuery
	seen    []string
	chunks  []fakeChunk
}

func newFake(t *testing.T) *fakeSnowflake {
	return &fakeSnowflake{t: t, user: "SYNCLET_TEST", password: "test-password", key: &testKey.PublicKey, format: formatJSON}
}

// in sets the result format of every query.
func (f *fakeSnowflake) in(format string) *fakeSnowflake {
	f.format = format
	return f
}

func (f *fakeSnowflake) on(q ...fakeQuery) *fakeSnowflake {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q...)
	return f
}

// executed returns the SQL text of every query run so far.
func (f *fakeSnowflake) executed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.seen)
}

// source starts the server and returns a Source that reaches it.
func (f *fakeSnowflake) source() Source {
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	f.t.Cleanup(srv.Close)
	f.mu.Lock()
	f.url = srv.URL
	f.mu.Unlock()
	u, err := url.Parse(srv.URL)
	if err != nil {
		f.t.Fatal(err)
	}
	return Source{endpoint: u}
}

func (f *fakeSnowflake) serve(w http.ResponseWriter, r *http.Request) {
	var body io.Reader = r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		body = zr
	}
	raw, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodPost && r.URL.Path == "/session/v1/login-request":
		f.login(w, raw)
	case r.Method == http.MethodPost && r.URL.Path == "/queries/v1/query-request":
		f.query(w, raw)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/chunk/"):
		f.chunk(w, r)
	case r.Method == http.MethodPost && r.URL.Path == "/session" && r.URL.Query().Get("delete") == "true":
		_, _ = fmt.Fprint(w, `{"success":true,"data":null}`)
	default:
		f.t.Errorf("fake Snowflake: unexpected %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected request", http.StatusNotFound)
	}
}

func fail(w http.ResponseWriter, code, msg string) {
	b, _ := json.Marshal(map[string]any{"success": false, "code": code, "message": msg, "data": map[string]any{"sqlState": "08004"}})
	_, _ = w.Write(b)
}

func (f *fakeSnowflake) login(w http.ResponseWriter, raw []byte) {
	if f.loginStatus != 0 {
		w.WriteHeader(f.loginStatus)
		return
	}
	if f.loginErr[0] != "" {
		fail(w, f.loginErr[0], f.loginErr[1])
		return
	}
	var req struct {
		Data struct {
			LoginName     string `json:"LOGIN_NAME"`
			Password      string `json:"PASSWORD"`
			Authenticator string `json:"AUTHENTICATOR"`
			Token         string `json:"TOKEN"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		f.t.Errorf("fake Snowflake: login body: %v", err)
		return
	}
	d := req.Data
	switch {
	case d.Authenticator == "SNOWFLAKE_JWT":
		if !f.validJWT(d.Token) {
			fail(w, "390144", "JWT token is invalid. [fake-request-id]")
			return
		}
	case d.LoginName != f.user || d.Password != f.password:
		fail(w, "390100", "Incorrect username or password was specified.")
		return
	}
	_, _ = fmt.Fprint(w, `{"success":true,"data":{"token":"session-token","masterToken":"master-token","sessionId":1,"parameters":[],"sessionInfo":{}}}`)
}

// validJWT reports whether token is an RS256 JWT for f.user signed by
// f.key.
func (f *fakeSnowflake) validJWT(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if json.Unmarshal(payload, &claims) != nil || !strings.HasSuffix(claims.Sub, "."+f.user) {
		return false
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return false
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	return rsa.VerifyPKCS1v15(f.key, crypto.SHA256, sum[:], sig) == nil
}

func (f *fakeSnowflake) query(w http.ResponseWriter, raw []byte) {
	var req struct {
		SQLText  string `json:"sqlText"`
		Bindings map[string]struct {
			Value *string `json:"value"`
		} `json:"bindings"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		f.t.Errorf("fake Snowflake: query body: %v", err)
		return
	}
	var binds []string
	for i := 1; i <= len(req.Bindings); i++ {
		b := req.Bindings[fmt.Sprint(i)]
		if b.Value == nil {
			binds = append(binds, "<nil>")
			continue
		}
		binds = append(binds, *b.Value)
	}
	f.mu.Lock()
	f.seen = append(f.seen, req.SQLText)
	var match *fakeQuery
	for i := range f.queries {
		q := &f.queries[i]
		if q.sql == req.SQLText && slices.Equal(q.bindings, binds) {
			match = q
			break
		}
	}
	f.mu.Unlock()
	if match == nil {
		f.t.Errorf("fake Snowflake: unexpected query\n%s\nbindings %q", req.SQLText, binds)
		fail(w, "000904", "fake: unexpected query")
		return
	}
	if match.errCode != "" {
		fail(w, match.errCode, match.errMsg)
		return
	}
	rowtype := make([]map[string]any, len(match.columns))
	for i, c := range match.columns {
		rowtype[i] = map[string]any{"name": c.name, "type": c.typ, "nullable": true, "precision": 38, "scale": 0}
	}
	total := len(match.rows)
	var chunks []map[string]any
	f.mu.Lock()
	for i, rows := range match.chunks {
		total += len(rows)
		body, err := f.encodeChunk(match.columns, rows)
		if err != nil {
			f.mu.Unlock()
			f.t.Errorf("fake Snowflake: %v", err)
			fail(w, "000904", "fake: cannot encode chunk")
			return
		}
		c := fakeChunk{body: body, status: http.StatusOK}
		if match.failChunk == i+1 {
			c.status = http.StatusForbidden
		}
		chunks = append(chunks, map[string]any{"url": fmt.Sprintf("%s/chunk/%d", f.url, len(f.chunks)), "rowCount": len(rows)})
		f.chunks = append(f.chunks, c)
	}
	f.mu.Unlock()
	data := map[string]any{
		"queryResultFormat": f.format, "queryId": "01fake00-0000-0000-0000-000000000000",
		"rowtype": rowtype, "total": total, "returned": total,
	}
	if len(chunks) > 0 {
		data["chunks"] = chunks
		data["qrmk"] = fakeQRMK
	}
	if f.format == formatArrow {
		if len(match.rows) > 0 {
			b, err := arrowRows(match.columns, match.rows)
			if err != nil {
				f.t.Errorf("fake Snowflake: %v", err)
				fail(w, "000904", "fake: cannot encode rows")
				return
			}
			data["rowsetBase64"] = base64.StdEncoding.EncodeToString(b)
		}
	} else {
		rows := match.rows
		if rows == nil {
			rows = [][]*string{}
		}
		data["rowset"] = rows
	}
	b, _ := json.Marshal(map[string]any{"success": true, "data": data})
	_, _ = w.Write(b)
}

// chunk serves a result chunk to a request that carries the result's
// master key.
func (f *fakeSnowflake) chunk(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/chunk/"))
	f.mu.Lock()
	ok := err == nil && n >= 0 && n < len(f.chunks)
	var c fakeChunk
	if ok {
		c = f.chunks[n]
	}
	f.mu.Unlock()
	if !ok {
		f.t.Errorf("fake Snowflake: unknown chunk %s", r.URL.Path)
		http.Error(w, "unknown chunk", http.StatusNotFound)
		return
	}
	if got := r.Header.Get("X-Amz-Server-Side-Encryption-Customer-Key"); got != fakeQRMK {
		f.t.Errorf("fake Snowflake: chunk %d requested with key %q", n, got)
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	if c.status != http.StatusOK {
		w.WriteHeader(c.status)
		_, _ = fmt.Fprint(w, "<Error><Code>AccessDenied</Code></Error>")
		return
	}
	_, _ = w.Write(c.body)
}

// encodeChunk renders rows as a chunk body: JSON rows without the
// enclosing brackets, or an Arrow IPC stream.
func (f *fakeSnowflake) encodeChunk(columns []fakeColumn, rows [][]*string) ([]byte, error) {
	if f.format == formatArrow {
		return arrowRows(columns, rows)
	}
	b, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	return b[1 : len(b)-1], nil
}

// arrowRows renders rows as an Arrow IPC stream of one record batch: text
// columns as strings, real as float64 and fixed as int64.
func arrowRows(columns []fakeColumn, rows [][]*string) ([]byte, error) {
	fields := make([]arrow.Field, len(columns))
	for i, c := range columns {
		var dt arrow.DataType
		switch c.typ {
		case "text":
			dt = arrow.BinaryTypes.String
		case "real":
			dt = arrow.PrimitiveTypes.Float64
		case "fixed":
			dt = arrow.PrimitiveTypes.Int64
		default:
			return nil, fmt.Errorf("no Arrow type for column type %q", c.typ)
		}
		fields[i] = arrow.Field{Name: c.name, Type: dt, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for _, r := range rows {
		for i, cell := range r {
			if cell == nil {
				b.Field(i).AppendNull()
				continue
			}
			switch fb := b.Field(i).(type) {
			case *array.StringBuilder:
				fb.Append(*cell)
			case *array.Float64Builder:
				v, err := strconv.ParseFloat(*cell, 64)
				if err != nil {
					return nil, fmt.Errorf("real cell %q: %w", *cell, err)
				}
				fb.Append(v)
			case *array.Int64Builder:
				v, err := strconv.ParseInt(*cell, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("fixed cell %q: %w", *cell, err)
				}
				fb.Append(v)
			}
		}
	}
	rec := b.NewRecord()
	defer rec.Release()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(schema))
	if err := w.Write(rec); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// str is a fake cell value; null is SQL NULL.
func str(s string) *string { return &s }

var null *string

func row(cells ...*string) []*string { return cells }

// textColumns are columns of driver type text.
func textColumns(names ...string) []fakeColumn {
	cols := make([]fakeColumn, len(names))
	for i, n := range names {
		cols[i] = fakeColumn{name: n, typ: "text"}
	}
	return cols
}

func rsaKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return k
}
