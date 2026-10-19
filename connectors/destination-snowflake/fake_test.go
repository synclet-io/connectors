package main

import (
	"bytes"
	"compress/gzip"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeQuery answers a query whose SQL text equals sql with text rows.
type fakeQuery struct {
	sql     string
	columns []string
	rows    [][]*string
}

// fakeFailure fails the nth statement (1-based; 0 means every one) whose
// SQL text contains match with a Snowflake error.
type fakeFailure struct {
	match string
	nth   int
	code  string
	msg   string
	seen  int
}

// fakeSnowflake speaks the part of Snowflake's HTTP protocol gosnowflake
// uses for the connector: login, queries, PUT to a LOCAL_FS stage, and
// session close. COPY INTO reads the staged file back, so tests see the rows
// as Snowflake would load them. A SELECT no route answers fails the test;
// other statements succeed.
type fakeSnowflake struct {
	t *testing.T

	user        string
	key         *rsa.PublicKey
	loginStatus int
	loginErr    [2]string

	stageDir string

	mu       sync.Mutex
	queries  []fakeQuery
	failures []*fakeFailure
	seen     []string
	// loaded holds, per table COPY INTO loaded into, every row it loaded.
	loaded map[string][][]string
	// copyLoaded, when set, is the rows_loaded every COPY reports.
	copyLoaded *int
}

func newFake(t *testing.T) *fakeSnowflake {
	return &fakeSnowflake{t: t, user: "SYNCLET_TEST", key: &testKey.PublicKey, stageDir: t.TempDir(), loaded: map[string][][]string{}}
}

func (f *fakeSnowflake) on(q ...fakeQuery) *fakeSnowflake {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, q...)
	return f
}

func (f *fakeSnowflake) failOn(match string, nth int, code, msg string) *fakeSnowflake {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = append(f.failures, &fakeFailure{match: match, nth: nth, code: code, msg: msg})
	return f
}

// executed returns the SQL text of every statement run so far.
func (f *fakeSnowflake) executed() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.seen)
}

// rowsOf returns the rows COPY INTO loaded into table.
func (f *fakeSnowflake) rowsOf(table string) [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.loaded[table])
}

// destination starts the server and returns a Destination that reaches it.
func (f *fakeSnowflake) destination() Destination {
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	f.t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		f.t.Fatal(err)
	}
	d := NewDestination()
	d.endpoint = u
	d.loginTimeout = time.Second
	return d
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
	case r.Method == http.MethodPost && r.URL.Path == "/session" && r.URL.Query().Get("delete") == "true":
		_, _ = fmt.Fprint(w, `{"success":true,"data":null}`)
	default:
		f.t.Errorf("fake Snowflake: unexpected %s %s", r.Method, r.URL.Path)
		http.Error(w, "unexpected request", http.StatusNotFound)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	b, _ := json.Marshal(v)
	_, _ = w.Write(b)
}

func fail(w http.ResponseWriter, code, msg string) {
	writeJSON(w, map[string]any{"success": false, "code": code, "message": msg, "data": map[string]any{"sqlState": "42000", "queryId": "01fake00-0000-0000-0000-00000000dead"}})
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
			Authenticator string `json:"AUTHENTICATOR"`
			Token         string `json:"TOKEN"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		f.t.Errorf("fake Snowflake: login body: %v", err)
		return
	}
	d := req.Data
	if d.Authenticator != "SNOWFLAKE_JWT" || !f.validJWT(d.Token) {
		fail(w, "390144", "JWT token is invalid. [fake-request-id]")
		return
	}
	_, _ = fmt.Fprint(w, `{"success":true,"data":{"token":"session-token","masterToken":"master-token","sessionId":1,"parameters":[],"sessionInfo":{}}}`)
}

// validJWT reports whether token is an RS256 JWT of f.user signed by f.key.
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

var (
	putStmt  = regexp.MustCompile(`^PUT 'file://([^']+)' `)
	copyStmt = regexp.MustCompile(`^COPY INTO (\S+) .*FILES = \('([^']+)'\)`)
)

func (f *fakeSnowflake) query(w http.ResponseWriter, raw []byte) {
	var req struct {
		SQLText string `json:"sqlText"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		f.t.Errorf("fake Snowflake: query body: %v", err)
		return
	}
	q := req.SQLText
	f.mu.Lock()
	f.seen = append(f.seen, q)
	for _, fl := range f.failures {
		if !strings.Contains(q, fl.match) {
			continue
		}
		fl.seen++
		if fl.nth == 0 || fl.nth == fl.seen {
			f.mu.Unlock()
			fail(w, fl.code, fl.msg)
			return
		}
	}
	var match *fakeQuery
	for i := range f.queries {
		if f.queries[i].sql == q {
			match = &f.queries[i]
			break
		}
	}
	f.mu.Unlock()

	if m := putStmt.FindStringSubmatch(q); m != nil {
		writeJSON(w, map[string]any{"success": true, "data": map[string]any{
			"command": "UPLOAD", "src_locations": []string{m[1]}, "autoCompress": false, "sourceCompression": "gzip",
			"parallel": 1, "overwrite": false, "queryId": "01fake00-0000-0000-0000-0000000000a1",
			"stageInfo": map[string]any{"locationType": "LOCAL_FS", "location": f.stageDir},
		}})
		return
	}
	if m := copyStmt.FindStringSubmatch(q); m != nil {
		f.copy(w, m[1], m[2])
		return
	}
	if match != nil {
		f.respond(w, match.columns, match.rows)
		return
	}
	if strings.HasPrefix(q, "SELECT") {
		f.t.Errorf("fake Snowflake: unexpected query\n%s", q)
		fail(w, "000904", "fake: unexpected query")
		return
	}
	s := "Statement executed successfully."
	f.respond(w, []string{"status"}, [][]*string{{&s}})
}

// copy loads the staged file into table and purges it.
func (f *fakeSnowflake) copy(w http.ResponseWriter, table, file string) {
	path := filepath.Join(f.stageDir, file)
	b, err := os.ReadFile(path)
	if err != nil {
		fail(w, "091016", "Remote file '"+file+"' was not found.")
		return
	}
	_ = os.Remove(path)
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		f.t.Errorf("fake Snowflake: staged file %s is not gzip: %v", file, err)
		return
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		f.t.Errorf("fake Snowflake: staged file %s: %v", file, err)
		return
	}
	rows, err := parseStagedCSV(plain)
	if err != nil {
		f.t.Errorf("fake Snowflake: staged file %s: %v", file, err)
		return
	}
	f.mu.Lock()
	f.loaded[table] = append(f.loaded[table], rows...)
	n := len(rows)
	if f.copyLoaded != nil {
		n = *f.copyLoaded
	}
	f.mu.Unlock()
	status, parsed, loaded := "LOADED", fmt.Sprint(n), fmt.Sprint(n)
	f.respond(w, []string{"file", "status", "rows_parsed", "rows_loaded"}, [][]*string{{&file, &status, &parsed, &loaded}})
}

// nullCell marks a staged field that Snowflake reads as NULL.
const nullCell = "<NULL>"

// parseStagedCSV reads the connector's CSV as the COPY file format reads
// it: an empty unquoted field is NULL, a quoted field is its text with ""
// as a quote. The connector quotes every value, so an unquoted value is an
// error.
func parseStagedCSV(b []byte) ([][]string, error) {
	var out [][]string
	var rec []string
	i := 0
	for i < len(b) {
		val := nullCell
		switch b[i] {
		case ',', '\n':
		case '"':
			var sb strings.Builder
			for i++; ; i++ {
				if i >= len(b) {
					return nil, errors.New("unterminated quoted field")
				}
				if b[i] != '"' {
					sb.WriteByte(b[i])
					continue
				}
				if i+1 < len(b) && b[i+1] == '"' {
					sb.WriteByte('"')
					i++
					continue
				}
				i++
				break
			}
			val = sb.String()
		default:
			return nil, fmt.Errorf("offset %d: unquoted value", i)
		}
		rec = append(rec, val)
		switch {
		case i >= len(b):
			return nil, errors.New("last record has no newline")
		case b[i] == ',':
			i++
		case b[i] == '\n':
			out = append(out, rec)
			rec = nil
			i++
		default:
			return nil, fmt.Errorf("offset %d: text after a quoted field", i)
		}
	}
	if rec != nil {
		return nil, errors.New("last record has no newline")
	}
	return out, nil
}

func (f *fakeSnowflake) respond(w http.ResponseWriter, columns []string, rows [][]*string) {
	rowtype := make([]map[string]any, len(columns))
	for i, c := range columns {
		rowtype[i] = map[string]any{"name": c, "type": "text", "nullable": true}
	}
	if rows == nil {
		rows = [][]*string{}
	}
	writeJSON(w, map[string]any{"success": true, "data": map[string]any{
		"queryResultFormat": "json", "queryId": "01fake00-0000-0000-0000-000000000000",
		"rowtype": rowtype, "rowset": rows, "total": len(rows), "returned": len(rows),
	}})
}

// str is a fake cell value; null is SQL NULL.
func str(s string) *string { return &s }

var null *string

func row(cells ...*string) []*string { return cells }
