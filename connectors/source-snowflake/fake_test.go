package main

import (
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
	"strings"
	"sync"
	"testing"
)

// fakeColumn is one column of a fake result: its name and the driver's
// type of it (text, fixed, real).
type fakeColumn struct {
	name string
	typ  string
}

// fakeQuery answers a query whose SQL text and bound values equal sql and
// bindings, with rows or with a Snowflake error.
type fakeQuery struct {
	sql      string
	bindings []string
	columns  []fakeColumn
	rows     [][]*string
	errCode  string
	errMsg   string
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

	mu      sync.Mutex
	queries []fakeQuery
	seen    []string
}

func newFake(t *testing.T) *fakeSnowflake {
	return &fakeSnowflake{t: t, user: "SYNCLET_TEST", password: "test-password", key: &testKey.PublicKey}
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
	rows := match.rows
	if rows == nil {
		rows = [][]*string{}
	}
	b, _ := json.Marshal(map[string]any{"success": true, "data": map[string]any{
		"queryResultFormat": "json", "queryId": "01fake00-0000-0000-0000-000000000000",
		"rowtype": rowtype, "rowset": rows, "total": len(rows), "returned": len(rows),
	}})
	_, _ = w.Write(b)
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
