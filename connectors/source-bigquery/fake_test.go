package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
)

// fakeRoute answers requests whose method and path match: path is the part
// after /projects/, and a trailing * matches any rest.
type fakeRoute struct {
	method string
	path   string
	status int
	body   string
}

// fakeBigQuery serves canned BigQuery REST responses and fails the test on
// any request no route matches.
func fakeBigQuery(t *testing.T, routes ...fakeRoute) Source {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, rest, ok := strings.Cut(r.URL.Path, "/projects/")
		for _, rt := range routes {
			match := rest == rt.path || (strings.HasSuffix(rt.path, "*") && strings.HasPrefix(rest, strings.TrimSuffix(rt.path, "*")))
			if ok && r.Method == rt.method && match {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(rt.status)
				_, _ = fmt.Fprint(w, rt.body)
				return
			}
		}
		t.Errorf("fake BigQuery: unexpected %s %s", r.Method, r.URL.Path)
		http.Error(w, `{"error":{"code":500,"message":"unexpected request"}}`, http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return Source{clientOpts: []option.ClientOption{option.WithEndpoint(srv.URL), option.WithoutAuthentication()}}
}

func googleError(code int, reason, message string) string {
	return fmt.Sprintf(`{"error":{"code":%d,"message":%q,"errors":[{"reason":%q,"message":%q}]}}`, code, message, reason, message)
}

func testdata(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	require.NoError(t, err)
	return string(b)
}
