package apiwatch

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	userAgent = "synclet-api-watch (+https://github.com/synclet-io/connectors)"
	maxBody   = 32 << 20
)

// NewClient returns the client the checkers use against the network.
func NewClient() *http.Client {
	return &http.Client{Timeout: 60 * time.Second}
}

// open sends a GET and returns the response for the caller to read and close.
// Requests to api.github.com carry $GITHUB_TOKEN when it is set, which lifts
// the anonymous rate limit shared by Actions runners.
func open(ctx context.Context, client *http.Client, target string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", userAgent)

	if req.URL.Host == "api.github.com" {
		req.Header.Set("Accept", "application/vnd.github+json")

		if tok := os.Getenv("GITHUB_TOKEN"); tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", target, err)
	}

	return resp, nil
}

// get returns the body and status of a GET.
func get(ctx context.Context, client *http.Client, target string) (_ []byte, _ int, rerr error) {
	resp, err := open(ctx, client, target)
	if err != nil {
		return nil, 0, err
	}
	defer func() { rerr = errors.Join(rerr, resp.Body.Close()) }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return nil, 0, fmt.Errorf("GET %s: %w", target, err)
	}

	return body, resp.StatusCode, nil
}

// getOK is get that fails on any status but 200.
func getOK(ctx context.Context, client *http.Client, target string) ([]byte, error) {
	body, code, err := get(ctx, client, target)
	if err != nil {
		return nil, err
	}

	if code != http.StatusOK {
		return nil, fmt.Errorf("GET %s: HTTP %d", target, code)
	}

	return body, nil
}

var (
	reScript = regexp.MustCompile(`(?is)<script\b.*?</script>|<style\b.*?</style>`)
	reTag    = regexp.MustCompile(`(?s)<[^>]*>`)
	reSpace  = regexp.MustCompile(`\s+`)
)

// pageText is the visible text of an HTML page with whitespace collapsed, so
// scrapers match on the words a reader sees rather than on markup.
func pageText(page []byte) string {
	s := reScript.ReplaceAllString(string(page), " ")
	s = html.UnescapeString(reTag.ReplaceAllString(s, " "))

	return strings.TrimSpace(reSpace.ReplaceAllString(s, " "))
}

// FixtureTransport answers every request with a recorded file and never
// touches the network: <Dir>/<any subdirectory>/<FixtureName(url)>, or 404
// when there is none.
type FixtureTransport struct {
	Dir string
}

func (f FixtureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	matches, err := filepath.Glob(filepath.Join(f.Dir, "*", filepath.FromSlash(FixtureName(req.URL))))
	if err != nil {
		return nil, err
	}

	resp := &http.Response{
		StatusCode: http.StatusNotFound,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(`{"error":"no fixture"}`)),
		Request:    req,
	}

	if len(matches) > 0 {
		body, err := os.ReadFile(matches[0])
		if err != nil {
			return nil, err
		}

		resp.StatusCode = http.StatusOK
		resp.Body = io.NopCloser(strings.NewReader(string(body)))
	}

	resp.Status = http.StatusText(resp.StatusCode)

	return resp, nil
}

var reUnsafe = regexp.MustCompile(`[^A-Za-z0-9._/-]`)

// FixtureName is the relative path a recorded response of u is stored at:
// host and path, "index" for a path ending in a slash, "@query" for a query.
func FixtureName(u *url.URL) string {
	name := u.Host + u.EscapedPath()
	if strings.HasSuffix(name, "/") {
		name += "index"
	}

	if u.RawQuery != "" {
		name += "@" + u.RawQuery
	}

	return reUnsafe.ReplaceAllString(name, "_")
}
