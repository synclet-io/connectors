package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// filesClient stages files in a Unity Catalog volume through the Files API.
type filesClient struct {
	base   string
	http   *http.Client
	bearer *bearer
}

// apiError is a Files API response with an error status.
type apiError struct {
	status  int
	code    string
	message string
	method  string
	path    string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("files API %s %s: HTTP %d %s %s", e.method, e.path, e.status, e.code, e.message)
}

func escapePath(p string) string {
	parts := strings.Split(p, "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}

func (f *filesClient) do(ctx context.Context, method, endpoint, path string, body []byte) error {
	u := f.base + endpoint + escapePath(path)
	if method == http.MethodPut {
		u += "?overwrite=true"
	}
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	if err := f.bearer.Authenticate(req); err != nil {
		return err
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return fmt.Errorf("files API %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	e := &apiError{status: resp.StatusCode, method: method, path: path}
	var payload struct {
		ErrorCode string `json:"error_code"`
		Message   string `json:"message"`
	}
	if json.Unmarshal(raw, &payload) == nil && payload.ErrorCode != "" {
		e.code, e.message = payload.ErrorCode, payload.Message
	} else {
		e.message = strings.TrimSpace(string(raw))
	}
	return e
}

// put uploads one file, replacing a file of the same path.
func (f *filesClient) put(ctx context.Context, path string, body []byte) error {
	if body == nil {
		body = []byte{}
	}
	return f.do(ctx, http.MethodPut, "/api/2.0/fs/files", path, body)
}

func (f *filesClient) delete(ctx context.Context, path string) error {
	return f.do(ctx, http.MethodDelete, "/api/2.0/fs/files", path, nil)
}

// deleteDir removes an empty directory.
func (f *filesClient) deleteDir(ctx context.Context, path string) error {
	return f.do(ctx, http.MethodDelete, "/api/2.0/fs/directories", path, nil)
}
