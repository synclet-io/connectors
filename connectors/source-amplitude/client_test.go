package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testCreds = Config{APIKey: testAPIKey, SecretKey: testSecretKey}

func TestRetry(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(status)
					return
				}
				user, pass, ok := r.BasicAuth()
				assert.True(t, ok)
				assert.Equal(t, testAPIKey, user)
				assert.Equal(t, testSecretKey, pass)
				_, _ = w.Write([]byte(`{"data":[]}`))
			}))
			t.Cleanup(srv.Close)
			var out map[string]any
			require.NoError(t, testSource(srv).apiClient(testCreds).getJSON(t.Context(), "2/annotations", nil, &out))
			assert.Equal(t, int32(2), calls.Load())
		})
	}
}

func TestRetryGivesUp(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	var out map[string]any
	err := testSource(srv).apiClient(testCreds).getJSON(t.Context(), "2/annotations", nil, &out)
	require.EqualError(t, err, "GET /2/annotations: HTTP 502")
	assert.Equal(t, int32(attempts), calls.Load())
}

func TestRetryTransportError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	src := testSource(srv)
	srv.Close()
	var out map[string]any
	err := src.apiClient(testCreds).getJSON(t.Context(), "2/annotations", nil, &out)
	require.ErrorContains(t, err, "GET /2/annotations: ")
	assert.NotContains(t, err.Error(), testSecretKey)
}

func TestNotRetried(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":"example"}`))
			}))
			t.Cleanup(srv.Close)
			var out map[string]any
			err := testSource(srv).apiClient(testCreds).getJSON(t.Context(), "2/annotations", nil, &out)
			require.ErrorContains(t, err, "HTTP "+strconv.Itoa(status))
			require.ErrorContains(t, err, `: {"error":"example"}`)
			assert.Equal(t, int32(1), calls.Load())
		})
	}
}

func TestRetryAfter(t *testing.T) {
	assert.Equal(t, time.Duration(0), retryAfter(""))
	assert.Equal(t, time.Duration(0), retryAfter("Wed, 21 Oct 2026 07:28:00 GMT"))
	assert.Equal(t, 30*time.Second, retryAfter("30"))
	assert.Equal(t, maxRetryAfter, retryAfter("86400"))
}

func TestRetryWaitsRetryAfter(t *testing.T) {
	var calls atomic.Int32
	var first time.Time
	var waited time.Duration
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			first = time.Now()
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		waited = time.Since(first)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	var out map[string]any
	require.NoError(t, testSource(srv).apiClient(testCreds).getJSON(t.Context(), "2/annotations", nil, &out))
	assert.GreaterOrEqual(t, waited, time.Second)
}

func TestErrorBodyCut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write(make([]byte, 1000))
	}))
	t.Cleanup(srv.Close)
	var out map[string]any
	err := testSource(srv).apiClient(testCreds).getJSON(t.Context(), "2/annotations", nil, &out)
	require.Error(t, err)
	assert.Less(t, len(err.Error()), maxErrorBody+50)
}

func TestRetrySucceedsWhenCanceledAfter(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	c := &apiClient{retryDelay: time.Millisecond}
	err := c.retry(ctx, func() error {
		cancel()
		return nil
	})
	require.NoError(t, err, "a request that succeeded hands its result to the caller, which owns cleaning it up")
}

func TestDownloadSizeLimit(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write(make([]byte, 2048))
	}))
	t.Cleanup(srv.Close)
	c := testSource(srv).apiClient(testCreds)
	c.maxDownload = 1024
	f, err := c.download(t.Context(), exportPath, nil)
	require.ErrorContains(t, err, "larger than 1024 bytes")
	assert.Nil(t, f)
	assert.Equal(t, int32(1), calls.Load())
	left, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, left, "the partial download is removed")
}
