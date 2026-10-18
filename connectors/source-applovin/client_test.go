package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var advertiserDef, _ = streamDefByName(streamAdvertiserReport)

func TestRetry(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"code":200,"count":1,"results":[{"day":"2026-10-17"}]}`))
	}))
	t.Cleanup(srv.Close)
	rows, err := testSource(srv).apiClient(Config{APIKey: testReportKey}).report(t.Context(), advertiserDef, "2026-10-17", []string{"day"})
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
	assert.Len(t, rows, 1)
}

func TestRetryGivesUp(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(srv.Close)
	_, err := testSource(srv).apiClient(Config{APIKey: testReportKey}).report(t.Context(), advertiserDef, "2026-10-17", []string{"day"})
	require.ErrorContains(t, err, "GET /report (advertiser report): HTTP 502")
	assert.Equal(t, int32(attempts), calls.Load())
}

func TestNotRetried(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusBadRequest} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
			}))
			t.Cleanup(srv.Close)
			_, err := testSource(srv).apiClient(Config{APIKey: testReportKey}).report(t.Context(), advertiserDef, "2026-10-17", []string{"day"})
			require.Error(t, err)
			assert.Equal(t, int32(1), calls.Load())
		})
	}
}

// TestKeyRedacted checks that neither a transport error, which names the
// request URL, nor an error body that echoes the key carries the key.
func TestKeyRedacted(t *testing.T) {
	const key = "secret/key+value"
	t.Run("transport error", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		src := testSource(srv)
		srv.Close()
		_, err := src.apiClient(Config{APIKey: key}).report(t.Context(), advertiserDef, "2026-10-17", []string{"day"})
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "secret")
		assert.NotContains(t, err.Error(), "api_key")
	})
	t.Run("error body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(r.URL.RawQuery))
		}))
		t.Cleanup(srv.Close)
		_, err := testSource(srv).apiClient(Config{APIKey: key}).report(t.Context(), advertiserDef, "2026-10-17", []string{"day"})
		require.ErrorContains(t, err, "api_key=REDACTED")
		assert.NotContains(t, err.Error(), "secret")
	})
}

func TestPaging(t *testing.T) {
	var offsets []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset := r.URL.Query().Get("offset")
		offsets = append(offsets, offset)
		assert.Equal(t, strconv.Itoa(pageSize), r.URL.Query().Get("limit"))
		n := pageSize
		if offset != "0" {
			n = 3
		}
		rows := strings.TrimSuffix(strings.Repeat(`{"day":"2026-10-17"},`, n), ",")
		_, _ = w.Write([]byte(`{"code":200,"count":` + strconv.Itoa(n) + `,"results":[` + rows + `]}`))
	}))
	t.Cleanup(srv.Close)
	rows, err := testSource(srv).apiClient(Config{APIKey: testReportKey}).report(t.Context(), advertiserDef, "2026-10-17", []string{"day"})
	require.NoError(t, err)
	assert.Len(t, rows, pageSize+3)
	assert.Equal(t, []string{"0", strconv.Itoa(pageSize)}, offsets)
}

func TestResponseCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":500,"results":[]}`))
	}))
	t.Cleanup(srv.Close)
	_, err := testSource(srv).apiClient(Config{APIKey: testReportKey}).report(t.Context(), advertiserDef, "2026-10-17", []string{"day"})
	require.ErrorContains(t, err, "response code 500")
}

// TestKeyRedactedInLongBody checks that a key echoed where an error body is
// cut short leaves no prefix of the key in the error.
func TestKeyRedactedInLongBody(t *testing.T) {
	const key = "abcdefghijklmnopqrstuvwxyz0123456789"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(strings.Repeat("x", 290) + " key=" + key))
	}))
	t.Cleanup(srv.Close)
	_, err := testSource(srv).apiClient(Config{APIKey: key}).report(t.Context(), advertiserDef, "2026-10-17", []string{"day"})
	require.Error(t, err)
	assert.NotContains(t, err.Error(), key[:4])
}
