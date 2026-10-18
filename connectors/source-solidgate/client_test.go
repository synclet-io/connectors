package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// signatureVector is the signature of body {"limit":1} with api_pk_test and
// api_sk_test, computed with
//
//	printf 'api_pk_test{"limit":1}api_pk_test' | openssl dgst -sha512 -hmac api_sk_test | awk '{print $NF}' | tr -d '\n' | base64
const signatureVector = "NmNkYTc5NDM1NGRmNzlmNmQyZjhmYjRhMGY3MGU1NzE5NjlmZmI0MTNhYzA2ZGU4YmQ5MzUwN2YzZDI1ZTA3OGY1NTI3NDU4Y2U1ZWEwYWY3ZDI5NGFkYjQ1Y2Y3MjU0OTYzNjM3Yzc5ZjI5NDQ0N2FkZjJjMDc1OTA5MTNlOTI="

var testAccount = Account{Name: "acme_test", PublicKey: testPublicKey, SecretKey: testSecretKey}

func TestSignature(t *testing.T) {
	assert.Equal(t, signatureVector, signature(testPublicKey, testSecretKey, []byte(`{"limit":1}`)))
	for _, body := range []string{"", `{"date_from":"2026-10-16 00:00:00"}`} {
		assert.Equal(t, expectedSignature([]byte(body)), signature(testPublicKey, testSecretKey, []byte(body)), body)
	}

	var merchant, sig string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		merchant, sig = r.Header.Get("merchant"), r.Header.Get("signature")
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	var out map[string]any
	require.NoError(t, testSource(srv).apiClient(testAccount).postJSON(t.Context(), srv.URL+"/", "card-orders", map[string]int{"limit": 1}, &out))
	assert.Equal(t, testPublicKey, merchant)
	assert.Equal(t, signatureVector, sig)
}

func TestRetry(t *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusTooManyRequests} {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				w.WriteHeader(status)
				return
			}
			_, _ = w.Write([]byte(`{"orders":[]}`))
		}))
		var out map[string]any
		require.NoError(t, testSource(srv).apiClient(testAccount).postJSON(t.Context(), srv.URL+"/", "card-orders", struct{}{}, &out))
		assert.Equal(t, int32(2), calls.Load(), "HTTP %d is retried", status)
		srv.Close()
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
	err := testSource(srv).apiClient(testAccount).postJSON(t.Context(), srv.URL+"/", "card-orders", struct{}{}, &out)
	require.ErrorContains(t, err, "POST /card-orders (HTTP 502)")
	assert.Equal(t, int32(maxAttempts), calls.Load())
}

func TestAuthErrorNotRetried(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusUnauthorized} {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":{"code":"1.01","messages":["Authentication failed"]}}`))
		}))
		var out map[string]any
		err := testSource(srv).apiClient(testAccount).postJSON(t.Context(), srv.URL+"/", "card-orders", struct{}{}, &out)
		require.ErrorIs(t, err, errInvalidKeys, "HTTP %d", status)
		assert.Equal(t, int32(1), calls.Load())
		srv.Close()
	}
}

func TestErrorBodyIn200Fails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"error":{"code":"2.01","messages":["Invalid data"]}}`))
	}))
	t.Cleanup(srv.Close)
	var out map[string]any
	err := testSource(srv).apiClient(testAccount).postJSON(t.Context(), srv.URL+"/", "card-orders", struct{}{}, &out)
	require.ErrorContains(t, err, "POST /card-orders (HTTP 200, code 2.01): Invalid data")
}

func TestConfigValidate(t *testing.T) {
	neg := -1
	err := Config{
		Accounts:     []Account{{Name: "a", PublicKey: "pk", SecretKey: "api_sk_x"}, {Name: "a", PublicKey: "api_pk_x", SecretKey: "sk"}},
		StartDate:    "16.10.2026",
		LookbackDays: &neg,
	}.validate()
	require.Error(t, err)
	for _, want := range []string{
		"accounts[0]: public_key must start with api_pk_", `accounts[1]: name "a" is used by another account`,
		"accounts[1]: secret_key must start with api_sk_", `start_date "16.10.2026" is not a YYYY-MM-DD date`,
		"lookback_days must not be negative",
	} {
		assert.ErrorContains(t, err, want)
	}
	require.ErrorContains(t, Config{}.validate(), "accounts needs at least one account")

	cfg := Config{Accounts: []Account{testAccount}}
	require.NoError(t, cfg.validate())
	assert.Equal(t, "2024-10-18", cfg.start(testNow).Format(dateLayout))
	assert.Equal(t, defaultLookbackDays, cfg.lookback())
}
