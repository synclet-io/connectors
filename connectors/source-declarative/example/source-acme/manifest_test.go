package sourceacme

import (
	"encoding/json"
	"testing"
	"time"

	"source-declarative/declarativetest"
)

func TestManifest(t *testing.T) {
	declarativetest.Run(t, declarativetest.Suite{
		Config:        json.RawMessage(`{"api_key":"test-key","start_date":"2026-01-01T00:00:00Z"}`),
		BrokenConfig:  json.RawMessage(`{"api_key":"wrong-key","start_date":"2026-01-01T00:00:00Z"}`),
		Auth:          declarativetest.Header("Authorization", "Bearer test-key"),
		Now:           time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC),
		MinPages:      2,
		RateLimitOnce: true,
	})
}
