// Command source-google-ads reads Google Ads accounts, their entities and
// daily performance through GAQL queries of the Google Ads REST API.
package main

import (
	"context"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Google Ads source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock and
// without real waiting.
type Source struct {
	baseURL    string
	tokenURL   string
	now        func() time.Time
	client     *http.Client
	retryDelay time.Duration
	jitter     func(time.Duration) time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func main() { cli.RunSource(Source{}) }

func (s Source) today() time.Time {
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	return now().UTC().Truncate(24 * time.Hour)
}

// equalJitter returns a random duration between d/2 and d, so concurrent
// syncs throttled together do not retry in step.
func equalJitter(d time.Duration) time.Duration {
	if d < 2 {
		return d
	}
	return d/2 + rand.N(d/2) //nolint:gosec // backoff jitter needs no cryptographic randomness
}

// sleep waits for d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
