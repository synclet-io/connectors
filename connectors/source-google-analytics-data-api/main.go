// Command source-google-analytics-data-api reads Google Analytics 4 reports,
// built-in and configured, through the runReport method of the Google
// Analytics Data API.
package main

import (
	"context"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Google Analytics 4 source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock,
// small pages and without real waiting.
type Source struct {
	baseURL    string
	tokenURL   string
	now        func() time.Time
	client     *http.Client
	retryDelay time.Duration
	jitter     func(time.Duration) time.Duration
	wait       func(ctx context.Context, d time.Duration) error
	pageLimit  int
}

func main() { cli.RunSource(Source{}) }

func (s Source) clock() time.Time {
	if s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

func (s Source) today() time.Time { return s.clock().Truncate(24 * time.Hour) }

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
