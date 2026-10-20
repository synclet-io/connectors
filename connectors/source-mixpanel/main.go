// Command source-mixpanel reads raw events, user profiles, cohorts and their
// members, annotations, funnels and revenue from the Mixpanel Export and
// Query APIs.
package main

import (
	"context"
	"net/http"
	"time"
	_ "time/tzdata"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Mixpanel source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fake clock
// and without real waits.
type Source struct {
	queryURL   string
	exportURL  string
	client     *http.Client
	now        func() time.Time
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func main() { cli.RunSource(Source{}) }

func (s Source) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
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
