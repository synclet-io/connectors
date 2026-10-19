// Command source-linkedin-ads reads LinkedIn ad accounts, campaign groups,
// campaigns, creatives and daily ad analytics from the versioned LinkedIn
// Marketing API.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the LinkedIn Ads source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock and
// without real waiting.
type Source struct {
	apiURL     string
	tokenURL   string
	now        func() time.Time
	client     *http.Client
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func main() { cli.RunSource(Source{}) }

func (s Source) clock() func() time.Time {
	if s.now != nil {
		return s.now
	}
	return time.Now
}

func (s Source) today() time.Time {
	return s.clock()().UTC().Truncate(24 * time.Hour)
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
