// Command source-facebook-marketing reads Meta ad accounts, their entities and
// daily ad insights from the Facebook Marketing API.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Facebook Marketing source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock and
// without real waiting.
type Source struct {
	baseURL      string
	now          func() time.Time
	client       *http.Client
	retryDelay   time.Duration
	pollInterval time.Duration
	wait         func(ctx context.Context, d time.Duration) error
}

func main() { cli.RunSource(Source{}) }

func (s Source) today() time.Time {
	now := time.Now
	if s.now != nil {
		now = s.now
	}
	return now().UTC().Truncate(24 * time.Hour)
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
