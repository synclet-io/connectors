// Command source-salesforce reads Salesforce objects through SOQL queries of
// the Salesforce REST API, with schemas built from each object's describe.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Salesforce source connector. Its fields are zero in
// production; tests set them to reach a fixture server without real waiting
// and with a short query length limit.
type Source struct {
	// loginURL and sandboxURL are the OAuth hosts of production and sandbox
	// orgs.
	loginURL   string
	sandboxURL string
	client     *http.Client
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
	// maxSelectLen is the longest URL-encoded SOQL query sent as one
	// request; a longer SELECT is split into field chunks.
	maxSelectLen int
}

func main() { cli.RunSource(Source{}) }

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
