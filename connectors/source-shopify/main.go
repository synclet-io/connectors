// Command source-shopify reads a Shopify store through the Admin GraphQL
// API: large streams as bulk operations, small ones page by page.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Shopify source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock
// and without real waits.
type Source struct {
	// baseURL returns the origin of a shop's Admin API from its name.
	baseURL      func(shop string) string
	now          func() time.Time
	client       *http.Client
	retryDelay   time.Duration
	wait         func(ctx context.Context, d time.Duration) error
	pollInterval time.Duration
}

func main() { cli.RunSource(Source{}) }
