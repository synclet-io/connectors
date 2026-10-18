// Command source-revenuecat reads projects, apps, products, entitlements,
// offerings, customers with their subscriptions, purchases and active
// entitlements, and overview metrics from the RevenueCat REST API v2.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the RevenueCat source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fake clock
// and without real waits.
type Source struct {
	baseURL    string
	client     *http.Client
	now        func() time.Time
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func main() { cli.RunSource(Source{}) }
