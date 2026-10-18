// Command source-solidgate reads orders, subscriptions, disputes, fraud alerts,
// products and financial entries from Solidgate.
package main

import (
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Solidgate source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock,
// short delays and no rate limit.
type Source struct {
	reportsURL       string
	subscriptionsURL string
	now              func() time.Time
	client           *http.Client
	retryDelay       time.Duration
	pollDelay        time.Duration
	pollTimeout      time.Duration
	noRateLimit      bool
}

func main() { cli.RunSource(Source{}) }
