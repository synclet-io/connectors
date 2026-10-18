// Command source-applovin reads the AppLovin advertiser (Axon) report and the
// MAX revenue report.
package main

import (
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the AppLovin source connector. Its fields are zero in production;
// tests set them to reach a fixture server with a fixed clock and short
// retry delays.
type Source struct {
	baseURL    string
	now        func() time.Time
	client     *http.Client
	retryDelay time.Duration
}

func main() { cli.RunSource(Source{}) }
