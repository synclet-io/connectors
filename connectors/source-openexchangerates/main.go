// Command source-openexchangerates reads currencies and exchange rates from
// Open Exchange Rates.
package main

import (
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Open Exchange Rates source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock.
type Source struct {
	baseURL    string
	now        func() time.Time
	client     *http.Client
	retryDelay time.Duration
}

func main() { cli.RunSource(Source{}) }
