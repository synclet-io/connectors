// Command source-amplitude reads raw events through the Amplitude Export API
// and dashboard metrics, cohorts, annotations and the event list through the
// Dashboard REST API.
package main

import (
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Amplitude source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock and
// short retry delays.
type Source struct {
	baseURL    string
	now        func() time.Time
	client     *http.Client
	retryDelay time.Duration
}

func main() { cli.RunSource(Source{}) }
