// Command source-airtable reads the tables of Airtable bases, one stream per
// table, with a schema built from the table's fields.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Airtable source connector. Its fields are zero in
// production; tests set them to reach a fixture server with a fixed clock
// and without real waits.
type Source struct {
	baseURL    string
	now        func() time.Time
	client     *http.Client
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
	// metadataTimeout replaces defaultMetadataTimeout when set.
	metadataTimeout time.Duration
}

func main() { cli.RunSource(Source{}) }
