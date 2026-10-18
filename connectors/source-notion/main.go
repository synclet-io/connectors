// Command source-notion reads the users, databases, pages and blocks a
// Notion internal integration can see.
package main

import (
	"context"
	"net/http"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Notion source connector. Its fields are zero in production;
// tests set them to reach a fixture server with a fixed clock and without
// real waits.
type Source struct {
	baseURL    string
	now        func() time.Time
	client     *http.Client
	retryDelay time.Duration
	wait       func(ctx context.Context, d time.Duration) error
}

func main() { cli.RunSource(Source{}) }
