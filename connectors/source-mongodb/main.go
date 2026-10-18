// Command source-mongodb reads the collections of a MongoDB database.
package main

import (
	"context"
	"net"
	"time"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the MongoDB source connector. Its fields are zero in main;
// tests set them.
type Source struct {
	// dial opens every connection to a server when no SSH tunnel is
	// configured. Nil dials the address as given.
	dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// selectTimeout is how long the driver waits for a server it can use.
	// Zero is serverSelectTimeout.
	selectTimeout time.Duration
}

func main() { cli.RunSource(Source{}) }
