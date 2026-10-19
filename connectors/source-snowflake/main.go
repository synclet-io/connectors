// Command source-snowflake reads tables and views of a Snowflake database.
package main

import (
	"net/url"

	"github.com/synclet-io/synclet-go-sdk/cli"
)

// Source is the Snowflake source connector. endpoint, when set, replaces the
// account's host, port and protocol.
type Source struct {
	endpoint *url.URL
}

func main() {}
var _ = cli.RunSource[Config]
