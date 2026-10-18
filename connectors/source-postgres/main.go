// Command source-postgres reads tables and views of a Postgres database.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

// Source is the Postgres source connector.
type Source struct{}

func main() { cli.RunSource(Source{}) }
