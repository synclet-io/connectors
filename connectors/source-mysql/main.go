// Command source-mysql reads tables and views of a MySQL database.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

// Source is the MySQL source connector.
type Source struct{}

func main() { cli.RunSource(Source{}) }
