// Command source-example emits a fixed number of generated rows into one stream.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

// Source is the example source connector.
type Source struct{}

func main() { cli.RunSource(Source{}) }
