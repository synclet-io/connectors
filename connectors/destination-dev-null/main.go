// Command destination-dev-null accepts every record and stores none of them.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(Destination{}) }
