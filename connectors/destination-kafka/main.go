// Command destination-kafka produces every record as a JSON message to Kafka.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(Destination{}) }
