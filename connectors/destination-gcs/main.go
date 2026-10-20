package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() {
	cli.RunDestination(NewGCSDestination())
}
