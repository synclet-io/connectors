// Command destination-mysql writes records into tables of a MySQL database.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(NewDestination()) }
