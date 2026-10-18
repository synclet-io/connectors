// Command destination-postgres writes records into tables of a Postgres database.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(NewDestination()) }
