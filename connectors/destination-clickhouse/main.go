// Command destination-clickhouse writes records into tables of a ClickHouse database.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(NewDestination()) }
