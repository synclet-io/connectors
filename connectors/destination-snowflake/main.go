// Command destination-snowflake writes records into tables of a Snowflake database.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(NewDestination()) }
