// Command destination-databricks writes records into Delta tables of a
// Databricks Unity Catalog schema.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(NewDestination()) }
