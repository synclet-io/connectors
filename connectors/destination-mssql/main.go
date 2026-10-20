// Command destination-mssql writes records into tables of a SQL Server or
// Azure SQL database.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(NewDestination()) }
