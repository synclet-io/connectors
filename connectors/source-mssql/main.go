// Command source-mssql reads tables and views of a Microsoft SQL Server database.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

// Source is the SQL Server source connector.
type Source struct{}

func main() { cli.RunSource(Source{}) }
