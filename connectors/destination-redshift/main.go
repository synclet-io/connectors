// Command destination-redshift loads records into Redshift tables through S3 staging and COPY.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

func main() { cli.RunDestination(NewDestination()) }
