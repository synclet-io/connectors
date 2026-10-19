// Command destination-s3-data-lake writes streams as Apache Iceberg tables on
// S3-compatible storage through an Iceberg REST catalog.
package main

import (
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	"github.com/synclet-io/synclet-go-sdk/cli"
)

func main() { cli.RunDestination(NewDestination()) }
