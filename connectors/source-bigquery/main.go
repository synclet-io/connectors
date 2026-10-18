// Command source-bigquery reads tables and views of BigQuery datasets.
package main

import (
	"github.com/synclet-io/synclet-go-sdk/cli"
	"google.golang.org/api/option"
)

// Source is the BigQuery source connector. clientOpts, when set, replace the
// configured credentials in every BigQuery client it creates.
type Source struct {
	clientOpts []option.ClientOption
}

func main() { cli.RunSource(Source{}) }
