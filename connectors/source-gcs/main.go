// Command source-gcs reads CSV, TSV, JSONL and Parquet files from Google Cloud Storage.
package main

import (
	"github.com/synclet-io/synclet-go-sdk/cli"
	apioption "google.golang.org/api/option"
)

// Source is the Cloud Storage source connector. store is nil in main, where
// discover and read open the configured bucket; tests set it to an in-memory
// store, or set clientOptions to reach a fake Cloud Storage server.
type Source struct {
	store         objectStore
	clientOptions []apioption.ClientOption
}

func main() { cli.RunSource(Source{}) }
