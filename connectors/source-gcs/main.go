// Command source-gcs reads CSV, TSV, JSONL and Parquet files from Google Cloud Storage.
package main

import (
	"github.com/synclet-io/synclet-go-sdk/cli"
	apioption "google.golang.org/api/option"
)

// Source is the Cloud Storage source connector. store is nil in main, where
// discover and read open the configured bucket; tests set it to an in-memory
// store, or set clientOptions and tokenURL to reach a fake Cloud Storage
// server and token endpoint.
type Source struct {
	store         objectStore
	clientOptions []apioption.ClientOption
	tokenURL      string
}

// tokenEndpoint is where a service account key's tokens come from.
func (src Source) tokenEndpoint() string {
	if src.tokenURL != "" {
		return src.tokenURL
	}
	return googleTokenURL
}

func main() { cli.RunSource(Source{}) }
