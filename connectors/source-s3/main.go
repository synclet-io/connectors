// Command source-s3 reads CSV, TSV, JSONL and Parquet files from S3-compatible storage.
package main

import "github.com/synclet-io/synclet-go-sdk/cli"

// Source is the S3 source connector. store is nil in main, where discover
// and read open the configured bucket; tests set it to an in-memory store.
type Source struct {
	store objectStore
}

func main() { cli.RunSource(Source{}) }
