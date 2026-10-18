# destination-bigquery

BigQuery destination on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk): overwrite, append and append_dedup, loaded directly or staged through GCS. The integration tests need a real project (`BIGQUERY_PROJECT_ID`) and run with `go test -tags integration ./...`.

```bash
task test CONNECTOR=destination-bigquery
task build CONNECTOR=destination-bigquery
task spec CONNECTOR=destination-bigquery
```
