# destination-bigquery

BigQuery destination on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk): overwrite, append and append_dedup, loaded directly or staged through GCS. The integration tests need a real project (`BIGQUERY_PROJECT_ID`) and run with `go test -tags integration ./...`.

## Names

Datasets, tables and columns are named the way Airbyte's BigQuery destination names them, so a stream lands in the table Airbyte writes it to.

- **Tables and columns.** The stream or field name is decomposed (NFKD) and its combining marks dropped. Each run of whitespace and every other character that is not an ASCII letter, digit or `_` becomes `_`, and `_` is prepended when the name does not start with a letter or `_`. `Price (USD)` becomes `Price__USD_`, `№ of items` becomes `No_of_items`, `Sales Pipeline/Deals` becomes `Sales_Pipeline_Deals`.
- **Datasets.** The namespace, or `dataset_id` for a stream without one, is cleaned the same way. `n` is prepended when it does not start with a letter or digit.
- **Collisions.** BigQuery column names are case-insensitive. When two fields get the same column, the fields are taken in name order and the later one gets the first free name of `<field>_1`, `<field>_2`, ….

The primary key and cursor of a stream refer to these column names.

## Overwrite

An overwrite stream is loaded into `_airbyte_tmp_<table>`, which is partitioned and clustered like the final table, and copied over the final table at the end of the sync. A temp table left by a failed sync is dropped first.

```bash
task test CONNECTOR=destination-bigquery
task build CONNECTOR=destination-bigquery
task spec CONNECTOR=destination-bigquery
```
