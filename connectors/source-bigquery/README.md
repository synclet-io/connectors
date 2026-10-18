# source-bigquery

BigQuery source built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Reads tables, views and materialized views of BigQuery datasets, as full refresh or incremental by a
cursor column you choose.

```bash
task test CONNECTOR=source-bigquery     # needs Docker; go test -short skips the emulator tests
task build CONNECTOR=source-bigquery
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-bigquery:dev check --config /data/config.json
```

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `project_id` | | Project that runs the query jobs and holds the datasets |
| `datasets` | every dataset of the project | Datasets to discover and read |
| `location` | chosen by BigQuery | Location the query jobs run in, such as `US` or `europe-west1` |
| `credentials` | `{"auth_type": "ApplicationDefault"}` | `{"auth_type": "Service", "service_account_info": "<key JSON>"}`, or Application Default Credentials |

## Auth

The account (service account or ADC principal) needs:

- **BigQuery Job User** (`roles/bigquery.jobUser`) on `project_id`: check runs `SELECT 1`, and views and
  incremental reads run queries.
- **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) on each dataset read, or on the project.
  Discovering every dataset (`datasets` empty) needs `bigquery.datasets.list` on the project too, which
  Data Viewer on the project grants.

`check` names the cause of a failure: a `service_account_info` that is not a valid key, a project or
dataset that does not exist, or the missing Job User or Data Viewer role.

## Types

Every property is nullable.

| BigQuery | JSON schema | Value |
|---|---|---|
| `INT64` | `integer` | exact, also beyond 2^53 |
| `NUMERIC`, `BIGNUMERIC` | `number` | exact digits, trailing zeros dropped |
| `FLOAT64` | `number` | `NaN`, `Infinity` and `-Infinity` read as `null` |
| `BOOL` | `boolean` | |
| `STRING` | `string` | |
| `BYTES` | `string` | base64 |
| `DATE` | `string`, format `date` | `2024-02-29` |
| `TIME` | `string`, airbyte_type `time_without_timezone` | `23:59:59.123456` |
| `DATETIME` | `string`, airbyte_type `timestamp_without_timezone` | `2024-02-29T12:34:56.789` |
| `TIMESTAMP` | `string`, format `date-time`, airbyte_type `timestamp_with_timezone` | RFC 3339 in UTC, `2024-02-29T10:34:56.789Z` |
| `GEOGRAPHY` | `string` | WKT, `POINT(30.5 50.45)` |
| `JSON` | any (`{}`) | the document |
| `INTERVAL` | `string` | BigQuery's canonical form, `1-2 3 4:5:6` |
| `RANGE` | `string` | `[2024-01-01, UNBOUNDED)` |
| `STRUCT` (`RECORD`) | `object` with a property per field | object |
| `ARRAY` (`REPEATED`) | `array` of the element schema | array; a `NULL` array reads as `[]` |

Fractional seconds keep up to microseconds and drop trailing zeros.

## Streams

Discover returns one stream per table, view and materialized view; the namespace is the dataset.
External tables and snapshots are left out. A table's `PRIMARY KEY ... NOT ENFORCED` becomes the
stream's source-defined primary key. A configured stream without a namespace is read from the only
configured dataset; with several or none configured, the read fails with a config error.

A full refresh reads a table through `tabledata.list`, which costs no query bytes, and a view or
materialized view through a query.

## Incremental sync

Pick a column as the cursor (`cursor_field`, one top-level column of type `INT64`, `NUMERIC`,
`BIGNUMERIC`, `FLOAT64`, `STRING`, `DATE`, `TIME`, `DATETIME` or `TIMESTAMP`). The connector queries
`SELECT ... WHERE cursor >= CAST(@cursor AS <type>) ORDER BY cursor ASC NULLS LAST` and keeps the
greatest cursor, as its record value, in STREAM state:

```json
{"cursor_field": ["updated_at"], "cursor": "2024-01-05T00:00:00Z"}
```

- The next read emits rows whose cursor is **greater than or equal to** the saved one, so rows that
  share the last cursor value are emitted again; use a destination sync mode that deduplicates
  (`append_dedup`) when that matters.
- A STATE message follows every 10,000 rows and the end of the stream.
- Rows whose cursor is `NULL` are emitted by the first read only.
- When the saved state names a different cursor field than the catalog, the read logs a warning and
  starts the stream over.
- Every incremental read is a query and is billed for the bytes of the columns it reads; partition or
  cluster the table by the cursor to keep that down. Deletes are not seen.

## Limits

- The BigQuery Storage Read API is not used: reads go through `tabledata.list` and query results over
  REST, which is slower for very large tables.
- No CDC (change history) yet.

## Tests

Tests need no credentials. `go test -short ./...` runs the unit tests and the tests against a fake
HTTP server; `go test ./...` also starts the BigQuery emulator `ghcr.io/goccy/bigquery-emulator:0.8.1`
in Docker (testcontainers) and runs discover, read, incremental resume and the SDK conformance suite
against it.

Two cases are tested with canned responses in BigQuery's REST wire format (`testdata/real_*.json`)
instead of the emulator:

- `GEOGRAPHY`: the emulator renders WKT with a space after the type name (`POINT (30.5 50.45)`),
  BigQuery without one.
- Primary keys: the emulator drops table constraints.

`check` failures (bad key, missing project or dataset, missing role) are tested against a fake server
returning Google API error bodies.
