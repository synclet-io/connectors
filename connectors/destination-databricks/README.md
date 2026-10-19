# destination-databricks

Databricks destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Writes every stream into one Delta table of a Unity Catalog schema, as overwrite, append or append
with deduplication. Batches are staged as JSON files in a Unity Catalog volume through the Files API
and loaded with `COPY INTO`; SQL runs on a SQL warehouse through
[databricks-sql-go](https://github.com/databricks/databricks-sql-go).

```bash
task test CONNECTOR=destination-databricks     # offline: SQL and staging run against fakes
task build CONNECTOR=destination-databricks
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-databricks:dev check --config /data/config.json
```

## Configuration

The keys follow Airbyte's destination-databricks where it has one.

| Key | Default | Meaning |
|---|---|---|
| `hostname` | | Workspace host name, from the SQL warehouse's connection details |
| `http_path` | | HTTP path of the SQL warehouse |
| `port` | `443` | HTTPS port |
| `database` | | Unity Catalog catalog |
| `schema` | `default` | Schema every table is written to; it must exist |
| `volume` | | Volume in the schema where batches are staged; it must exist |
| `authentication` | | `{"auth_type": "BASIC", "personal_access_token": "..."}` or `{"auth_type": "OAUTH", "client_id": "...", "secret": "..."}` (OAuth machine-to-machine service principal) |

```json
{
  "hostname": "dbc-a1b2c3d4-e5f6.cloud.databricks.com",
  "http_path": "/sql/1.0/warehouses/0123456789abcdef",
  "database": "main",
  "schema": "raw",
  "volume": "synclet_staging",
  "authentication": {"auth_type": "OAUTH", "client_id": "...", "secret": "..."}
}
```

The principal needs `CAN USE` on the warehouse, `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` and
`MODIFY` on the schema's tables, and `READ VOLUME` and `WRITE VOLUME` on the volume. `check` names
the cause of a failure: an invalid token or rejected client credentials, an unreachable SQL
warehouse or none at `http_path`, a missing catalog, schema or volume, or no write privilege on the
volume or the schema. It writes and removes a probe file and a probe table.

## Tables

Every stream lands in a Delta table named after the stream, lower-cased, with every character
outside `[a-z0-9_]` replaced by `_`; namespaces are ignored. Tables use column mapping
(`delta.columnMapping.mode = name`), so a column keeps its property's name whatever characters it
has. Two streams or two properties whose names map to the same table or column (Databricks ignores
case) fail the sync before anything is written. Each table gets `_airbyte_raw_id STRING` and
`_airbyte_extracted_at TIMESTAMP`.

| JSON schema | Delta type |
|---|---|
| `boolean` | `BOOLEAN` |
| `integer`, `number` with `airbyte_type: integer` | `BIGINT` |
| `number` | `DECIMAL(38,9)` |
| `string` with `airbyte_type: big_integer` | `DECIMAL(38,0)` |
| `string` with `format: date` | `DATE` |
| `string` with `format: date-time`, `timestamp_with_timezone` | `TIMESTAMP` (stored in UTC) |
| `timestamp_without_timezone` | `TIMESTAMP_NTZ` |
| times, `string` | `STRING` (Databricks has no time type) |
| `object`, `array`, unions, no type | `VARIANT` |

Values are checked before they are staged, and a value the column cannot hold exactly fails the
sync, naming the stream, the record and the column: a number with more than 9 digits after the point
or 29 before it, an integer outside `BIGINT`, a timestamp with digits finer than microseconds, a zone
on a `TIMESTAMP_NTZ`, a malformed date, or a string that is not valid UTF-8. A timestamp without a
zone is read as UTC. Record fields that are not in the stream's schema are not written; they are
logged once per stream.

## Sync modes

- **append** copies each batch into a stage table and inserts it into the table.
- **append_dedup** merges each batch on the primary key: within a batch the record with the highest
  cursor wins (the latest among equal cursors), and a stored row is replaced only by a cursor that
  is not lower. A null primary key fails the sync.
- **overwrite** writes into `_synclet_tmp_<table>` and replaces the table with it in one
  `CREATE OR REPLACE TABLE ... AS SELECT` at the end of a successful sync; a failed sync drops it and
  the old table stays.

A batch is committed at 50,000 records, 64 MiB of record data, on a STATE a minute after the last
commit, and at the end of the input. Every stream's batch is copied into its stage table first, and
only when every copy loaded all its rows are the stage tables appended or merged into the tables. A
STATE is echoed once the records before it are committed; the states of an overwrite stream, and
GLOBAL and LEGACY states while the catalog has an overwrite stream, wait for the replace. If the
append or merge of one stream fails after another stream's batch was committed, no state is echoed
for that batch, so a retry may write the committed stream's records again.

## Tests

The offline tests run SQL generation, staging and the state protocol against a fake Files API and
a SQL-recording warehouse, and the check's connection failures against databricks-sql-go talking to
a local HTTPS server. `TestLiveWrite` runs against a real workspace when `DATABRICKS_TEST_HOST` is
set, with `DATABRICKS_TEST_HTTP_PATH`, `DATABRICKS_TEST_CATALOG` and `DATABRICKS_TEST_TOKEN` (or
`DATABRICKS_TEST_CLIENT_ID` and `DATABRICKS_TEST_CLIENT_SECRET`); it creates and drops a schema of
its own, so the principal needs `CREATE SCHEMA` on the catalog. It skips otherwise.
