# destination-s3-data-lake

S3 Data Lake destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Writes each
stream as an [Apache Iceberg](https://iceberg.apache.org) table on S3-compatible storage (AWS S3, MinIO,
Cloudflare R2 and other stores that speak the S3 API), registered in an Iceberg REST catalog. Uses
[iceberg-go](https://github.com/apache/iceberg-go) (Apache-2.0) with Parquet data files.

```bash
task test CONNECTOR=destination-s3-data-lake     # needs Docker; go test -short skips the Docker tests
task build CONNECTOR=destination-s3-data-lake
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-s3-data-lake:dev check --config /data/config.json
```

The tests run MinIO (`pgsty/minio`) and the reference REST catalog (`apache/iceberg-rest-fixture`) in Docker on
one network, plus an in-process proxy in front of the catalog that demands a token or answers commits with 409.
They need no real credentials.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `catalog.catalog_type` | `rest` | The only catalog type |
| `catalog.uri` | | Base URL of the REST catalog, without `/v1` |
| `catalog.warehouse` | | Warehouse sent to the catalog's `/v1/config`; empty when the catalog needs none |
| `catalog.credential` | | OAuth2 `client_id:client_secret`, exchanged for a token at the catalog's token endpoint |
| `catalog.token` | | A static bearer token; set this or `credential` |
| `catalog.scope` | `catalog` | Scope requested with `credential` |
| `namespace` | | Namespace every table is written to; dots separate levels (`analytics.raw`) |
| `s3.warehouse_location` | | `s3://bucket/prefix`; a table's files go under `<warehouse_location>/<namespace levels>/<table>` |
| `s3.region` | `us-east-1` | Region of the bucket; `auto` for R2 |
| `s3.endpoint` | | URL of an S3-compatible service; empty for AWS S3 |
| `s3.force_path_style` | `false` | Bucket in the URL path instead of the host name; MinIO needs it |
| `s3.credentials` | | See [Authentication](#authentication) |
| `number_type` | `decimal` | `decimal` writes numbers exactly as `decimal(38, decimal_scale)`; `double` as `double` |
| `decimal_scale` | `9` | Digits after the decimal point of number columns, 0 to 38 |
| `dedup_key_limit` | `1000000` | Most distinct primary keys per `append_dedup` stream with a cursor that one sync remembers; see [Sync modes](#sync-modes) |

```json
{
  "catalog": {"catalog_type": "rest", "uri": "https://polaris.example.com/api/catalog", "warehouse": "lake", "credential": "client:secret", "scope": "PRINCIPAL_ROLE:ALL"},
  "namespace": "synclet",
  "s3": {
    "warehouse_location": "s3://lake/warehouse",
    "region": "eu-central-1",
    "credentials": {"auth_type": "assume_role", "role_arn": "arn:aws:iam::123456789012:role/synclet-writer"}
  }
}
```

### Authentication

| `s3.credentials.auth_type` | Fields |
|---|---|
| `access_key` | `access_key_id`, `secret_access_key` |
| `default_chain` | none: environment variables, shared configuration, web identity, container or instance role |
| `assume_role` | `role_arn`, optional `external_id`; the role is assumed with the default chain's credentials |

The connector writes the data and manifest files itself, with these credentials and this endpoint, whatever
file IO properties or vended credentials the catalog returns; the catalog writes the table metadata files with
its own. The credentials need `s3:PutObject`, `s3:GetObject` and `s3:DeleteObject` under `warehouse_location`.

`check` writes and deletes a probe object `<warehouse_location>/_synclet_check_<uuid>`, connects to the
catalog and checks that the namespace exists. A failure names its cause: `invalid S3 credentials`,
`bucket "<name>" does not exist`, `no write permission` or `no delete permission`, `cannot reach catalog at
<uri>`, `catalog rejected the credentials`, or `namespace "<name>" does not exist`.

## Tables

Each stream is written to the table `<namespace>.<stream name>`; stream namespaces are ignored, so two
streams of one name fail the sync before anything is written. The namespace must exist: the connector never
creates it. A missing table is created unpartitioned, in format v2, at `<warehouse_location>/<namespace
levels>/<stream name>`.

Every table has `_airbyte_raw_id` (`string`, a UUID) and `_airbyte_extracted_at` (`timestamptz`, the record's
`emitted_at`), then one optional column per top-level property of the stream's JSON schema, in schema order:

| JSON schema | Iceberg |
|---|---|
| `boolean` | `boolean` |
| `integer`, `number` with `airbyte_type: integer` | `long` |
| `number` | `decimal(38, decimal_scale)`; `double` with `number_type: double` |
| `big_integer` (`integer`, `number` or `string` with `airbyte_type: big_integer`) | `decimal(38, 0)` |
| `string` | `string` |
| `string`, `format: date` | `date` |
| `string`, `format: date-time` | `timestamptz`; `timestamp` with `airbyte_type: timestamp_without_timezone` |
| `string`, `format: time` (`airbyte_type: time_without_timezone` or none) | `time` |
| `string`, `format: time`, `airbyte_type: time_with_timezone` | `string` holding the value as sent |
| `object`, `array`, a union of types, `anyOf`, `oneOf`, no type | `string` holding the compact JSON |

Timestamps and times are microseconds; a `date-time` without an offset in a `timestamptz` column is read as
UTC. A non-string value in a `string` column is written as its JSON text.

### Schema evolution

- A field the table lacks is added as an optional column, in one schema commit before any data; rows written
  earlier read it as null.
- A field the catalog no longer has keeps its column, written null.
- A field whose type differs from its column's fails the sync before anything is written, naming the column,
  the table's type and the catalog's; the table is unchanged.

## Sync modes

| Mode | Snapshots |
|---|---|
| `append` | Each flush commits one `append` snapshot per stream with records in it |
| `overwrite` | Data files are written at every flush but committed once, at the end of a successful sync, as one `overwrite` snapshot holding only this sync's files (a `delete` snapshot when the sync had no records). Readers see the previous snapshot until then; a failed sync commits nothing and removes its files |
| `append_dedup` | Each flush commits one `overwrite` snapshot per stream (an Iceberg row delta): data files plus an equality-delete file on the primary key, which deletes the rows of earlier snapshots with those keys (merge-on-read) |

`append_dedup` needs a primary key of top-level fields, none of them a `double` column; a stream without one
fails when the write starts (`check` sees no catalog). A null primary key fails the sync.

Within a sync, the record with the highest cursor wins, the latest one among equal cursors; a null cursor is
the lowest. The sync remembers the highest cursor written per primary key across its flushes, so a record
whose cursor is lower than one already written for its key is skipped, never written; the number skipped is
logged as a warning per stream at the end. Remembering costs memory per key: a sync of a stream with more
than `dedup_key_limit` distinct keys fails rather than drop the guard. A stream without a cursor keeps the
latest record per key.

Across syncs the later record wins whatever its cursor: sources emit incremental records in cursor order and
resume from the last state, so only a source that emits out of cursor order can bring an older version of a
row back.

## Files and state

- Records are buffered in memory and written when the buffers hold 50,000 records or 64 MiB of record data,
  on a STATE when the last commit is 60 seconds old or older, and at the end of the input. The buffer is the
  bound on memory: records are never accumulated beyond a flush, also for `overwrite`, which keeps only the
  written files' metadata until its final commit.
- A STATE is echoed only after the snapshots holding every record received before it are committed.
- The STREAM states of an `overwrite` stream, and GLOBAL and LEGACY states while the catalog has an
  `overwrite` stream, are held until the `overwrite` snapshots are committed.

## Commits and failures

- A commit that conflicts with another writer (HTTP 409, `CommitFailedException`) reloads the table and is
  rebuilt on it from the files already written, up to 5 attempts with backoff from 100 ms to 2 s; then the
  sync fails with `stream "<name>": commit to <table> failed after 5 conflicts`.
- A commit the catalog rejects outright (HTTP 400, 401, 403, 404 or 419) is not retried: the sync fails,
  naming the rejection, and its files are removed.
- A commit whose outcome is unknown (a server error, another status, or a broken connection while committing) is never retried:
  the sync fails and its files stay, since the catalog may have accepted them.
- Any other failed commit, and any failure before a commit, removes the uncommitted data and delete files
  (best-effort, logged when a removal fails) and echoes no state for their records.

The connector fails the sync rather than drop, null or truncate a value. The error names the stream, the
record's position, the column and the value: a wrong type, a number with more decimal places than
`decimal_scale` or more than 38 digits, a time or timestamp with digits below microseconds, an integer outside
`long`, a timestamp with an offset in a `timestamp` column, a record that is not a JSON object, or a record of a
stream not in the catalog. Record fields not in the stream's schema are not written; they are logged once per
stream.

## Limits

- REST catalog only; AWS Glue is not supported. Nessie works through its Iceberg REST API, untested.
- Tables are unpartitioned and never compacted; expire snapshots and compact files with the catalog's or an
  engine's maintenance.
- No type changes, renames or column drops; no nested columns: objects and arrays are JSON strings.
- `append_dedup` keeps every equality-delete file; readers merge them until the table is compacted.
- No CDC deletes: a source's delete markers are written as ordinary records.
