# destination-clickhouse

ClickHouse destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Writes every stream into one table of the configured database, as overwrite, append or append with
deduplication, over the native protocol or HTTP with [clickhouse-go](https://github.com/ClickHouse/clickhouse-go).
Works with ClickHouse 24.5 and later; tested against 25.8.

```bash
task test CONNECTOR=destination-clickhouse     # needs Docker; go test -short skips the database tests
task build CONNECTOR=destination-clickhouse
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-clickhouse:dev check --config /data/config.json
```

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `host`, `port` | `9000` native, `9440` native with TLS, `8123` http, `8443` http with TLS | Server |
| `protocol` | `native` | `native` or `http` |
| `database` | | Database every table is written to; it must exist and is never created |
| `username`, `password` | | Login |
| `tls` | `{"enabled": false}` | `enabled`, with `ca_certificate`, `client_certificate` and `client_key` as PEM strings; without `ca_certificate` the system roots verify the server |
| `optimize_final` | `false` | Run `OPTIMIZE TABLE ... FINAL` on every `append_dedup` table after the last insert |

```json
{
  "host": "clickhouse.internal",
  "protocol": "native",
  "database": "raw",
  "username": "synclet_writer",
  "password": "...",
  "tls": {"enabled": true, "ca_certificate": "-----BEGIN CERTIFICATE-----\n..."}
}
```

The user needs `CREATE TABLE`, `INSERT`, `SELECT`, `DROP TABLE` and `ALTER ADD COLUMN` on `<database>.*`.
The database engine must be `Atomic`, `Replicated` or `Shared`, the engines `EXCHANGE TABLES` works in.
`check` names the cause of a failure: wrong user name or password, an unreachable host, a missing
database, a database engine without `EXCHANGE TABLES`, or a missing privilege (`CHECK GRANT`, which
needs ClickHouse 24.5). `check` creates nothing.

There is no SSH tunnel.

## Tables and naming

- One table per stream, named after the stream, in the configured `database`. The stream's namespace
  is ignored, so two streams that map to the same table fail the sync before anything is written.
- Table and column names are used as given, in backquotes, case and punctuation kept.
- ClickHouse stores names in file names. A name longer than 64 bytes becomes its first 55 bytes (cut
  back to a UTF-8 character boundary), `_` and the first 8 hex digits of the SHA-256 of the full name:
  stable across syncs and unique for names that share a long prefix. Two names that still map to the
  same identifier fail the sync, naming both.
- Every table gets two metadata columns ahead of the stream's properties: `_airbyte_raw_id UUID`
  (random per row) and `_airbyte_extracted_at DateTime64(6, 'UTC')` (the record's `emitted_at`).
- A property that is new in the catalog adds a `Nullable` column. A column whose type differs from the
  one the catalog maps to fails the sync; columns are never altered or dropped.
- Fields of a record that are not in the stream's schema are not written; one warning per stream
  lists them.

## Types

Every column of a property is `Nullable(T)`, except the primary key and cursor columns of an
`append_dedup` stream. Records are decoded with their numbers as text, so no value passes through a
float.

| JSON schema | Column |
|---|---|
| `boolean` | `Bool` |
| `integer`, `airbyte_type: integer` | `Int64`; a value outside the Int64 range fails the sync |
| `number` | `Decimal(76, 38)`: the exact digits of the JSON text; more than 38 digits on either side of the point fail the sync |
| `airbyte_type: big_integer` | `Decimal(76, 0)`; a fraction or more than 76 digits fails the sync |
| `string` | `String` |
| `string`, format `date` | `Date32`, 1900-01-01 to 2299-12-31 |
| `string`, format `date-time` or `airbyte_type: timestamp_with_timezone` | `DateTime64(6, 'UTC')`, converted to UTC |
| `string`, `airbyte_type: timestamp_without_timezone` | `DateTime64(6, 'UTC')` holding the wall-clock time as written |
| `string`, format `time` or `airbyte_type: time_with_timezone` / `time_without_timezone` | `String`, as written |
| `$ref` to Airbyte's `WellKnownTypes.json` | the type of that definition |
| `object`, `array`, more than one non-null type, `anyOf`/`oneOf`, no type | `String` with the exact JSON text |

Objects land as JSON text rather than ClickHouse's `JSON` type, whose inference turns large numbers
into `Float64`; ClickHouse's JSON functions (`JSONExtract...`) read the text.

`DateTime64(6)` holds 1900 to 2299 on the server, but clickhouse-go encodes a timestamp through Go's
`time.UnixNano`, which ends at 2262-04-11 23:47:16.854775807 UTC. A timestamp outside
1900-01-01 00:00:00 to 2262-04-11 23:47:16.854775 UTC fails the sync rather than wrap around.

A JSON `null` or a missing field lands as `NULL`. Numbers given as JSON strings (`"12.5"`) are accepted
in `Int64` and `Decimal` columns.

## Sync modes

| Mode | Table |
|---|---|
| `overwrite` | `MergeTree ORDER BY tuple()`. The sync writes into `_synclet_tmp_<table>`. After the last insert `EXCHANGE TABLES` swaps it with the old table atomically (`RENAME TABLE` when there is no old table) and the old table is dropped, so readers see the old rows until the sync succeeds. A failed sync drops `_synclet_tmp_<table>` and leaves the old table as it was. Each stream's swap is atomic on its own; ClickHouse has no transaction across tables |
| `append` | `MergeTree ORDER BY tuple()`; rows are inserted in arrival order |
| `append_dedup` | `ReplacingMergeTree(<cursor>) ORDER BY (<primary key>)` |

Deduplication in ClickHouse is eventual. Every record is inserted; ReplacingMergeTree keeps one row per
primary key, the one with the highest cursor, only when background merges reach the rows. Until then a
plain `SELECT` can return older versions. Read with `FINAL` (`SELECT ... FROM t FINAL`) for one row per
key, or set `optimize_final` to merge every `append_dedup` table at the end of each sync, which is
costly on large tables.

Deduplication rules:

- The cursor must map to `Int64`, `Date32` or `DateTime64`; any other cursor type fails the sync before
  anything is written.
- Without a cursor field (`ReplacingMergeTree()`) the last inserted row wins.
- Among equal cursors the last inserted row wins.
- A record whose primary key or cursor is `NULL` or missing fails the sync.
- An existing table that is not a ReplacingMergeTree sorted by exactly the stream's primary key with
  the stream's cursor as version fails the sync.
- Primary key and cursor must be top-level properties.

## Batching and state

- Rows are inserted at 10,000 rows or 32 MiB of record data, on a STATE when the last insert is 30
  seconds old or older, and at the end of the input: one `INSERT` per stream with pending rows.
- Inserts run with `async_insert=0`: an `INSERT` returns once the server has written its rows to a
  part. A STATE is echoed only after every record received before it is inserted. A STATE that
  arrives with no record pending is echoed at once.
- The STREAM states of an `overwrite` stream, and GLOBAL and LEGACY states while the catalog has an
  `overwrite` stream, are held until every swap is done: before it, the records are not in the table
  readers see.
- On a replicated table, an acknowledged insert is on one replica. For a stronger guarantee set
  `insert_quorum` (and `insert_quorum_parallel=0`) in the user's settings profile on the server. With
  `async_insert=1` in the profile the connector still sets `async_insert=0` per session; if a profile
  forces async inserts, `wait_for_async_insert=1` must stay on, or an acknowledged insert may not be
  written yet.

ClickHouse has no transaction across `INSERT`s. When the insert of one stream fails, the streams
inserted before it in the same flush keep their rows, and no state is echoed for any of them, so the
next sync sends those records again: `append` is at-least-once and may hold duplicates after a failed
sync; `append_dedup` collapses them.

## Failures

The connector fails the sync rather than drop, null or truncate a value. The error names the stream,
the record's position in the stream and the column:

- a record that is not a JSON object or not valid JSON;
- a value that does not fit its column: a non-numeric number, an integer outside `Int64`, a number
  with more digits than its `Decimal` holds, an invalid date, a date or timestamp outside the ranges
  above, fraction digits past microseconds that are not zeros, a time zone in a timestamp without
  time zone, a string that is not valid UTF-8 or holds a lone UTF-16 surrogate (`\ud800`);
- a `NULL` primary key or cursor in `append_dedup`;
- a changed column type, a record of a stream that is not in the catalog;
- an `INSERT` the server rejects.

`\u0000` is kept in `String` columns. On failure no state is echoed for the records not yet inserted,
and the shadow tables of `overwrite` streams are dropped. Inserts acknowledged before the failure
stay, and so do the states echoed for them.

## Limits

- No CDC deletes: a source's delete markers are written as ordinary rows.
- No schema evolution beyond adding `Nullable` columns.
- No SSH tunnel; no cluster DDL (`ON CLUSTER`): on a cluster, use a `Replicated` database.
- The stream namespace is ignored; every table goes in the configured database.
- Timestamps end at 2262-04-11 (see Types).
