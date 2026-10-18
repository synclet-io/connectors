# destination-postgres

Postgres destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Writes every stream into one table of the configured schema, as overwrite, append or append with
deduplication. Works with Postgres 12 and later; tested against 16.

```bash
task test CONNECTOR=destination-postgres     # needs Docker; go test -short skips the database tests
task build CONNECTOR=destination-postgres
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-postgres:dev check --config /data/config.json
```

## Configuration

The keys match Airbyte's destination-postgres, so an existing configuration carries over.

| Key | Default | Meaning |
|---|---|---|
| `host`, `port` | port `5432` | Database server |
| `database` | | Database name |
| `schema` | `public` | Schema every table is written to; created when missing |
| `username`, `password` | | Login |
| `ssl_mode` | `{"mode": "prefer"}` | libpq's `sslmode`: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`, with `ca_certificate`, `client_certificate` and `client_key` as PEM strings, as in [source-postgres](../source-postgres/README.md#ssl-modes) |

```json
{
  "host": "db.internal",
  "database": "warehouse",
  "schema": "raw",
  "username": "synclet_writer",
  "password": "...",
  "ssl_mode": {"mode": "verify-full", "ca_certificate": "-----BEGIN CERTIFICATE-----\n..."}
}
```

The user needs `CREATE` on the schema (or on the database, when the schema does not exist yet) and
`TEMP` on the database. `check` names the cause of a failure: wrong user name or password, an
unreachable host, a missing database, no `CREATE` privilege on the schema, a missing schema the user
cannot create, or no `TEMP` privilege. `check` creates nothing.

Only the configuration reaches the Postgres driver: the `PG*` environment variables and `~/.pgpass`
are ignored. Sessions run with `TimeZone=UTC`. There is no SSH tunnel.

## Tables and naming

- One table per stream, named after the stream, in the configured `schema`. The stream's namespace is
  ignored, so two streams that map to the same table fail the sync before anything is written.
- Table and column names are used as given, double-quoted, case and punctuation kept.
- Postgres cuts identifiers at 63 bytes. A name longer than 63 bytes becomes its first 54 bytes (cut
  back to a UTF-8 character boundary), `_` and the first 8 hex digits of the SHA-256 of the full name:
  stable across syncs and unique for names that share a long prefix. Two names that still map to
  the same identifier fail the sync, naming both.
- Every table gets two metadata columns ahead of the stream's properties: `_airbyte_raw_id uuid`
  (random per row) and `_airbyte_extracted_at timestamptz` (the record's `emitted_at`).
- A property that is new in the catalog adds a nullable column. A column whose type differs from the
  one the catalog maps to fails the sync; columns are never altered or dropped.
- Fields of a record that are not in the stream's schema are not written; one warning per stream
  lists them.

## Types

Records are decoded with their numbers as text, so no value passes through a float.

| JSON schema | Column |
|---|---|
| `boolean` | `boolean` |
| `integer`, `airbyte_type: integer` | `bigint`; a value outside the int64 range fails the sync |
| `number`, `airbyte_type: big_integer` | `numeric` without precision or scale; the exact digits of the JSON text, never `NULL` |
| `string` | `text` |
| `string`, format `date` | `date` |
| `string`, format `date-time` or `airbyte_type: timestamp_with_timezone` | `timestamptz` |
| `string`, format `time` or `airbyte_type: time_without_timezone` | `time` |
| `string`, `airbyte_type: timestamp_without_timezone` | `timestamp` |
| `string`, `airbyte_type: time_with_timezone` | `timetz` |
| `$ref` to Airbyte's `WellKnownTypes.json` | the type of that definition |
| `object`, `array`, more than one non-null type, `anyOf`/`oneOf`, no type | `jsonb` |

A JSON `null` or a missing field lands as `NULL`. Numbers given as JSON strings (`"12.5"`) are accepted
in `bigint` and `numeric` columns.

## Sync modes

Records are loaded in batches. A batch is one transaction: each stream's rows go through `COPY` into a
temporary table (`ON COMMIT DROP`) and from there into the target table.

| Mode | Table |
|---|---|
| `overwrite` | The sync writes into `_synclet_tmp_<table>`. After the last batch one transaction drops the old table and renames the new one, so readers see the old rows until the sync succeeds. A failed sync drops `_synclet_tmp_<table>` and leaves the old table as it was |
| `append` | `INSERT INTO <table> SELECT ... FROM <staging>` in arrival order |
| `append_dedup` | The table has a `PRIMARY KEY` on the stream's primary key. Each batch is reduced to one row per key, the one with the highest cursor (`DISTINCT ON (pk) ORDER BY cursor DESC NULLS LAST, arrival DESC`), then `INSERT ... ON CONFLICT (pk) DO UPDATE ... WHERE t.cursor IS NULL OR excluded.cursor >= t.cursor`, so a row with a lower cursor arriving later does not replace the stored one |

Deduplication rules:

- Without a cursor field the last arrival wins.
- Among equal cursors the last arrival wins.
- A record whose primary key is `NULL` or missing fails the sync.
- An existing table without a primary key on exactly the stream's key fails the sync.
- Primary key and cursor must be top-level properties.

## Batching and state

- A batch is committed at 10,000 rows or 32 MiB of record data, on a STATE when the last commit is 30
  seconds old or older, and at the end of the input.
- A STATE is echoed only after every record received before it is committed. A STATE that arrives
  with no record pending is echoed at once.
- The STREAM states of an `overwrite` stream, and GLOBAL and LEGACY states while the catalog has an
  `overwrite` stream, are held until the final swap commits: before it, the records are not in the
  table readers see. A STREAM state is matched to its catalog stream by name and namespace, and, when
  no stream has that exact namespace, by name alone: it is held if any `overwrite` stream has its
  name, or if it matches no catalog stream at all while the catalog has an `overwrite` stream.

## Failures

The connector fails the sync rather than drop, null or truncate a value. The error names the stream,
the record's position in the stream and the column:

- a record that is not a JSON object or not valid JSON;
- a value that does not fit its column: a non-numeric number, an integer outside `bigint`, a date or
  time Postgres cannot parse, a time zone (`Z`, an offset or a zone name) in a `timestamp` or `time`
  value, which Postgres would drop, `\u0000` in a `text`, date/time or `jsonb` value, a string that is
  not valid UTF-8 or holds a lone UTF-16 surrogate (`\ud800`);
- a `NULL` primary key in `append_dedup`;
- a changed column type, a record of a stream that is not in the catalog.

On failure the open batch is rolled back, no state is echoed for its records, and the shadow tables of
`overwrite` streams are dropped. Batches committed before the failure stay, and so do the states
echoed for them.

## Limits

- No CDC deletes: a source's delete markers are written as ordinary rows.
- No schema evolution beyond adding nullable columns.
- No SSH tunnel.
- The stream namespace is ignored; every table goes in the configured schema.
