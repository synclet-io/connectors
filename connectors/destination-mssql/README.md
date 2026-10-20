# destination-mssql

Microsoft SQL Server destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Writes every stream into one typed table of the configured schema, as overwrite, append or append with
deduplication. Works with SQL Server 2016 and later and with Azure SQL Database; tested against SQL
Server 2022.

```bash
MSSQL_TEST_DOCKER=1 task test CONNECTOR=destination-mssql   # without it the database tests are skipped
task build CONNECTOR=destination-mssql
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-mssql:dev check --config /data/config.json
```

The database tests start a throwaway `mcr.microsoft.com/mssql/server:2022-latest` container on a free
port and remove it afterwards; they never connect to any other server.

## Configuration

The keys match Airbyte's destination-mssql, so an existing configuration carries over.

| Key | Default | Meaning |
|---|---|---|
| `host`, `port` | port `1433` | Database server |
| `database` | | Database name; it must exist |
| `schema` | `dbo` | Schema every table is written to; created when missing |
| `username`, `password` | | SQL Server login (Windows and Entra ID logins are not supported) |
| `ssl_method` | `{"ssl_method": "encrypted_trust_server_certificate"}` | `unencrypted` (only the login is encrypted, when the server supports it), `encrypted_trust_server_certificate` (encrypted, certificate not checked) or `encrypted_verify_certificate` with optional `hostNameInCertificate` and `certificate` (a CA in PEM; empty: the CAs of the image) |

```json
{
  "host": "sql.internal",
  "database": "warehouse",
  "schema": "raw",
  "username": "synclet_writer",
  "password": "...",
  "ssl_method": {"ssl_method": "encrypted_verify_certificate", "hostNameInCertificate": "sql.internal"}
}
```

The user needs a user in the database with `CREATE TABLE` on the database and `ALTER`, `INSERT`,
`UPDATE` and `SELECT` on the schema (or `CREATE SCHEMA` on the database when the schema does
not exist yet; the schema it creates is its own). `check` runs every kind of statement a sync runs
(`CREATE SCHEMA` when the schema is missing, `CREATE TABLE`, `INSERT`, `MERGE`, `sp_rename`,
`DROP TABLE`) inside a transaction it rolls back, so it creates nothing. It names the cause of a
failure: wrong user name or password, an unreachable host, a missing database, a login without a user
in the database, an untrusted server certificate, or the permission a statement lacks.

Sessions run with `XACT_ABORT`, `ANSI_WARNINGS` and `ARITHABORT` on, so a failed statement rolls back
its whole batch. There is no SSH tunnel.

## Tables and naming

- One table per stream, named after the stream, in the configured `schema`. The stream's namespace is
  ignored, so two streams that map to the same table fail the sync before anything is written.
- Table and column names are used as given, in brackets, case and punctuation kept.
- SQL Server names hold 128 UTF-16 code units. A longer name becomes its first 119 code units (cut back
  to a whole character), `_` and the first 8 hex digits of the SHA-256 of the full name: stable across
  syncs and unique for names that share a long prefix. Names are compared without regard to case, as
  the usual database collations do, so two names that differ only in case, or that still map to the
  same identifier, fail the sync naming both. A name ending in a space fails too.
- Every table gets two metadata columns ahead of the stream's properties: `_airbyte_raw_id
  uniqueidentifier` (random per row) and `_airbyte_extracted_at datetimeoffset(7)` (the record's
  `emitted_at`, in UTC).
- String columns are created with the `Latin1_General_100_BIN2` collation, whatever the database
  collation is, so keys and cursors that differ in case or accents stay distinct.
- A property that is new in the catalog adds a nullable column. A column whose type differs from the
  one the catalog maps to fails the sync; columns are never altered or dropped.
- Fields of a record that are not in the stream's schema are not written; one warning per stream
  lists them.

## Types

Records are decoded with their numbers as text, and every value is checked in the connector before it
is bulk-copied, so nothing is rounded or truncated on the way.

| JSON schema | Column |
|---|---|
| `boolean` | `bit` |
| `integer`, `airbyte_type: integer` | `bigint`; an exact integer in any notation (`1.0`, `1e3`), outside the int64 range fails the sync |
| `number` | `float(53)` (a double); a value fails the sync unless the nearest double prints back as the same decimal value, so `0.1` passes and `0.912345678901234567890` fails |
| `airbyte_type: big_integer` | `decimal(38,0)`; more than 38 digits fail the sync |
| `string` | `nvarchar(max)` |
| `string`, format `date` | `date` |
| `string`, format `date-time` or `airbyte_type: timestamp_with_timezone` | `datetimeoffset(7)`, converted to UTC; a value without a zone is taken as UTC |
| `string`, `airbyte_type: timestamp_without_timezone` | `datetime2(7)` |
| `string`, format `time` or `airbyte_type: time_without_timezone` | `time(7)` |
| `string`, `airbyte_type: time_with_timezone` | `nvarchar(max)`, as written |
| `$ref` to Airbyte's `WellKnownTypes.json` | the type of that definition |
| `object`, `array`, more than one non-null type, `anyOf`/`oneOf`, no type | `nvarchar(max)` holding the JSON text exactly as the source wrote it; query it with `JSON_VALUE` and `OPENJSON` |

A JSON `null` or a missing field lands as `NULL`. Numbers given as JSON strings (`"12.5"`) are accepted
in numeric columns, and `"true"`, `"1"`, `"false"`, `"0"` in `bit` columns.

## Sync modes

Records are loaded in batches. A batch is one transaction: each stream's rows are bulk-copied (the TDS
bulk load of `mssql.CopyIn`) into a session temporary table, and moved from there into the target table.

| Mode | Table |
|---|---|
| `overwrite` | The sync writes into `_synclet_tmp_<table>`. After the last batch one transaction drops the old table and renames the new one with `sp_rename`, so readers see the old rows until the sync succeeds. A failed sync drops `_synclet_tmp_<table>` and leaves the old table as it was |
| `append` | `INSERT INTO <table> SELECT ... FROM <stage>` in arrival order |
| `append_dedup` | The table has a `PRIMARY KEY NONCLUSTERED` on the stream's primary key. Each batch is reduced to one row per key, the one with the highest cursor (`ROW_NUMBER() OVER (PARTITION BY pk ORDER BY cursor DESC, arrival DESC)`, a `NULL` cursor lowest), then `MERGE ... WITH (HOLDLOCK) WHEN MATCHED AND (t.cursor IS NULL OR s.cursor >= t.cursor) THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`, so a row with a lower cursor arriving later does not replace the stored one |

Deduplication rules:

- Without a cursor field the last arrival wins.
- Among equal cursors the last arrival wins. SQL Server ignores trailing spaces when it compares
  strings, so string cursors that differ only in trailing spaces are equal.
- A record whose primary key is `NULL` or missing fails the sync.
- A string key column is `nvarchar(255)`: a longer key value fails the sync, and so does one ending in
  a space, which SQL Server would treat as the same key as the value without it.
- An index key holds at most 1,700 bytes; a key that could need more (a string field takes 510, so four
  string fields do) fails the sync before anything is written. Object, array and untyped fields cannot
  be keys or cursors.
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

The connector fails the sync rather than drop, null, round or truncate a value. The error names the
stream, the record's position in the stream and the column:

- a record that is not a JSON object or not valid JSON;
- a value that does not fit its column: a non-numeric number, an integer outside `bigint` or
  `decimal(38,0)`, a number `float` cannot hold exactly, an invalid date, a date or time outside
  0001-01-01 to 9999-12-31, more than 7 fractional-second digits, a time zone (`Z`, an offset or a zone
  name) in a `datetime2` or `time` value, a string that is not valid UTF-8 or holds a lone UTF-16
  surrogate (`\ud800`);
- a `NULL` primary key, or a string key value that does not fit, in `append_dedup`;
- a changed column type, a record of a stream that is not in the catalog.

An error raised by the database (a constraint or trigger someone added) names the stream and its table.
On failure the open batch is rolled back, no state is echoed for its records, and the shadow tables of
`overwrite` streams are dropped. Batches committed before the failure stay, and so do the states
echoed for them.

## Limits

- No CDC deletes: a source's delete markers are written as ordinary rows.
- No schema evolution beyond adding nullable columns.
- `overwrite` replaces the table, so permissions granted on the table itself do not carry over (grant
  on the schema instead), and a view bound to it `WITH SCHEMABINDING` makes the swap fail.
- No `_airbyte_meta` column.
- No SSH tunnel, no Windows (NTLM) or Entra ID authentication.
- The stream namespace is ignored; every table goes in the configured schema.
