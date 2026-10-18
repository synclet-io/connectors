# destination-mysql

MySQL destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Writes every stream into one table of the configured database, as overwrite, append or append with
deduplication. Works with MySQL 8.0.19 and later; tested against 8.4. MariaDB is not supported.

```bash
task test CONNECTOR=destination-mysql     # needs Docker; go test -short skips the database tests
task build CONNECTOR=destination-mysql
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-mysql:dev check --config /data/config.json
```

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `host`, `port` | port `3306` | Database server |
| `database` | | Database every table is written to; it must exist |
| `username`, `password` | | Login |
| `ssl_mode` | `{"mode": "preferred"}` | `disabled`, `preferred`, `required`, `verify_ca`, `verify_identity`; see below |

```json
{
  "host": "db.internal",
  "database": "warehouse",
  "username": "synclet_writer",
  "password": "...",
  "ssl_mode": {"mode": "verify_identity", "ca_certificate": "-----BEGIN CERTIFICATE-----\n..."}
}
```

### SSL modes

| Mode | Encryption | Server certificate |
|---|---|---|
| `disabled` | never | |
| `preferred` | when the server supports it | not verified |
| `required` | always | not verified; with `ca_certificate`, its chain is |
| `verify_ca` | always | signed by `ca_certificate`; the host name is not checked |
| `verify_identity` | always | signed by `ca_certificate` and issued for `host` |

`ca_certificate`, `client_certificate` and `client_key` are PEM strings; the client certificate is
optional and sent when the server asks for one.

### Privileges

The user needs `CREATE`, `DROP`, `ALTER`, `INSERT`, `UPDATE` and `SELECT` on the database:

```sql
GRANT CREATE, DROP, ALTER, INSERT, UPDATE, SELECT ON warehouse.* TO 'synclet_writer'@'%';
```

`check` creates a probe table `_synclet_check_<8 hex digits>`, inserts and upserts a row, renames it
and drops it. It names the cause of a failure: wrong user name or password, an unreachable host, a
missing database, no access to the database, or the privilege a step lacks (`user "u" has no CREATE
privilege on database "d"`). When a later step fails, the probe table is dropped if the user may.

Sessions run with `time_zone = '+00:00'`, `utf8mb4` and
`sql_mode = 'STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'`,
so MySQL rejects a value that does not fit instead of storing a truncated or zero value. There is no
SSH tunnel.

## Tables and naming

- One table per stream, named after the stream, in the configured `database`. The stream's namespace is
  ignored, so two streams that map to the same table fail the sync before anything is written.
- Tables are `ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`.
- Table and column names are used as given, in backticks, case and punctuation kept.
- MySQL names hold at most 64 characters. A longer name becomes its first 55 characters, `_` and the
  first 8 hex digits of the SHA-256 of the full name: stable across syncs and unique for names that
  share a long prefix.
- MySQL compares column names, and on some platforms table names, without regard to case, so two
  names that differ only in case fail the sync, naming both. So does an empty name, a name ending in a
  space, or one with a NUL or a character outside the Basic Multilingual Plane (an emoji).
- Every table gets two metadata columns ahead of the stream's properties: `_airbyte_raw_id CHAR(36)`
  (a random UUID per row) and `_airbyte_extracted_at DATETIME(6)` (the record's `emitted_at`, UTC).
- A property that is new in the catalog adds a nullable column. A column whose type differs from the
  one the catalog maps to fails the sync; columns are never altered or dropped.
- Fields of a record that are not in the stream's schema are not written; one warning per stream
  lists them.

## Types

Records are decoded with their numbers as text, so no value passes through a float.

| JSON schema | Column |
|---|---|
| `boolean` | `BOOLEAN` (`TINYINT(1)`); `true`/`false`, also as the strings `"true"`, `"false"`, `"1"`, `"0"` |
| `integer`, `airbyte_type: integer` | `BIGINT`; a value outside the int64 range fails the sync |
| `number` | `DECIMAL(65,30)`; at most 35 integer and 30 fraction digits |
| `airbyte_type: big_integer` | `DECIMAL(65,0)`; at most 65 digits |
| `string` | `LONGTEXT` |
| `string`, format `date` | `DATE`, given as `YYYY-MM-DD` |
| `string`, format `date-time` or `airbyte_type: timestamp_with_timezone` | `DATETIME(6)`, converted to UTC; a value without a zone is taken as UTC |
| `string`, `airbyte_type: timestamp_without_timezone` | `DATETIME(6)` as given; a value with a zone fails |
| `string`, format `time` or `airbyte_type: time_without_timezone` | `TIME(6)`; a value with a zone fails |
| `string`, `airbyte_type: time_with_timezone` | `LONGTEXT`, stored as given (MySQL has no time-with-zone type) |
| `$ref` to Airbyte's `WellKnownTypes.json` | the type of that definition |
| `object`, `array`, more than one non-null type, `anyOf`/`oneOf`, no type | `JSON` |

- A JSON `null` or a missing field lands as `NULL`. Numbers given as JSON strings (`"12.5"`) are
  accepted in `BIGINT` and `DECIMAL` columns.
- Numbers in exponent form (`1e3`) are written as plain digits. A number with more digits than its
  `DECIMAL` column holds fails the sync rather than being rounded; trailing fraction zeros and leading
  zeros do not count.
- Date-times are `YYYY-MM-DD[THH:MM[:SS[.f]]]` with an optional `Z` or `±HH[:MM]` offset. More than
  6 non-zero fractional-second digits fail the sync, since MySQL would round them.
- MySQL keeps a number inside a `JSON` value as a 64-bit integer when it is one and as a double
  otherwise. A number that a double cannot hold exactly as written (`0.912345678901234567890`)
  fails the sync; `0.1` and `1.10` are fine, since their shortest double reads back the same.

## Sync modes

Records are loaded in batches. A batch is one transaction of multi-row `INSERT` statements of up to
1,000 rows or 4 MiB of record data each.

| Mode | Table |
|---|---|
| `overwrite` | The sync writes into `_synclet_tmp_<table>`. After the last batch one `RENAME TABLE <table> TO _synclet_old_<table>, _synclet_tmp_<table> TO <table>` (atomic, over every overwrite stream) swaps the tables and `_synclet_old_<table>` is dropped, so readers see the old rows until the sync succeeds. A failed sync drops `_synclet_tmp_<table>` and leaves the old table as it was |
| `append` | `INSERT` in arrival order |
| `append_dedup` | The table has a `PRIMARY KEY` on the stream's primary key. Rows are inserted in arrival order with `INSERT ... AS new ON DUPLICATE KEY UPDATE c = IF(cursor IS NULL OR new.cursor >= cursor, new.c, c)`, the cursor column assigned last, so a row with a lower cursor arriving later does not replace the stored one |

Deduplication rules:

- Without a cursor field the last arrival wins.
- Among equal cursors the last arrival wins; a stored `NULL` cursor is replaced, an incoming `NULL`
  cursor does not replace a stored one.
- A record whose primary key is `NULL` or missing fails the sync.
- String key columns are `VARCHAR(255)` (binary collation, so `A` and `a` are different keys). A key
  value over 255 characters fails the sync, and so does one ending in a space, which the collation
  would ignore when it compares keys. At most 3 string key fields fit a MySQL key; more fail the sync.
  An object, array or untyped key or cursor field fails the sync.
- An existing table without a primary key on exactly the stream's key fails the sync.
- Primary key and cursor must be top-level properties.

## Batching and state

- A batch is committed at 10,000 rows or 32 MiB of record data, on a STATE when the last commit is 30
  seconds old or older, and at the end of the input.
- A STATE is echoed only after every record received before it is committed. A STATE that arrives
  with no record pending is echoed at once.
- The STREAM states of an `overwrite` stream, and GLOBAL and LEGACY states while the catalog has an
  `overwrite` stream, are held until the final swap: before it, the records are not in the table
  readers see.

## Failures

The connector fails the sync rather than drop, null, round or truncate a value. The error names the
stream, the record's position in the stream and the column:

- a record that is not a JSON object or not valid JSON;
- a value that does not fit its column: a non-numeric number, an integer outside `BIGINT`, a number
  with too many digits, a date MySQL rejects (`2024-02-30`), a malformed date-time, a time zone in a
  `timestamp_without_timezone` or `time` value, a string that is not valid UTF-8 or holds a lone
  UTF-16 surrogate (`\ud800`), a JSON number a double cannot hold;
- a `NULL` primary key, or a string key value that does not fit, in `append_dedup`;
- a changed column type, a record of a stream that is not in the catalog.

On failure the open batch is rolled back, no state is echoed for its records, and the shadow tables of
`overwrite` streams are dropped. Batches committed before the failure stay, and so do the states
echoed for them.

## Limits

- MySQL 8.0.19 or later (row aliases in `ON DUPLICATE KEY UPDATE`); MariaDB is not supported.
- No `LOAD DATA LOCAL INFILE`: `LOCAL` turns data errors into warnings, and servers default to
  `local_infile=OFF`.
- No CDC deletes: a source's delete markers are written as ordinary rows.
- No schema evolution beyond adding nullable columns.
- No SSH tunnel.
- The stream namespace is ignored; every table goes in the configured database.
