# destination-snowflake

Snowflake destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk) and
[gosnowflake](https://github.com/snowflakedb/gosnowflake) (Apache-2.0). Writes every stream into one
table of the configured schema, as overwrite, append or append with deduplication. Batches are staged
with `PUT` to the user stage and loaded with `COPY INTO`.

```bash
task test CONNECTOR=destination-snowflake     # offline: a fake Snowflake server, no credentials
task build CONNECTOR=destination-snowflake
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-snowflake:dev check --config /data/config.json
```

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `account` | | Account identifier `orgname-accountname`, or the account locator with its region |
| `warehouse` | | Warehouse that runs the loads |
| `database` | | Database the tables are written to |
| `schema` | `PUBLIC` | Schema every table is written to, in upper case; created when missing |
| `role` | the user's default role | Role of the session |
| `credentials` | | `{"auth_type": "Key Pair Authentication", "username", "private_key", "private_key_password"}` |

Only key-pair (JWT) login is supported. `private_key` is an RSA key in PEM: PKCS#8, encrypted PKCS#8
(with `private_key_password`) or PKCS#1. The user's `RSA_PUBLIC_KEY` must be its public key.

```json
{
  "account": "myorg-myaccount",
  "warehouse": "LOADING_WH",
  "database": "RAW",
  "schema": "SYNCLET",
  "role": "SYNCLET_ROLE",
  "credentials": {
    "auth_type": "Key Pair Authentication",
    "username": "SYNCLET_LOADER",
    "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
  }
}
```

The role needs USAGE on the warehouse and the database, CREATE SCHEMA on the database when the schema
is missing, and CREATE TABLE on the schema. On a table it did not create it needs INSERT and SELECT
for `append`, UPDATE as well for `append_dedup`, and OWNERSHIP for `overwrite`, which swaps the table.
Batch files go to the user's own stage `@~`, which every user can write to without a grant.

`check` logs in, creates the schema when it is missing, and creates, writes and drops a table. A failure
names the cause: a key that does not match the user (or no such user), an unknown account, a role not
granted, a missing or unusable warehouse or database, or a missing CREATE privilege.

## Tables

- Schema, table and column names are the stream and property names in upper case, always quoted, so
  `select id from users` finds them. Two streams, or two properties of a stream, that map to the same
  name fail the sync. Names longer than 255 characters fail. The stream namespace is ignored.
- Each table has `_AIRBYTE_RAW_ID VARCHAR` (random per row) and `_AIRBYTE_EXTRACTED_AT TIMESTAMP_TZ`
  (the record's `emitted_at`).
- A property that is new in the catalog adds a column. In `append` and `append_dedup` a column whose
  type differs from the one the catalog maps to fails the sync; columns are never altered or dropped.
  An `overwrite` table is replaced with the catalog's columns.
- Fields of a record that are not in the stream's schema are not written; one warning per stream lists
  them.

## Types

| JSON schema | Column |
|---|---|
| `boolean` | `BOOLEAN` |
| `integer`, `airbyte_type: integer` or `big_integer` | `NUMBER(38,0)` |
| `number` | `FLOAT` (a double, as in Airbyte's Snowflake destination) |
| `string` | `VARCHAR` |
| `string`, format `date` | `DATE` |
| `string`, format `date-time` or `airbyte_type: timestamp_with_timezone` | `TIMESTAMP_TZ` |
| `string`, `airbyte_type: timestamp_without_timezone` | `TIMESTAMP_NTZ` |
| `string`, format `time` or `airbyte_type: time_without_timezone` | `TIME` |
| `string`, `airbyte_type: time_with_timezone` | `VARCHAR` (Snowflake has no time with zone) |
| `$ref` to Airbyte's `WellKnownTypes.json` | the type of that definition |
| `object`, `array`, more than one non-null type, `anyOf`/`oneOf`, no type | `VARIANT` |

Values keep their exact value: an integer is never parsed into a float, a `number` is written only
when the double nearest to it is the same number (`0.30000000000000004`, `37.77492912345`, `1e-10`
and `1e29` are; `9007199254740993` is not), and timestamps keep their own offset.
A JSON `null` or a missing field is `NULL`. A `TIMESTAMP_TZ` value without an offset is UTC.

## Sync modes

Each batch is rendered as gzip CSV, `PUT` to the run's path of the user stage, `@~/synclet/<run id>/`,
and loaded with `COPY INTO` into a temporary staging table of text columns. `COPY` must report every row of the batch loaded. One
transaction then casts the staged text into the target table and empties the staging table. At the end
of the sync, failed or not, `REMOVE @~/synclet/<run id>/` deletes the run's files; a failed `REMOVE`
is a warning, not a failed sync.

| Mode | Table |
|---|---|
| `overwrite` | The sync writes into `_SYNCLET_TMP_<TABLE>`, created `LIKE` the existing table `COPY GRANTS` (then altered to the catalog's columns) so the swapped-in table keeps the grants readers have. After the last batch `ALTER TABLE ... SWAP WITH` replaces the table and the old rows are dropped, so readers see the old rows until the sync succeeds. A failed sync drops `_SYNCLET_TMP_<TABLE>` and leaves the table as it was. Several overwrite streams are swapped one after the other, each atomically |
| `append` | `INSERT INTO <table> SELECT ... FROM <staging>` in arrival order |
| `append_dedup` | `MERGE` on the primary key. Each batch is reduced to one row per key, the one with the highest cursor (`QUALIFY ROW_NUMBER() OVER (PARTITION BY pk ORDER BY cursor DESC NULLS LAST, arrival DESC) = 1`); a stored row is replaced only when the incoming cursor is not lower. Without a cursor the last arrival wins. The table gets an informational `PRIMARY KEY` |

## Batching and state

- A batch is committed at 100,000 rows or 64 MiB of record data, on a STATE when the last commit is a
  minute old or older, and at the end of the input.
- A STATE is echoed only after every record received before it is committed. A STATE that arrives
  with no record pending is echoed at once.
- The STREAM states of an `overwrite` stream, and GLOBAL and LEGACY states while the catalog has an
  `overwrite` stream, are held until the swaps: before them, the records are not in the table readers
  see.

## Failures

The connector fails the sync rather than drop, round or truncate a value. The error names the stream,
the record's position in the stream and the column:

- a record that is not a JSON object;
- an integer with a fraction or more than 38 digits; a `number` that a double cannot hold exactly or
  that is outside its range;
- a date, time or timestamp in another format than ISO 8601, or one that does not exist; a time zone in
  a `TIME` or `TIMESTAMP_NTZ` value, which the column would drop;
- a number nested in a `VARIANT` that `PARSE_JSON` would store as a double and so change: more than 38
  significant digits, more than 15 in exponent form, or more than 37 digits after the decimal point;
- a string that is not valid UTF-8 or holds a lone UTF-16 surrogate; a value over 16 MiB;
- a `NULL` primary key in `append_dedup`; a changed column type; a record of a stream not in the
  catalog; a `COPY` that loads fewer rows than the batch has.

On failure the open batch is rolled back, no state is echoed for its records, and the shadow tables of
`overwrite` streams are dropped. Batches committed before the failure stay, and so do the states echoed
for them.

## Tests

`go test ./...` runs offline against a fake Snowflake server (`fake_test.go`) that speaks the driver's
HTTP protocol: login with JWT, queries, `PUT` to a local stage and `COPY INTO`, which reads the staged
file back. `TestLive` runs against a real account when `SNOWFLAKE_TEST_ACCOUNT` is set, with
`SNOWFLAKE_TEST_USER`, `SNOWFLAKE_TEST_PRIVATE_KEY` (and `_PRIVATE_KEY_PASSWORD`), `_WAREHOUSE`,
`_DATABASE` and `_ROLE`; it writes into a scratch schema and drops it. Without the variable it is skipped.

## Live checks

The fake server does not run Snowflake's SQL, so these need a check against a real account
(`TestLive`, or by hand):

- `PUT` to `@~/synclet/<run id>/` by a role that owns none of the tables, and `REMOVE` of that path;
- the `COPY INTO ... FROM @~/synclet/<run id>/ FILES = (...)` result columns (`file`, `status`,
  `rows_loaded`) and `PURGE = TRUE`;
- after an `overwrite` of an existing table, a role granted SELECT on it can still read it
  (`CREATE TABLE ... LIKE ... COPY GRANTS` then `SWAP WITH`);
- the cast formats: `TO_TIMESTAMP_TZ(..., 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')`,
  `TO_TIMESTAMP_NTZ`, `TO_TIME`, `TO_DATE`, `TO_NUMBER(..., 38, 0)` and `TO_DOUBLE` of the text
  `encodeValue` writes.

## Limits

- No CDC deletes: a source's delete markers are written as ordinary rows.
- No password or OAuth login; no Snowpipe Streaming.
- No schema evolution beyond adding columns.
