# destination-redshift

Amazon Redshift destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
It stages each batch as a gzipped CSV file in S3 and loads it with `COPY` into one typed table per
stream, in overwrite, append or append-with-deduplication mode. It works with provisioned clusters
and Serverless workgroups. It uses [pgx](https://github.com/jackc/pgx) in simple-protocol mode and
[aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2).

```bash
task test CONNECTOR=destination-redshift     # needs Docker; go test -short skips the MinIO and Postgres tests
task build CONNECTOR=destination-redshift
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-redshift:dev check --config /data/config.json
```

## Tests

The offline tests need no AWS or Redshift credentials:

- **Statement recorder.** The SQL is asserted exactly as generated: DDL, `COPY`, `INSERT`, the dedup
  `MERGE`, the swap and the transaction statements. A recorder stands in for the cluster. It logs
  every statement, answers catalog queries and fails the statements a test names.
- **MinIO in Docker.** Batches are uploaded to MinIO, and at each `COPY` the test reads back the
  staged CSV it names.
- **Postgres 16 in Docker.** It answers only what Postgres does the same way as Redshift: logins,
  unreachable hosts and the `has_*_privilege` checks of `check`. The probe `COPY` goes to the
  recorder.

Several parts of Redshift have no local equivalent and are covered only by the recorded SQL: `SUPER`,
`MERGE`, `COPY FROM 's3://…'`, VARCHAR byte limits, `sys_load_error_detail`, and the cluster
assuming an IAM role. `TestLiveRedshift` checks them against a real cluster. It runs only when
`REDSHIFT_TEST_HOST` is set and is skipped otherwise. It reads these variables:

| Variable | Meaning |
|---|---|
| `REDSHIFT_TEST_HOST`, `REDSHIFT_TEST_PORT` | Endpoint of the cluster or workgroup (port 5439 by default) |
| `REDSHIFT_TEST_DATABASE`, `REDSHIFT_TEST_USER`, `REDSHIFT_TEST_PASSWORD` | Database login |
| `REDSHIFT_TEST_S3_BUCKET`, `REDSHIFT_TEST_S3_REGION` | Staging bucket; the upload uses the AWS default credential chain |
| `REDSHIFT_TEST_IAM_ROLE` | Role `COPY` reads the bucket with; empty passes the upload credentials to `COPY` |

## Configuration

The keys follow Airbyte's destination-redshift.

| Key | Default | Meaning |
|---|---|---|
| `host` | | Endpoint of the cluster or workgroup, without the port |
| `port` | `5439` | |
| `database` | | |
| `schema` | `public` | Schema every table is written to; it is created when missing |
| `username`, `password` | | |
| `ssl_mode.mode` | `require` | `require` (encrypted, certificate not verified), `verify-full` (system roots and host name), `disable` (local tests only) |
| `uploading_method.method` | `S3 Staging` | The only method |
| `uploading_method.s3_bucket_name` | | Staging bucket |
| `uploading_method.s3_bucket_path` | | Key prefix; files go to `<path>/<sync id>/<table>/<n>.csv.gz` |
| `uploading_method.s3_bucket_region` | `us-east-1` | Region of the bucket, passed to `COPY` as `REGION` |
| `uploading_method.access_key_id`, `secret_access_key` | | Upload keys; empty uses the AWS default credential chain |
| `uploading_method.role_arn`, `external_id` | | Role assumed on top of the keys or the chain for the upload |
| `uploading_method.iam_role_arn` | | Role attached to the cluster that `COPY` uses (`IAM_ROLE`); a comma-separated list chains roles |
| `uploading_method.purge_staging_data` | `true` | Delete the staged files after their batch is committed |

`COPY` authorization:

- When `iam_role_arn` is set, `COPY` runs with `IAM_ROLE '<arn>'`.
- Otherwise `COPY` gets the upload credentials as `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY`. When
  `role_arn` is set, these are the role's temporary credentials with a `SESSION_TOKEN`, fetched again
  for every `COPY`.

So an assumed role works for both the upload and the load (Airbyte
[#33039](https://github.com/airbytehq/airbyte/issues/33039)).

`check` tests each part of the setup in turn:

- It logs in.
- It verifies `CREATE` on the schema, or on the database when the schema is missing, and the `TEMP`
  privilege.
- It assumes `role_arn`.
- It uploads a probe file, loads it with `COPY` into a temporary table, then deletes it.

Each failure names its cause:

| Cause | Message contains |
|---|---|
| Wrong password | `cannot log in as "<user>": wrong user name or password` |
| Unreachable cluster | `cannot reach <host>:<port>` |
| No CREATE privilege | `user "<user>" has no CREATE privilege on schema "<schema>"` |
| Bucket not writable | `S3 staging bucket is not writable: no write permission on s3://…` |
| `role_arn` not assumable | `cannot assume IAM role <arn>` |
| `iam_role_arn` not assumable by the cluster | `the Redshift cluster cannot assume IAM role <arn>` |
| `COPY` cannot read the bucket | `COPY cannot read s3://… with the configured authorization` |

## Tables and naming

Every stream goes to one table in the configured schema, and the stream namespace is ignored. Each
table has these columns, then the schema's properties sorted by name:

| Column | Type |
|---|---|
| `_airbyte_raw_id` | `VARCHAR(36)` (UUID) |
| `_airbyte_extracted_at` | `TIMESTAMPTZ` |
| `_airbyte_meta` | `SUPER` (`{"changes":[…]}`) |

Redshift folds the ASCII letters of identifiers to lower case, quoted or not, and so does the
connector. A name over 127 bytes becomes its first 118 bytes, `_`, and 8 hex digits of its SHA-256.
Two names that map to the same identifier, such as `Id` and `id`, fail the sync before anything is
written.

## Types

| JSON schema | Redshift |
|---|---|
| `boolean` | `BOOLEAN` |
| `integer`, `number` with `airbyte_type: integer` | `BIGINT` |
| `number` | `DOUBLE PRECISION` |
| `string` with `airbyte_type: big_integer` | `NUMERIC(38,0)` |
| `string` | `VARCHAR(65535)` |
| `string` `format: date` | `DATE` |
| `string` `format: date-time`, `airbyte_type: timestamp_with_timezone` | `TIMESTAMPTZ` |
| `airbyte_type: timestamp_without_timezone` | `TIMESTAMP` |
| `string` `format: time`, `airbyte_type: time_without_timezone` | `TIME` |
| `airbyte_type: time_with_timezone` | `TIMETZ` |
| `object`, `array`, a union of types, no type | `SUPER` |

Values are put in canonical form before staging, so `COPY` never has to round, cut or guess:

- **Exact number rule.** A `number` lands in `DOUBLE PRECISION` and is accepted only when it holds
  the value exactly as written: the nearest float64 must print back as the same decimal value.
  `41.25`, `0.30000000000000004` and `1e-30` pass. `0.912345678901234567890` fails the sync, with an error that names the stream, the record's
  position in the stream and the column. `DECIMAL(38,9)` is not used instead, since it would round.
- `BIGINT` and `NUMERIC(38,0)` take a number that is an exact integer in any notation, such as
  `1.0` or `1e3`. A fraction fails, and so does a value outside the `BIGINT` range or with more than
  38 digits for `NUMERIC(38,0)`.
- Timestamps and times are parsed in Go.
  - Up to 6 fractional digits are kept, and a non-zero 7th or later digit fails.
  - A `TIMESTAMPTZ` is converted to UTC. A value without a zone is read as UTC.
  - A zone in a `TIMESTAMP` or `TIME` value fails.
- `SUPER` values are compacted JSON. Numbers keep the text the source wrote.

### Strings over 65535 bytes

`VARCHAR(65535)` is Redshift's largest string column (Airbyte
[#14441](https://github.com/airbytehq/airbyte/issues/14441)), and a string inside a `SUPER` value
holds at most 65535 bytes too ([SUPER limitations](https://docs.aws.amazon.com/redshift/latest/dg/limitations-super.html)).
`COPY`'s `TRUNCATECOLUMNS` does not apply to `SUPER`. A longer string, in a `VARCHAR` column or
anywhere in a `SUPER` value, does not fail the load:

- It is cut to the longest UTF-8 prefix of at most 65535 bytes.
- The record's `_airbyte_meta` gets
  `{"field":"<path>","change":"TRUNCATED","reason":"DESTINATION_FIELD_SIZE_LIMITATION"}` in
  `changes`. The path is the column name, followed by `.key` and `[index]` steps for a string
  inside a `SUPER` value, as in `payload.items[0].body`.
- The sync logs one warning per stream with the count and the paths.

To find the truncated rows, use `WHERE _airbyte_meta.changes[0] IS NOT NULL`.

## Sync modes

Each batch is uploaded, then loaded in one transaction:
`CREATE TEMP TABLE "_synclet_stage_<n>"`, then `COPY … FORMAT AS CSV GZIP`, then the mode's statement
below.

- **overwrite** writes to a shadow table, `_synclet_tmp_<table>`, created fresh at the start. After
  the last batch, one transaction drops the old table and renames the shadow table into its place.
  The old table stays until the sync succeeds. A failed sync drops the shadow table.
- **append** creates the table when it is missing, adds a nullable column for each new schema
  property, and inserts the batch with `INSERT … SELECT`.
- **append_dedup** creates the table with an informational `PRIMARY KEY`, then merges each batch:

  ```sql
  CREATE TEMP TABLE "_synclet_dedup_<n>" AS SELECT <cols> FROM (
    SELECT …, ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <cursor> DESC NULLS LAST, "_synclet_seq" DESC) AS "_synclet_rn"
    FROM "_synclet_stage_<n>") AS src
  WHERE src."_synclet_rn" = 1 AND NOT EXISTS (SELECT 1 FROM <table> AS tgt
    WHERE <pk match> AND tgt.<cursor> IS NOT NULL AND (src.<cursor> IS NULL OR src.<cursor> < tgt.<cursor>));
  MERGE INTO <table> USING "_synclet_dedup_<n>" AS src ON <pk match>
    WHEN MATCHED THEN UPDATE SET … WHEN NOT MATCHED THEN INSERT … VALUES …;
  ```

  - **One row per key.** The batch first keeps one row per key: the one with the highest cursor, or
    the one that arrived last when cursors are equal. It also drops rows whose cursor is lower than
    the stored row's. Redshift's `MERGE` fails when two source rows match one target row, and it has
    no `WHEN MATCHED AND …`.
  - **No cursor.** The latest record wins.
  - **Key and cursor fields.** They must be top-level scalar fields. A `SUPER` field cannot be
    compared, and a `NULL` key fails the sync.

## Batching and state

- **Commit points.** A batch is committed at 100,000 rows or 64 MiB of record data, on a STATE when
  the last commit is 60 seconds old or older, and at the end of the input.
- **Echo after commit.** A STATE is echoed only after every record received before it is committed.
  A STATE that arrives with no record pending is echoed at once.
- **Held states.** The STREAM states of an `overwrite` stream are held until the swap commits. So
  are GLOBAL and LEGACY states while the catalog has an `overwrite` stream.
- **Staged files.** They are deleted after their batch commits (`purge_staging_data`), or right away
  when the batch fails. If the delete fails after the commit, it is only a warning, because the rows
  are already loaded.

## Failures

The connector fails the sync rather than drop, null, round or silently truncate a value. The error
names the stream, the record's position in the stream and the column. These cause it:

- a record that is not a JSON object or not valid JSON;
- a value that does not fit its column as written, as listed under Types;
- `\u0000` in a `VARCHAR` value;
- a string that is not valid UTF-8 or holds a lone UTF-16 surrogate;
- a value equal to the staging NULL marker `__synclet_null__`;
- a `SUPER` value over 16 MB, the largest Redshift holds (`SUPER value is larger than 16 MB`);
- a CSV row over 4 MiB, the largest row `COPY` loads;
- a `NULL` primary key in `append_dedup`;
- a changed column type;
- a record of a stream that is not in the catalog.

When `COPY` itself rejects a row, the connector rolls back and reads `sys_load_error_detail`. It then
reports the record whose CSV row spans the line Redshift names, counting the lines of quoted values
that hold a newline, and the column.

On failure:

- the open batch is rolled back;
- no state is echoed for its records;
- the batch's staged files are deleted;
- the shadow tables of `overwrite` streams are dropped.

Batches committed before the failure stay, and so do the states already echoed for them.

## Limits

- **CDC deletes.** None: a source's delete markers are written as ordinary rows.
- **Schema evolution.** Only new nullable columns are added.
- **Connections.** No SSH tunnel, and no IAM database authentication. The login is a user and a
  password.
- **`SUPER` and `DOUBLE PRECISION` precision.** Numbers inside a `SUPER` value follow Redshift's JSON
  parsing. `DOUBLE PRECISION` columns hold float64 values. Exact decimals with more significant digits
  fail, and do not round.
