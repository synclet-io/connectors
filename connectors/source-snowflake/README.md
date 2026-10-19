# source-snowflake

Snowflake source built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk) and the
[gosnowflake](https://github.com/snowflakedb/gosnowflake) driver. It reads the tables, views and
materialized views of one Snowflake database, either as a full refresh or incrementally by a cursor
column you choose.

```bash
task test CONNECTOR=source-snowflake    # needs no account: tests run against a fake Snowflake server
task build CONNECTOR=source-snowflake
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-snowflake:dev check --config /data/config.json
```

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `account` | | Account identifier: `orgname-accountname`, or the account locator with its region (`xy12345.eu-central-1`) |
| `warehouse` | | Warehouse that runs the queries |
| `database` | | Database to read |
| `schemas` | every schema of the database | Schemas to discover and read, written as Snowflake stores them (unquoted names are upper case) |
| `role` | the user's default role | Role of the session |
| `credentials` | | `{"auth_type": "Key Pair Authentication", "username": "...", "private_key": "<PEM>", "private_key_password": "..."}` or `{"auth_type": "username/password", "username": "...", "password": "..."}` |

`private_key` is an RSA key in PEM: PKCS#8 (`BEGIN PRIVATE KEY`), encrypted PKCS#8
(`BEGIN ENCRYPTED PRIVATE KEY`, which needs `private_key_password`), or PKCS#1. Its public key must be set
as the user's `RSA_PUBLIC_KEY`. Snowflake is phasing out password-only logins for service users, so use a
key pair where you can. destination-snowflake takes the same `credentials` block.

## Auth

The role needs:

```sql
GRANT USAGE ON WAREHOUSE <warehouse> TO ROLE <role>;
GRANT USAGE ON DATABASE <database> TO ROLE <role>;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <database> TO ROLE <role>;      -- or each schema
GRANT SELECT ON ALL TABLES IN SCHEMA <database>.<schema> TO ROLE <role>;
GRANT SELECT ON ALL VIEWS IN SCHEMA <database>.<schema> TO ROLE <role>;
```

`check` logs in and runs `SELECT CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_ROLE()`. It lists the
schemas and tables the role can see, then reads one row of the first table of each configured schema
(or of each schema that has a table, when `schemas` is empty). When it fails, it names the cause:

- a wrong username or password;
- a `private_key` that does not match the user's `RSA_PUBLIC_KEY`, or one that does not parse;
- an unknown account (the login is rejected with HTTP 403, or the host does not resolve);
- a role that is not granted to the user;
- a warehouse or database that does not exist or has no USAGE grant for the role;
- a schema that does not exist or has no USAGE grant (the message suggests the upper-case name when
  that one exists);
- a schema with no table the role can read;
- a missing SELECT on a table.

Other server errors are reported as they come.

## Types

Every property is nullable. Each column is selected through an SQL expression that renders it as text,
and that text is decoded into JSON. So both result formats the driver can receive (Arrow and JSON)
produce the same values.

| Snowflake | JSON schema | Value |
|---|---|---|
| `NUMBER(p,0)` (`INT`, `BIGINT`, `NUMBER(38,0)`, ...) | `integer` | exact, every digit kept, also past 2^63 |
| `NUMBER(p,s)` with s > 0, `DECFLOAT` | `number` | exact decimal text, such as `1.50` |
| `FLOAT` | `number` | float64; `NaN` and `±inf` read as `null`, because JSON cannot hold them |
| `TEXT` (`VARCHAR`, `CHAR`, `STRING`) | `string` | |
| `BOOLEAN` | `boolean` | |
| `DATE` | `string`, `format: date` | `2024-02-29` |
| `TIME` | `string`, `airbyte_type: time_without_timezone` | `23:59:59.123456` |
| `TIMESTAMP_NTZ` | `string`, `airbyte_type: timestamp_without_timezone` | `2024-02-29T12:34:56.789` |
| `TIMESTAMP_LTZ`, `TIMESTAMP_TZ` | `string`, `format: date-time`, `airbyte_type: timestamp_with_timezone` | RFC 3339 in UTC: `2024-02-29T10:34:56.789Z` |
| `VARIANT` | any (`{}`) | the JSON value as stored (`TO_JSON`) |
| `OBJECT`, `MAP` | `object` | JSON object |
| `ARRAY` | `array` | JSON array |
| `VECTOR` | `array` of `number` | JSON array |
| `BINARY` | `string` | base64 |
| `GEOGRAPHY`, `GEOMETRY` | `string` | WKT (`ST_ASWKT`): `POINT(30.5 50.45)` |

Fractional seconds keep up to nanoseconds, with trailing zeros dropped. If discover finds a column of
any other type, it fails and names the table, the column and the type. If the server returns text that
does not decode as the column's type (for example a non-integer in a `NUMBER(p,0)` column, or invalid
JSON), the read fails and names the column. A value is never guessed.

## Streams

Discover reads `INFORMATION_SCHEMA.COLUMNS` and `TABLES` of the database. It returns one stream per
table, view and materialized view the role can see in the configured schemas, and uses the schema as
the namespace. A declared primary key (`SHOW PRIMARY KEYS`) becomes the stream's source-defined primary
key. Snowflake does not enforce primary keys.

A configured stream without a namespace is read from the only configured schema. With several schemas
configured, or none, such a read fails with a config error.

## Incremental sync

Pick one top-level column as the cursor (`cursor_field`). It must be of type `NUMBER`, `DECFLOAT`,
`FLOAT`, `TEXT`, `DATE`, `TIME` or `TIMESTAMP_*`. The connector queries:

```sql
SELECT ... FROM "DB"."SCHEMA"."TABLE"
WHERE "CURSOR" >= TO_TIMESTAMP_TZ(?, 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM')   -- the cast fits the cursor type
ORDER BY "CURSOR" ASC NULLS LAST
```

It keeps the greatest cursor in STREAM state, in the text form that it selects the cursor in:

```json
{"cursor_field": ["UPDATED_AT"], "cursor": "2024-01-03T02:00:00.000000000+02:00"}
```

- The next read emits the rows whose cursor is **greater than or equal to** the saved one. Rows that
  share the last cursor value are emitted again, so use a destination sync mode that deduplicates
  (`append_dedup`) when that matters.
- A STATE message follows every 10,000 rows and the end of the stream.
- Rows whose cursor is `NULL` are emitted by the first read only.
- A `FLOAT` cursor that is `NaN`, `inf` or `-inf` fails the read with an error naming the stream,
  the column and the row. Snowflake orders `NaN` above every number, so a saved `NaN` would make each
  later read return only the `NaN` rows; no such value is ever saved as state. Use another cursor
  column, or a view that filters these rows or maps them to `NULL`.
- When the saved state names a different cursor field than the catalog, the read logs a warning and
  starts the stream over.
- Deletes are not seen.

## Limits

- No CDC (streams or change tracking) yet.
- External tables, dynamic-table metadata and the `FILE` type are not read.
- Private-link hosts and OAuth logins are not configurable yet.

## Tests

The tests need no Snowflake account. `go test ./...` (or `-short`) runs every test against a fake
`httptest` server. The fake speaks the part of Snowflake's HTTP protocol that gosnowflake uses: login
(it checks the password, or the RS256 signature of the key-pair JWT), queries with bound values, result
chunks downloaded with the result master key, and session close. It answers in JSON or in Arrow (the
format real accounts send to Go clients), and the read tests run in both. It answers each query only when the SQL text and the bindings match exactly, and it fails
the test on anything else. So the tests pin the queries the connector builds, run the real driver, and
cover type decoding, `check` failure causes, discover, incremental resume and the SDK conformance suite
(`testkit.RunSourceSuite`). Every account, name and value in them is synthetic.

`TestLive` runs against a real account only when `SNOWFLAKE_TEST_ACCOUNT` is set; otherwise it skips.
It also reads `SNOWFLAKE_TEST_USER`, `SNOWFLAKE_TEST_PRIVATE_KEY` (with
`SNOWFLAKE_TEST_PRIVATE_KEY_PASSWORD`) or `SNOWFLAKE_TEST_PASSWORD`, `SNOWFLAKE_TEST_WAREHOUSE`,
`SNOWFLAKE_TEST_DATABASE` and `SNOWFLAKE_TEST_ROLE`. The test:

1. creates a scratch schema and loads into it the same rows the fake serves;
2. checks that discover returns the same schemas and read returns the same records as against the fake;
3. resumes an incremental read from a saved state;
4. runs the conformance suite;
5. drops the schema.
