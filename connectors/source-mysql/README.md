# source-mysql

MySQL source built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Reads the tables and views of one database, as full refresh, incremental by a cursor column you
choose, or CDC from the binlog. Tested against MySQL 8.0; MariaDB is not tested.

```bash
task test CONNECTOR=source-mysql     # needs Docker; go test -short skips the database tests
task build CONNECTOR=source-mysql
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-mysql:dev check --config /data/config.json
```

## Configuration

The keys match Airbyte's source-mysql, so an existing Airbyte configuration carries over.

| Key | Default | Meaning |
|---|---|---|
| `host`, `port` | port `3306` | Database server |
| `database` | | Database to discover and read; streams carry it as namespace |
| `username`, `password` | | Login |
| `ssl_mode` | `{"mode": "preferred"}` | See [SSL modes](#ssl-modes) |
| `tunnel_method` | `{"tunnel_method": "NO_TUNNEL"}` | `SSH_KEY_AUTH` (`ssh_key`) or `SSH_PASSWORD_AUTH` (`tunnel_user_password`), both with `tunnel_host`, `tunnel_port` (22), `tunnel_user` and optionally `tunnel_host_key` |
| `replication_method` | `{"method": "STANDARD"}` | `STANDARD` reads with SELECT queries; `CDC` reads the binlog, with `server_id`, `checkpoint_seconds` (300) and `initial_waiting_seconds` (accepted, unused: the binlog is read until the sync is stopped) |
| `table_filter.table_patterns` | all tables | SQL `LIKE` patterns of the tables discover lists; `table_filter.schema_patterns` is accepted and has no effect, since one database is read |
| `zero_date_handling` | `"null"` | See [Zero dates](#zero-dates) |

```json
{
  "host": "db.internal",
  "database": "app",
  "username": "synclet_reader",
  "password": "...",
  "ssl_mode": {"mode": "verify_identity", "ca_certificate": "-----BEGIN CERTIFICATE-----\n..."},
  "tunnel_method": {"tunnel_method": "SSH_KEY_AUTH", "tunnel_host": "bastion.example.com", "tunnel_user": "synclet", "ssh_key": "-----BEGIN OPENSSH PRIVATE KEY-----\n..."}
}
```

Configurations of earlier versions keep working: these keys are read when the Airbyte key is absent.

| Earlier key | Read as |
|---|---|
| `ssl` | `ssl_mode` |
| `tunnel.method` (`no_tunnel`, `ssh_key`, `ssh_password`), `tunnel.host`, `tunnel.port`, `tunnel.user`, `tunnel.ssh_key`, `tunnel.password` | `tunnel_method.tunnel_method` (`NO_TUNNEL`, `SSH_KEY_AUTH`, `SSH_PASSWORD_AUTH`), `tunnel_host`, `tunnel_port`, `tunnel_user`, `ssh_key`, `tunnel_user_password` |
| `tunnel_method.method`, `tunnel_method.tunnel_password` | `tunnel_method.tunnel_method`, `tunnel_user_password` |
| `replication.method`, `replication.cdc.server_id`, `replication.cdc.initial_wait_seconds`, `replication.cdc.checkpoint_seconds` | `replication_method.method`, `server_id`, `initial_waiting_seconds`, `checkpoint_seconds` |
| `replication_method.initial_wait_seconds` | `replication_method.initial_waiting_seconds` |

Through an SSH tunnel the database host is resolved and dialed by the jump server, so `host` can be a
name only the jump server knows. Set `tunnel_host_key` to the jump server's public host keys, one per
line, each in authorized_keys format (`ssh-ed25519 AAAA...`) or as a known_hosts line; the whole output
of `ssh-keyscan bastion.example.com` works. The tunnel is refused when the server presents none of the
pinned keys. Without `tunnel_host_key` the host key is not verified.

## Permissions and check

The user needs a privilege on the database and `SELECT` on the tables to read; discover lists the
tables and views the user has any privilege on. CDC also needs `REPLICATION CLIENT` and
`REPLICATION SLAVE`. `check` connects, runs `SELECT 1` and probes `SELECT` on every table discover
lists, and names the cause of a failure:

| Cause | Message |
|---|---|
| wrong user name or password | `cannot log in as "<user>": wrong user name or password` |
| missing database | `database "<db>" does not exist` |
| no privilege on the database | `user "<user>" has no privilege on database "<db>"` |
| a table without `SELECT` | `user "<user>" may not SELECT from table <db>.<table>` |
| unreachable host | `cannot reach <host>:<port>: ...` |
| CDC without `REPLICATION CLIENT` | `user "<user>" lacks the REPLICATION CLIENT privilege CDC needs` |
| CDC with binlog settings off | `CDC needs log_bin=ON, the server has log_bin=OFF` (also `binlog_format=ROW`, `binlog_row_image=FULL`) |

## SSL modes

Certificates are PEM strings in the configuration and are never written to disk.

| Mode | Encryption | Server certificate |
|---|---|---|
| `preferred` | when the server supports it | not verified |
| `required` | always | not verified |
| `verify_ca` | always | chain checked against `ca_certificate` |
| `verify_identity` | always | chain and host name checked |

`client_certificate` and `client_key` add a client certificate, for users created `REQUIRE X509`.
`preferred` refuses a `ca_certificate`: it may connect without encryption, so the CA would protect
nothing.

## Types

Every query runs on MySQL's text protocol, so the connector renders each value from the text MySQL
sends: numbers keep every digit. The session runs with `utf8mb4` and time zone `+00:00`. Every property
is nullable.

| MySQL | JSON schema | Value |
|---|---|---|
| `tinyint` (also `tinyint(1)`), `smallint`, `mediumint`, `int`, `bigint`, `year` | `integer` | exact; `bigint unsigned` up to `18446744073709551615` |
| `decimal`, `numeric` | `number` | exact digits with the column's scale, `0.10` |
| `ZEROFILL` numbers | as their type | without the padding: `00042` reads as `42` |
| `float`, `double` | `number` | MySQL's text form |
| `bit(1)` | `boolean` | `true` |
| `bit(n)`, n > 1 | `integer` | the bits as a big-endian unsigned number |
| `char`, `varchar`, `*text`, `enum`, `set` | `string` | as stored, utf8mb4 (emoji intact); `set` as `a,c` |
| `json` | any (`{}`) | the document, as MySQL normalizes it |
| `binary`, `varbinary`, `*blob` | `string` | base64 |
| `date` | `string`, format `date` | `2024-02-29` |
| `datetime` | `string`, airbyte_type `timestamp_without_timezone` | `2024-02-29T13:14:15.123456`, with the column's fraction digits |
| `timestamp` | `string`, format `date-time`, airbyte_type `timestamp_with_timezone` | `2024-02-29T11:14:15.120Z` (UTC) |
| `time` | `string` | as MySQL renders it, also beyond 24 hours and negative: `838:59:59`, `-01:02:03` |
| `geometry`, `point`, `linestring`, `polygon`, `multi*`, `geomcollection` | `string` | WKT (`ST_AsText`), `POINT(1 2)` |
| anything else | `string` | MySQL's text form |

## Zero dates

MySQL can store dates whose month or day is zero (`0000-00-00`, `2024-00-15`,
`2024-03-00 10:00:00`) when `sql_mode` allows them. They fit neither the `date` format nor a
timestamp, so `zero_date_handling` decides what they read as:

- `"null"` (default): the value reads as `null`.
- `"fail"`: the read stops with a config error naming the stream, the column, the value and the
  row's primary key, such as `stream app.orders: column "shipped" holds the zero date "0000-00-00" in
  the row with id=2`.

A date with year `0000` and a valid month and day, such as `0000-01-01`, reads as is.

## Streams

A configured stream names its table by name; its namespace, when set, must be the configured
database. Identifiers are quoted with backticks in every query, so table and column names with dots,
spaces or backticks work.

## Incremental sync

Pick any column as the cursor (`cursor_field`, one column). The connector reads rows in cursor order
in one streaming query and keeps the greatest cursor, as MySQL renders it, in STREAM state:

```json
{"cursor_field": ["updated_at"], "cursor": "2024-01-05 00:00:00.000000"}
```

- A `binary`, `varbinary` or `blob` cursor is saved base64-encoded and a `bit` cursor as an unsigned
  integer, so the saved value is the exact one MySQL compares against.
- The next read emits rows whose cursor is **greater than or equal to** the saved one, so rows that
  share the last cursor value are emitted again; use a destination sync mode that deduplicates
  (`append_dedup`) when that matters.
- A STATE message follows every 10,000 rows and the end of the stream.
- Rows whose cursor is `NULL` are emitted by the first read only.
- When the saved state names a different cursor field than the catalog, the read logs a warning and
  starts the stream over.
- A state saved by version 0.2 (`{"phase": "incremental", "cursor_field": "updated_at",
  "cursor_value": ...}`) resumes from its `cursor_value`; one saved during the 0.2 snapshot phase
  starts the stream over with a warning.
- 0.2 saved a binary or `bit` cursor as text, which replaced bytes that are not valid UTF-8 with
  U+FFFD. Such a cursor cannot be restored: the read fails with a config error naming the stream and
  asking to reset its state, instead of resuming from a guessed value. A cursor whose bytes survived
  resumes from them.
- A full refresh reads the table in one streaming query and saves no state.

## CDC

With `replication_method.method` `CDC` every stream is read from the binlog: first a snapshot of
each table, in primary key chunks of 10,000 rows (in one query for a table without a primary key),
then inserts, updates and deletes as they happen, until the sync is stopped. The state is a GLOBAL
state, the binlog position shared by all streams; a CDC state saved by 0.1 is still read. Records carry
`_ab_cdc_updated_at`, `_ab_cdc_deleted_at`, `_ab_cdc_log_file` and `_ab_cdc_log_pos`. The server needs
`log_bin=ON`, `binlog_format=ROW` and `binlog_row_image=FULL`.

Binlog values read as the same JSON as a SELECT reads them, with one exception: geometry arrives as
MySQL's internal value (SRID and WKB) and is emitted base64-encoded, not as WKT.

Limits: the binlog reader connects to `host` and `port` directly, without the SSH tunnel and without
TLS, whatever `tunnel_method` and `ssl_mode` say.
