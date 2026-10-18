# source-postgres

Postgres source built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Reads tables, partitioned tables, views, materialized views and foreign tables, as full refresh or
incremental by a cursor column you choose. Works with Postgres 12 and later; tested against 16.

```bash
task test CONNECTOR=source-postgres     # needs Docker; go test -short skips the database tests
task build CONNECTOR=source-postgres
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-postgres:dev check --config /data/config.json
```

## Configuration

The keys match Airbyte's source-postgres, so an existing configuration carries over.

| Key | Default | Meaning |
|---|---|---|
| `host`, `port` | port `5432` | Database server |
| `database` | | Database name |
| `schemas` | `["public"]` | Schemas to discover and read |
| `username`, `password` | | Login |
| `ssl_mode` | `{"mode": "prefer"}` | See [SSL modes](#ssl-modes) |
| `tunnel_method` | `{"tunnel_method": "NO_TUNNEL"}` | `SSH_KEY_AUTH` (`ssh_key`) or `SSH_PASSWORD_AUTH` (`tunnel_user_password`), both with `tunnel_host`, `tunnel_port` (22), `tunnel_user` and optionally `tunnel_host_key` |

```json
{
  "host": "db.internal",
  "database": "app",
  "username": "synclet_reader",
  "password": "...",
  "schemas": ["public", "billing"],
  "ssl_mode": {"mode": "verify-full", "ca_certificate": "-----BEGIN CERTIFICATE-----\n..."},
  "tunnel_method": {"tunnel_method": "SSH_KEY_AUTH", "tunnel_host": "bastion.example.com", "tunnel_user": "synclet", "ssh_key": "-----BEGIN OPENSSH PRIVATE KEY-----\n..."}
}
```

The user needs `CONNECT` on the database, `USAGE` on each schema and `SELECT` on the tables to read;
discover lists only the relations the user may `SELECT` from, with the columns the user may `SELECT`.
A table readable only through column grants (`GRANT SELECT (id, name) ON ...`) is listed with just
those columns, and without its primary key when a key column is not granted. `check` names the cause of a failure:
wrong user name or password, an unreachable host, a missing database or schema, or a missing
`CONNECT` or `USAGE` privilege.

Through an SSH tunnel the database host is resolved and dialed by the jump server, so `host` can be a
name only the jump server knows. Set `tunnel_host_key` to the jump server's public host keys, one per
line, each in authorized_keys format (`ssh-ed25519 AAAA...`) or as a known_hosts line; the whole output
of `ssh-keyscan bastion.example.com` works. The connector asks the server for a key of a pinned type and
refuses the tunnel when it presents none of the pinned keys. `@revoked` and `@cert-authority` lines are
refused. Without `tunnel_host_key` the host key is not verified.

Only the configuration reaches the Postgres driver: the `PG*` environment variables (`PGHOST`,
`PGSSLMODE`, `PGOPTIONS`, ...) and `~/.pgpass` and `~/.postgresql` are ignored. One exception:
`PGSERVICE` naming a service that is missing from the service file fails the connection.

## SSL modes

`ssl_mode.mode` behaves like libpq's `sslmode`. Certificates are PEM strings in the configuration and
are never written to disk.

| Mode | Encryption | Server certificate |
|---|---|---|
| `disable` | never | — |
| `allow` | only when the server insists | not verified |
| `prefer` | when the server supports it | not verified |
| `require` | always | not verified; chain checked against `ca_certificate` when one is given |
| `verify-ca` | always | chain checked against `ca_certificate` |
| `verify-full` | always | chain and host name checked |

`client_certificate` and `client_key` add a client certificate in the `verify-*` modes, for servers
that require one (`clientcert=verify-ca` or `verify-full` in `pg_hba.conf`). `prefer` and `allow` refuse
a `ca_certificate`: they may connect without encryption, so the CA would protect nothing.

## Types

Postgres renders each row as JSON (`row_to_json`), so numbers and JSON documents keep every digit.
Every property is nullable. Domains map as their base type.

| Postgres | JSON schema | Value |
|---|---|---|
| `boolean` | `boolean` | `true` |
| `smallint`, `integer`, `bigint` | `integer` | exact, also beyond 2^53 |
| `real`, `double precision`, `numeric` | `number` | exact digits; `NaN`, `Infinity` and `-Infinity` read as `null` |
| `text`, `varchar`, `char`, `name`, `uuid`, `xml`, enums | `string` | as stored (`char(n)` keeps its padding) |
| `bytea` | `string` | base64 |
| `json`, `jsonb` | any (`{}`) | the document; `json` verbatim, `jsonb` as Postgres normalizes it |
| `date` | `string`, format `date` | `2024-02-29` |
| `time` | `string`, airbyte_type `time_without_timezone` | `13:45:30.5` |
| `timetz` | `string`, format `time`, airbyte_type `time_with_timezone` | `13:45:30+02:00` |
| `timestamp` | `string`, airbyte_type `timestamp_without_timezone` | `2024-01-02T03:04:05.123456` |
| `timestamptz` | `string`, format `date-time`, airbyte_type `timestamp_with_timezone` | `2024-01-02T01:04:05+00:00` (UTC) |
| `interval` | `string` | ISO 8601, `P1Y2M3DT4H5M6S` |
| arrays | `array` of the element type | `bytea` and unmapped elements as their text form; `NaN`/`Infinity` and infinite date and timestamp elements as `null`; multidimensional arrays are flattened in storage order (`{{1,2},{3,4}}` reads as `[1,2,3,4]`) |
| anything else (`point`, `inet`, ranges, composites, `tsvector`, ...) | `string` | the Postgres text form |

`infinity` and `-infinity` dates and timestamps read as `null`: the `date` format and the timestamp
types of their schemas cannot hold them.

## Streams

A configured stream names its table by namespace (the schema) and name. A stream without a namespace
is looked up in every configured schema; when more than one has a table or view of that name, the read
fails with a config error asking for the namespace.

## Incremental sync

Pick any column as the cursor (`cursor_field`, one column). The connector reads rows in cursor order
and keeps the greatest cursor in STREAM state:

```json
{"cursor_field": ["updated_at"], "cursor": "2024-01-05 00:00:00+00"}
```

- The next read emits rows whose cursor is **greater than or equal to** the saved one, so rows that
  share the last cursor value are emitted again; use a destination sync mode that deduplicates
  (`append_dedup`) when that matters.
- A STATE message follows every 10,000 rows and the end of the stream.
- Rows whose cursor is `NULL` are emitted by the first read only.
- A cursor value that sorts above every real one, such as `infinity`, `NaN` or a far-future date,
  becomes the saved cursor: later reads emit only the rows at that value and never the rows changed
  since. Keep such values out of the cursor column, or pick another cursor.
- When the saved state names a different cursor field than the catalog, the read logs a warning and
  starts the stream over.
- Deletes are not seen, and an update shows up only when it moves the cursor forward. There is no
  CDC (logical replication) yet.
