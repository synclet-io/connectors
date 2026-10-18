# source-mssql

Microsoft SQL Server source built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Reads tables and views, as full refresh or incremental by a cursor column you choose. Uses the pure-Go
[go-mssqldb](https://github.com/microsoft/go-mssqldb) driver; tested against SQL Server 2022.

```bash
task test CONNECTOR=source-mssql     # needs Docker; go test -short skips the database tests
task build CONNECTOR=source-mssql
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-mssql:dev check --config /data/config.json
```

The tests run `mcr.microsoft.com/mssql/server:2022-latest`, which is published for amd64 only. On an arm64
host it runs under emulation (Rosetta in Docker Desktop or OrbStack); when it fails to start there, the
database tests are skipped with the reason. CI runs on amd64.

## Configuration

The keys match Airbyte's source-mssql, so an existing configuration carries over.

| Key | Default | Meaning |
|---|---|---|
| `host`, `port` | port `1433` | Database server |
| `database` | | Database name |
| `schemas` | `["dbo"]` | Schemas to discover and read |
| `username`, `password` | | Login |
| `authentication` | `SQL` | `SQL` for a SQL Server login, `NTLM` for a Windows domain account; see [NTLM](#ntlm) |
| `ssl_method` | `{"ssl_method": "encrypted_trust_server_certificate"}` | See [SSL methods](#ssl-methods) |
| `tunnel_method` | `{"tunnel_method": "NO_TUNNEL"}` | `SSH_KEY_AUTH` (`ssh_key`) or `SSH_PASSWORD_AUTH` (`tunnel_user_password`), both with `tunnel_host`, `tunnel_port` (22), `tunnel_user` and optionally `tunnel_host_key` |

```json
{
  "host": "sql.internal",
  "database": "app",
  "username": "synclet_reader",
  "password": "...",
  "schemas": ["dbo", "sales"],
  "ssl_method": {"ssl_method": "encrypted_verify_certificate", "certificate": "-----BEGIN CERTIFICATE-----\n..."},
  "tunnel_method": {"tunnel_method": "SSH_KEY_AUTH", "tunnel_host": "bastion.example.com", "tunnel_user": "synclet", "ssh_key": "-----BEGIN OPENSSH PRIVATE KEY-----\n..."}
}
```

The login needs a user in the database and `SELECT` on the tables and views to read, granted on the
objects, on the schema (`GRANT SELECT ON SCHEMA::sales TO synclet_reader`) or through `db_datareader`.
Discover lists only the tables and views with columns the user may `SELECT`, with just those columns: a table
readable only through column grants (`GRANT SELECT ON t (id, name) TO ...`) is listed without its primary key
when a key column is not granted. `check` names the cause of a failure: wrong user name or password, an
unreachable host, a missing database, a login without a user in the database, a missing schema, or no
`SELECT` permission on any table or view of a schema.

Through an SSH tunnel the database host is resolved and dialed by the jump server, so `host` can be a
name only the jump server knows. Set `tunnel_host_key` to the jump server's public host keys, one per
line, each in authorized_keys format (`ssh-ed25519 AAAA...`) or as a known_hosts line; the whole output
of `ssh-keyscan bastion.example.com` works. The connector asks the server for a key of a pinned type and
refuses the tunnel when it presents none of the pinned keys. `@revoked` and `@cert-authority` lines are
refused. Without `tunnel_host_key` the host key is not verified.

## SSL methods

| `ssl_method.ssl_method` | Encryption | Server certificate |
|---|---|---|
| `unencrypted` | the login only, when the server supports encryption | not verified |
| `encrypted_trust_server_certificate` | always | not verified |
| `encrypted_verify_certificate` | always | chain and host name checked |

`encrypted_verify_certificate` takes `certificate`, the PEM certificate of the CA that signed the server
certificate (empty: the CAs of the image), and `hostNameInCertificate`, the name the certificate must carry
when it is not `host` (for example when `host` is an IP address). The certificate stays in memory. A server
that cannot encrypt fails the two encrypted methods.

## NTLM

With `"authentication": "NTLM"` the connector logs in with a Windows domain account through NTLM: set
`username` to `DOMAIN\user` (in JSON `"CORP\\synclet"`) and `password` to its password. The server must be
joined to the domain and accept Windows logins. NTLM is implemented by go-mssqldb in pure Go; it is
unit-tested here, not tested against a domain. Kerberos is not supported yet.

## Types

Every property is nullable. Alias types map as their base type.

| SQL Server | JSON schema | Value |
|---|---|---|
| `bit` | `boolean` | `true` |
| `tinyint`, `smallint`, `int`, `bigint` | `integer` | exact, also beyond 2^53 |
| `decimal`, `numeric`, `money`, `smallmoney` | `number` | exact, every digit (`decimal(38,10)` included) |
| `real`, `float` | `number` | shortest form that reads back as the same value |
| `char`, `varchar`, `nchar`, `nvarchar`, `text`, `ntext`, `sysname` | `string` | as stored (`char(n)` keeps its padding) |
| `xml`, `sql_variant` | `string` | the text form |
| `uniqueidentifier` | `string` | lower case, `6f9619ff-8b86-d011-b42d-00c04fc964ff` |
| `binary`, `varbinary`, `image`, `rowversion` | `string` | base64 |
| `date` | `string`, format `date` | `2024-02-29` |
| `time` | `string`, airbyte_type `time_without_timezone` | `13:45:30.1234567` |
| `datetime`, `datetime2`, `smalldatetime` | `string`, airbyte_type `timestamp_without_timezone` | `2024-01-02T03:04:05.1234567` |
| `datetimeoffset` | `string`, format `date-time`, airbyte_type `timestamp_with_timezone` | `2024-01-02T03:04:05.1234567+02:00` |
| `hierarchyid` | `string` | `/1/2/` |
| `geography`, `geometry` | `string` | WKT, `POINT (30 10)` |
| anything else (CLR types) | `string` | the text form |

## Streams

A configured stream names its table by namespace (the schema) and name. A stream without a namespace
is looked up in every configured schema; when more than one has a table or view of that name, the read
fails with a config error asking for the namespace. Rows are streamed: the connector never holds a whole
table in memory.

## Incremental sync

Pick a column as the cursor (`cursor_field`, one column). `rowversion` is the best choice: SQL Server moves
it forward on every insert and update. A `datetime2` column that the application sets on every change works
too. The connector reads rows in cursor order and keeps the greatest cursor, as SQL Server renders it as
text, in STREAM state:

```json
{"cursor_field": ["updated_at"], "cursor": "2024-01-05T00:00:00"}
```

- The next read emits rows whose cursor is **greater than or equal to** the saved one, so rows that
  share the last cursor value are emitted again; use a destination sync mode that deduplicates
  (`append_dedup`) when that matters.
- A STATE message follows every 10,000 rows and the end of the stream.
- Rows whose cursor is `NULL` are emitted by the first read only.
- `xml`, `text`, `ntext`, `image`, `geography`, `geometry`, `sql_variant` and CLR type columns cannot be
  ordered, so they cannot be a cursor; the read fails with a config error.
- When the saved state names a different cursor field than the catalog, the read logs a warning and
  starts the stream over.
- Deletes are not seen, and an update shows up only when it moves the cursor forward. CDC through SQL Server
  change data capture is not supported yet.
