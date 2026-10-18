# source-mongodb

MongoDB source built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Reads the collections of one database, as full refresh or incremental by a cursor field. Uses the official
[mongo-go-driver v2](https://github.com/mongodb/mongo-go-driver); tested against MongoDB 7 as a single-node
replica set.

```bash
task test CONNECTOR=source-mongodb     # needs Docker; go test -short skips the database tests
task build CONNECTOR=source-mongodb
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-mongodb:dev check --config /data/config.json
```

## Configuration

The keys match Airbyte's source-mongodb-v2, so an existing configuration carries over.

| Key | Default | Meaning |
|---|---|---|
| `database_config.cluster_type` | `SELF_MANAGED_REPLICA_SET` | `ATLAS_REPLICA_SET` or `SELF_MANAGED_REPLICA_SET`; both connect the same way |
| `database_config.connection_string` | | `mongodb://h1:27017,h2:27017/?replicaSet=rs0` (a host list) or `mongodb+srv://cluster0.example.mongodb.net/` |
| `database_config.database` | | The database to read |
| `database_config.username`, `password` | | Login (SCRAM); overrides a user in the connection string |
| `database_config.auth_source` | the connection string's `authSource`, else `admin` | Database that holds the user |
| `discover_sample_size` | `10000` | Documents sampled per collection to infer its schema (1-100000) |
| `tls_ca_certificate` | | PEM certificate of the CA that signed the server certificates; TLS itself is turned on by `tls=true` in the connection string (on by default for `mongodb+srv://`) |
| `tunnel_method` | `{"tunnel_method": "NO_TUNNEL"}` | `SSH_KEY_AUTH` (`ssh_key`) or `SSH_PASSWORD_AUTH` (`tunnel_user_password`), both with `tunnel_host`, `tunnel_port` (22), `tunnel_user` and optionally `tunnel_host_key` |

```json
{
  "database_config": {
    "cluster_type": "ATLAS_REPLICA_SET",
    "connection_string": "mongodb+srv://cluster0.example.mongodb.net/",
    "database": "app",
    "username": "synclet_reader",
    "password": "..."
  },
  "discover_sample_size": 10000
}
```

The user needs `find` and `listCollections` on the database; the built-in `read` role on it is enough.
`check` names the cause of a failure: wrong user name or password, an unreachable host, an SRV record that
does not resolve, an untrusted server certificate, a user that may not read the database, or one that may
list the collections but not read them. Error messages never contain the connection string or a password.

Through an SSH tunnel every server address, including the replica set members the first server announces,
is dialed by the jump server, so they can be names only the jump server knows. Set `tunnel_host_key` to
the jump server's public host keys, one per line, in authorized_keys format or as known_hosts lines; the
output of `ssh-keyscan bastion.example.com` works. Without `tunnel_host_key` the host key is not verified.

## Streams

Discover lists the collections of the database: views and `system.*` collections are left out. Each stream
has the database as namespace, `_id` as primary key and default cursor, and supports full refresh and
incremental. Its schema is inferred from a `$sample` of `discover_sample_size` documents: every field seen
in the sample is a nullable property, nested documents and arrays get their own properties and item
schemas, and fields not in the sample are allowed (`additionalProperties` is not restricted). A record is
the whole document.

Documents are read through a driver cursor in batches of 1,000: the connector never holds a collection,
or more than one batch, in memory.

## Types

| BSON | JSON schema | Value |
|---|---|---|
| ObjectId | `string` | hex, `65a1b2c3d4e5f60718293a4b` |
| Int32, Int64 | `integer` | exact, also beyond 2^53 |
| Double | `number` | shortest form that reads back as the same value; NaN and ±Infinity are `null` |
| Decimal128 | `number` | exact, every digit, never through a float; NaN and ±Infinity are `null` |
| Date | `string`, format `date-time` | RFC 3339 UTC, `2024-02-29T13:45:30.123Z` |
| String, Symbol | `string` | as stored |
| Binary (every subtype, UUID included) | `string` | base64 |
| Embedded document | `object` | the document |
| Array | `array` | the items, each by this table |
| Boolean | `boolean` | `true` |
| Null, Undefined | `null` | `null` |
| Timestamp, Regex, JavaScript, MinKey, MaxKey, DBPointer | `string` | relaxed Extended JSON, `{"$timestamp":{"t":1700000000,"i":3}}` |

A field whose sampled values have different types gets the union of their types (`["null", "integer",
"string"]`); integer and number together are `number`. `format: date-time` is set only when every sampled
value of the field written as a string is an RFC 3339 date-time.

## Incremental sync

The cursor field defaults to `_id`, which works for ObjectIds the server or driver generates: they grow
with insert time. To see updates, choose a field the application sets on every change, such as
`updated_at`; a dotted path (`meta.updated_at`) works. The connector reads documents sorted by the cursor
and keeps the greatest cursor in STREAM state, as canonical Extended JSON so it keeps its BSON type:

```json
{"cursor_field": ["updated_at"], "cursor": {"$date": {"$numberLong": "1704412800000"}}}
```

- The next read emits documents whose cursor is **greater than or equal to** the saved one, so documents
  that share the last cursor value are emitted again; use a destination sync mode that deduplicates
  (`append_dedup`) when that matters.
- A STATE message follows every 10,000 documents and the end of the stream.
- Documents without the cursor field, or with `null` in it, are emitted by the first read only.
- The cursor field must hold one BSON type in every document. MongoDB compares values of different types by
  type order, and a query on the saved cursor only matches values of its type, so documents whose cursor has
  another type are skipped after the first read.
- When the saved state names a different cursor field than the catalog, the read logs a warning and starts
  the stream over.
- The state holds a cursor value, never a change stream resume token, so it cannot expire.

## Limitations

- Deletes are not seen, and an update shows up only when it moves the cursor forward.
- Change streams (CDC) are not supported yet; they are a planned follow-up.
- One database per source.
