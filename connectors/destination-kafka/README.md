# destination-kafka

Destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk) and
[franz-go](https://github.com/twmb/franz-go) that produces every record as one JSON message to a
Kafka topic per stream. Topics are append-only, so `append` is the only destination sync mode.
Each STATE is echoed only after the brokers have acknowledged every record before it (the
producer is flushed first), so a sync that stops resumes from the last state whose records are in
Kafka. Records produced after the last echoed state may be produced again on resume: delivery is
at least once.

```bash
task test CONNECTOR=destination-kafka
task build CONNECTOR=destination-kafka
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-kafka:dev check --config /data/config.json
```

## Messages

The message value is the JSON envelope of Airbyte's destination-kafka, so consumers written for it
keep working:

```json
{"_airbyte_ab_id":"8f0b6a52-6a2e-4b9e-9d53-2a3f0e3c1f6d","_airbyte_stream":"users","_airbyte_emitted_at":1700000000000,"_airbyte_data":{"id":1,"name":"Ada"}}
```

| Field | Value |
|---|---|
| `_airbyte_ab_id` | A random UUID per message |
| `_airbyte_stream` | The stream name, without its namespace |
| `_airbyte_emitted_at` | When the source read the record, Unix milliseconds |
| `_airbyte_data` | The record, as the source emitted it |

The message key is the record's primary key when the stream has one (the configured primary key,
else the source-defined one), so every version of a row lands in the same partition in order and
compacted topics keep the latest one:

| Primary key | Key |
|---|---|
| One field, a string | The string, without quotes: `usr_1` |
| One field, any other value | Its JSON: `42`, `true`, `{"a":1}` |
| Several fields | A JSON array of the values in key order, `null` for a missing one: `["acme",7]` |
| None, or every key field missing or null | The message's `_airbyte_ab_id`, as Airbyte does for every message |

Messages carry no headers. Partitioning is franz-go's default: by key hash (murmur2, the same
partition the Kafka Java client picks for that key).

## Topics

`topic_pattern` names each stream's topic; `{namespace}` and `{stream}` are replaced by the
stream's namespace (empty when it has none) and name. As in Airbyte's destination-kafka, the result
is then normalized: accents are dropped, whitespace runs become one `_` and every other character
outside `A-Z a-z 0-9 _` becomes `_`. So `{namespace}.{stream}` gives `public_users`, and a pattern
cannot produce a dot or a dash. A name longer than 249 characters fails the sync.

Topics are not created by the connector: the client asks the brokers to create a missing topic on
first use, which works when the cluster allows it (`auto.create.topics.enable`, Kafka's default).
Otherwise create the topics beforehand; producing to a missing one fails the sync.

`check` connects to the brokers (which also verifies SASL credentials) and, when `test_topic` is
set, produces one message to it, a regular envelope with `_airbyte_stream` `connection_check`.
`test_topic` is used as written, not normalized.

## Configuration

The keys are those of Airbyte's destination-kafka, so an existing configuration carries over.

| Key | Default | Notes |
|---|---|---|
| `bootstrap_servers` | (required) | `host:port[,host:port...]` |
| `topic_pattern` | (required) | See [Topics](#topics) |
| `test_topic` | | Topic `check` produces to |
| `sync_producer` | `false` | Wait for each record's acknowledgment before producing the next; slow |
| `protocol.security_protocol` | `PLAINTEXT` | `PLAINTEXT`, `SASL_PLAINTEXT` or `SASL_SSL` (TLS 1.2+, system root CAs) |
| `protocol.sasl_mechanism` | `PLAIN` | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512` |
| `protocol.sasl_username`, `protocol.sasl_password` | | SASL credentials |
| `protocol.sasl_jaas_config` | | Airbyte's way to give them: the `username="..."` and `password="..."` of a `sasl.jaas.config` value are used when `sasl_username` is empty |
| `client_id` | `synclet-producer` | |
| `acks` | `1` | `0`, `1` or `all` (`-1`) |
| `enable_idempotence` | `false` | Needs `acks` `all` |
| `compression_type` | `none` | `none`, `gzip`, `snappy`, `lz4` or `zstd` |
| `linger_ms` | `0` | |
| `max_request_size` | `1048576` | Largest record batch before compression, up to 1 GiB; a larger record fails the sync. Brokers also cap it (`message.max.bytes`) |
| `max_in_flight_requests_per_connection` | `5` | Used without idempotence; with it the client allows 5 |
| `retries` | `2147483647` | Retries of a failed batch; the default retries until `delivery_timeout_ms` |
| `request_timeout_ms` | `30000` | |
| `delivery_timeout_ms` | `120000` | `0` is no limit |
| `buffer_memory` | `33554432` | Bytes buffered before producing waits; at least `max_request_size` |

Numbers may be JSON numbers or numeric strings (Airbyte types `linger_ms` and `buffer_memory` as
strings).

### Differences from Airbyte's destination-kafka

- A failed produce fails the sync, and no state after the failed record is echoed, with
  `sync_producer` on or off.
- Records of a stream with a primary key are keyed by it; Airbyte keys every message by its random
  `_airbyte_ab_id`.
- `check` without `test_topic` still connects to the brokers.
- Not supported: `SASL_SSL` with `GSSAPI` (Kerberos) or `OAUTHBEARER`. TLS without SASL, client
  certificates and custom CA bundles are not configurable.
- Accepted and ignored, as Kafka Java client settings franz-go has no equivalent for:
  `batch_size` (franz-go fills a batch up to `max_request_size` while lingering), `client_dns_lookup`,
  `socket_connection_setup_timeout_ms`, `socket_connection_setup_timeout_max_ms`, `max_block_ms`,
  `send_buffer_bytes` and `receive_buffer_bytes`.

## Tests

Offline tests run against franz-go's in-process `kfake` cluster: message shape and keys, topic
names, state echo after acknowledgment (the cluster's message count is checked as each STATE is
written), failed produces, every compression and acks setting, SASL PLAIN/SCRAM and SASL_SSL with a
test certificate.

`TestIntegration` runs check and write against a real broker when `KAFKA_TEST_BOOTSTRAP_SERVERS` is
set, and skips otherwise:

```bash
docker run -d --rm --name redpanda -p 127.0.0.1:19092:19092 docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
  redpanda start --mode dev-container --smp 1 --kafka-addr 0.0.0.0:19092 --advertise-kafka-addr 127.0.0.1:19092
KAFKA_TEST_BOOTSTRAP_SERVERS=127.0.0.1:19092 go test -run TestIntegration ./...
```
