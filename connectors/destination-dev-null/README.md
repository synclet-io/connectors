# destination-dev-null

Destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk) that accepts
every record and stores none of them: for testing sources, pipelines and the platform without a
real target. Every STATE is echoed as soon as it arrives, so syncs checkpoint. All destination sync
modes are accepted, `check` always succeeds for a valid configuration and nothing touches the network.

```bash
task test CONNECTOR=destination-dev-null
task build CONNECTOR=destination-dev-null
echo '{"type":"RECORD","record":{"stream":"users","data":{"id":1},"emitted_at":0}}
{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"id":1}}}}' \
  | docker run --rm -i -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-dev-null:dev \
    write --config /data/config.json --catalog /data/catalog.json
```

## Configuration

The keys match Airbyte's destination-dev-null, so an existing configuration carries over. A
configuration without `test_destination` is `SILENT`, and `LOGGING` without `logging_config` logs
the first 100 records of each stream.

| `test_destination_type` | Keys | Behaviour |
|---|---|---|
| `SILENT` | | Drops every record |
| `LOGGING` | `logging_config` | Drops every record and logs some of each stream as INFO `LOG` messages |
| `THROTTLED` | `millis_per_record` | Waits that many milliseconds per record, then drops it |
| `FAILING` | `num_messages` | Accepts that many records of each stream; the next record of a stream fails the sync with a `TRACE` error and a non-zero exit |

`logging_config.logging_type` picks the records logged per stream, at most `max_entry_count` of
them (1 to 1,000, default 100):

| `logging_type` | Keys | Logs |
|---|---|---|
| `FirstN` | `max_entry_count` | The first records |
| `EveryNth` | `nth_entry_to_log`, `max_entry_count` | Records 1, N+1, 2N+1, … (N from 1 to 1,000) |
| `RandomSampling` | `sampling_ratio`, `seed`, `max_entry_count` | Each record with probability `sampling_ratio` (0 to 1, default 0.001); a `seed` makes the sample repeatable |

```json
{"test_destination": {"test_destination_type": "LOGGING",
  "logging_config": {"logging_type": "EveryNth", "nth_entry_to_log": 10, "max_entry_count": 50}}}
```

A logged record reads `Logging Destination(stream=<namespace.name>, recordIndex=<0-based index>,
logEntry=<n>/<max>): <record data>`.
