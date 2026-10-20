# destination-gcs

Google Cloud Storage destination built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Writes
each stream as JSON Lines, CSV or Parquet files into a Cloud Storage bucket. Uses
[cloud.google.com/go/storage](https://pkg.go.dev/cloud.google.com/go/storage) and the pure-Go
[parquet-go](https://github.com/parquet-go/parquet-go). Object names, formats, sync modes and state handling
are the same as [destination-s3](../destination-s3).

```bash
task test CONNECTOR=destination-gcs
task build CONNECTOR=destination-gcs
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/destination-gcs:dev check --config /data/config.json
```

The tests run [fake-gcs-server](https://github.com/fsouza/fake-gcs-server) in process and need no Docker and
no credentials. `TestIntegration` runs `check` and two overwrite syncs against a real bucket when
`DESTINATION_GCS_TEST_BUCKET` and `DESTINATION_GCS_TEST_CREDENTIALS` (a service account key JSON) are set,
under a random prefix it removes afterwards; it is skipped otherwise.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `bucket` | | Bucket the files are written to |
| `bucket_path` | | Name prefix of every object |
| `credentials` | | See [Authentication](#authentication) |
| `format` | `{"format_type": "jsonl"}` | See [Formats](#formats) |
| `path_template` | `{namespace}/{stream}/{yyyy}_{mm}_{dd}/{part}` | See [Object names](#object-names) |
| `file_size_mb` | `100` | A file is uploaded and a new one started after this many MB before compression (at most 4096) |
| `max_records_per_file` | `0` | A file is uploaded and a new one started after this many records; 0 is no limit |

```json
{
  "bucket": "warehouse",
  "bucket_path": "synclet",
  "credentials": {"auth_type": "service_account", "service_account_info": "{\"type\": \"service_account\", ...}"},
  "format": {"format_type": "parquet", "compression": "zstd"},
  "path_template": "{namespace}/{stream}/country={field:country}/{yyyy}/{mm}/{dd}/{part}"
}
```

### Authentication

| `credentials.auth_type` | Fields |
|---|---|
| `service_account` (default) | `service_account_info`: the JSON key of a service account; a key of another type is rejected |
| `application_default` | none: `GOOGLE_APPLICATION_CREDENTIALS`, workload identity or the metadata server |

`service_account_info` must be a key of type `service_account` whose `token_uri`, when set, is
`https://oauth2.googleapis.com/token` (or the legacy `https://accounts.google.com/o/oauth2/token`) and whose
`universe_domain`, when set, is `googleapis.com`; any other key is rejected before a request is sent.

HMAC keys are not supported. The credentials need `storage.objects.create`, `storage.objects.delete` and
`storage.objects.list` on the bucket (`roles/storage.objectAdmin`); bucket metadata is never read. `check`
writes and deletes a probe object `<bucket_path>/_synclet_check_<uuid>` and lists `bucket_path`, and names
the cause of a failure: `invalid credentials`, `bucket "<name>" does not exist`, `no write permission`, `no
delete permission` or `no list permission`.

## Object names

The name of a file is `<bucket_path>/<rendered path_template><extension>`. Tokens:

| Token | Value |
|---|---|
| `{namespace}`, `{stream}` | The stream's namespace and name |
| `{yyyy}`, `{mm}`, `{dd}`, `{hh}` | The sync's start time, UTC |
| `{field:<name>}` | A top-level field of the record: `__null__` when it is null or missing, `__empty__` for an empty string; an object or array fails the sync |
| `{part}` | `<sync start in ms>_<random sync id>_<sequence>`, so two syncs starting in the same millisecond never share a name; required once, in the last segment |

Values are path-escaped, so a value is always one segment. A segment that renders empty, such as
`{namespace}` of a stream without one, is dropped. `{stream}` is required in a directory segment, and only
literals, `{namespace}` and `{stream}` may come before the end of that segment: the names up to it are the
stream's prefix, which overwrite cleans. A sync whose overwrite stream's prefix contains, or lies inside,
another stream's prefix fails before writing. The extension is `.jsonl`, `.csv` (`.gz` appended when
gzipped) or `.parquet`.

Records with different partition values go to different files; at most 64 files are open at once, the
least recently written is uploaded first when another is needed.

## Formats

Every format writes `_airbyte_raw_id` (a UUID) and `_airbyte_extracted_at` (the record's `emitted_at`, in
milliseconds).

- **JSON Lines** (`compression`: `none`, `gzip`): one object per record, the record data verbatim under
  `_airbyte_data`.
- **CSV** (`compression`: `none`, `gzip`): a header, then one column per declared top-level property in schema
  order, then `_airbyte_additional_properties` with the undeclared fields as one JSON object. Every non-null
  value is quoted; null is an empty unquoted field, so it differs from an empty string (`""`). Strings are
  written as is, other values as JSON text.
- **Parquet** (`compression`: `snappy`, `zstd`, `gzip`, `none`): one optional column per declared top-level
  property plus `_airbyte_additional_properties` (JSON), typed by the JSON schema:

| JSON schema | Parquet |
|---|---|
| `integer`, `number` with `airbyte_type: integer` | INT64 |
| `number` | DECIMAL(38, `decimal_scale`) as FIXED_LEN_BYTE_ARRAY(16), `decimal_scale` default 9; STRING of the number's text with `parquet_number_type: string` |
| `integer` or `number` with `airbyte_type: big_integer` | DECIMAL(38, 0) |
| `boolean` | BOOLEAN |
| `string`, `format: date` | DATE |
| `string`, `format: date-time` | TIMESTAMP(MICROS), adjusted to UTC; not adjusted with `airbyte_type: timestamp_without_timezone` |
| `string`, `format: time`, `airbyte_type: time_without_timezone` | TIME(MICROS) |
| `string`, `format: time` otherwise | STRING |
| `string` | STRING |
| `object`, `array`, a union of types, `anyOf`, `oneOf`, no type | STRING with the JSON logical type |

Dates, times, timestamps and decimals are annotated leaves, never structs. A value that does not fit its
column fails the sync with an error naming the stream, field and value: a wrong type, a number with more
decimal places than `decimal_scale` or more than 38 digits, a time or timestamp with digits below
microseconds, an integer outside INT64, or a timestamp with an offset in a `timestamp_without_timezone`
column. A `date-time` without an offset in a column with a time zone is read as UTC. Nothing is nulled or
dropped.

## Sync modes

| Mode | Objects |
|---|---|
| `overwrite` | The sync writes new objects under the stream's prefix. Once the whole input is written and uploaded, every other object under that prefix is deleted, so readers see the previous objects until the sync succeeds. A failed sync deletes nothing |
| `append` | The sync adds objects; earlier objects stay |

`append_dedup` is not supported; the spec does not offer it and a catalog that asks for it fails the sync.

## Files and state

- A file is uploaded when it reaches `file_size_mb` or `max_records_per_file`, when a 65th file is needed
  (the least recently written one goes), on every STATE and at the end of the input.
- A STATE is echoed only after every open file is uploaded, so every record received before it is in the
  bucket.
- The STREAM states of an `overwrite` stream, and GLOBAL and LEGACY states while the catalog has an
  `overwrite` stream, are held until the previous objects are deleted: a sync resumed from them would
  delete the files of this one.

## Failures

The connector fails the sync rather than drop, null or truncate a value. The error names the stream, the
field and the value:

- a record that is not a JSON object, or of a stream that is not in the catalog;
- a Parquet value that does not fit its column (see [Formats](#formats));
- a partition field holding an object or array.

On failure the open files are discarded and no state is echoed for their records. Files uploaded before the
failure stay, and so do the states echoed for them; the files of an `overwrite` stream stay until the next
successful overwrite of that stream deletes them.

## Limits

- No Avro output.
- No schema evolution: each file follows the catalog of the sync that wrote it.
- No CDC deletes: a source's delete markers are written as ordinary records.
- Memory: a Parquet file buffers up to 16 MiB of records before it flushes a row group, and a sync keeps up to 64 files open, so a sync writing many partitions at once can hold close to 1 GiB.
