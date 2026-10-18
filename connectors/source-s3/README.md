# source-s3

S3 source built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads CSV, TSV, JSONL and
Parquet files from AWS S3 or any S3-compatible storage (MinIO, Cloudflare R2, ...), one stream per glob, as
full refresh or incremental by object modification time. Uses
[aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) and [parquet-go](https://github.com/parquet-go/parquet-go).

```bash
task test CONNECTOR=source-s3     # needs Docker for MinIO; go test -short skips those tests
task build CONNECTOR=source-s3
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-s3:dev check --config /data/config.json
```

The Docker-backed tests run MinIO from `bitnamilegacy/minio`, the archived Bitnami build, which also ships
`mc` for creating restricted users; MinIO no longer publishes `minio/minio` images.

## Configuration

| Key | Default | Meaning |
|---|---|---|
| `bucket` | | Bucket name |
| `region` | `us-east-1` | Region of the bucket |
| `endpoint` | AWS S3 | Endpoint of an S3-compatible service, such as `http://minio:9000` or `https://<account>.r2.cloudflarestorage.com` |
| `force_path_style` | `false` | Address the bucket as `endpoint/bucket` (MinIO usually needs it) |
| `credentials` | | See [Credentials](#credentials) |
| `streams` | | One entry per stream, see [Streams](#streams) |

```json
{
  "bucket": "exports",
  "region": "eu-west-1",
  "credentials": {"auth_type": "assume_role", "role_arn": "arn:aws:iam::123456789012:role/synclet-reader"},
  "streams": [
    {"name": "orders", "glob": "orders/**/*.csv.gz", "format": {"filetype": "csv"}},
    {"name": "events", "glob": "events/*.parquet", "format": {"filetype": "parquet"}},
    {"name": "logs", "glob": "logs/*.jsonl", "format": {"filetype": "jsonl"}, "skip_bad_rows": true}
  ]
}
```

### Credentials

| `credentials.auth_type` | Keys | Signs with |
|---|---|---|
| `access_key` | `access_key_id`, `secret_access_key` | A static access key |
| `default_chain` | | The AWS SDK default chain: environment, shared config files, web identity (EKS IRSA), ECS task role, EC2 instance profile |
| `assume_role` | `role_arn`, optional `external_id`, `role_session_name` (`synclet-source-s3`), `access_key_id`, `secret_access_key` | Temporary credentials from STS `AssumeRole`; the STS call is signed with the access key when one is given, with the default chain otherwise. With `endpoint` set, STS is called at that endpoint (MinIO serves STS there) |

The credentials need `s3:ListBucket` on the stream prefixes and `s3:GetObject` on the objects. `check` resolves
the credentials (assuming the role), lists each stream's prefix and reads the first byte of its first matching
object, and names the cause of a failure: invalid credentials, a role that cannot be assumed, a missing bucket,
no list permission on a prefix, no read permission on an object, or a bucket in another region. A stream
without a schema and without a matching object fails `check`, since discover could not infer its schema.

### Streams

| Key | Default | Meaning |
|---|---|---|
| `name` | | Stream name, unique |
| `glob` | | Object keys of the stream: `*` matches within a path segment, `**` across segments, `?` one character, `[a-z]` a class, `{a,b}` alternatives. The literal part before the first wildcard is the listing prefix, so `exports/2026/*.csv` lists only `exports/2026/` |
| `format` | | `{"filetype": "csv" \| "tsv" \| "jsonl" \| "parquet", ...}`, see [Formats](#formats) |
| `compression` | `auto` | `auto`: by extension (`.gz`, `.gzip`, `.zst`, `.zstd`), then by the first bytes (gzip or zstd magic); `none`, `gzip` or `zstd`: always that |
| `schema` | inferred | The records' JSON schema as a string: an object schema with `properties`. A property's `type` may be one type or a list |
| `schema_sample_files` | `10` | How many of the newest matching files the schema is inferred from |
| `skip_bad_rows` | `false` | Skip bad rows and log how many per file, instead of failing the read |

Keys ending in `/` (folder markers) are never read.

## Formats

**CSV and TSV.** Options in `format`:

| Key | CSV default | TSV default | Meaning |
|---|---|---|---|
| `delimiter` | `,` | tab | Character between fields |
| `quote_char` | `"` | empty | Quote character; empty turns quoting off |
| `escape_char` | empty | empty | Character that makes the next one literal, inside or outside quotes |
| `header` | `true` | `true` | First row (after `skip_rows`) names the columns |
| `column_names` | | | Column names; required without a header, replace the header's names otherwise |
| `skip_rows` | `0` | `0` | Lines skipped at the start of each file |
| `null_values` | `[""]` | `[""]` | Unquoted values read as null |

A quoted field can hold the delimiter, line breaks (`\n` or `\r\n`) and the quote doubled (`""`). A quoted
value is never null, so `""` is an empty string and an unquoted empty field is null. Empty lines are skipped. A
header cell that is empty names its column `column_<n>`; a header naming a column twice fails the file. A row
with more or fewer fields than the header is a bad row.

**JSONL.** One JSON object per line; blank lines are skipped. A line that is not one JSON object is a bad row.
Nested objects and arrays are kept as they are.

**Parquet.** The object is downloaded to a temporary file, since Parquet needs random access. Every column
type is read; nested groups become objects, `LIST` groups and repeated fields arrays, `MAP` groups with string
keys objects and other `MAP` groups arrays of `{"key", "value"}`.

## Schema and types

Without a configured `schema`, discover reads the newest `schema_sample_files` matching files (whole files)
and makes every field nullable:

- CSV and TSV columns take the narrowest type all values fit: `boolean` (`true`, `false`, any case of those
  two), `integer`, `number` (JSON number syntax) or `string`; integer and number widen to number, any other mix
  to string. A column with only nulls is a string.
- JSONL fields take the union of the JSON types seen, with `integer` folded into `number`.
- Parquet fields take the type of the file's schema; several files give the union.

Inference stays bounded to the sampled files instead of reading every matching file, since discover over a
large bucket would otherwise download all of it. A column that only files outside the sample have (older
files, when the schema changed) is not in the inferred schema, so reading such a file fails, also with
`skip_bad_rows`, naming the file, the column and the fix: set `schema_sample_files` to at least the file's
position among the matching files, newest first, which the error gives, or configure a `schema` with the
column, then run discover again and refresh the catalog.

| Parquet | JSON schema | Value |
|---|---|---|
| `BOOLEAN` | `boolean` | |
| `INT32`, `INT64`, `INT(n, signed or not)` | `integer` | Exact, unsigned 64-bit too |
| `FLOAT`, `DOUBLE` | `number` | Shortest decimal that round-trips; NaN and infinity are bad rows |
| `DECIMAL(p, s)` | `number` | Every digit, from the unscaled integer |
| `STRING`, `ENUM`, `JSON` | `string` | |
| `UUID` | `string` | `xxxxxxxx-xxxx-...` |
| `DATE` | `string`, `format: date` | `2026-10-18` |
| `TIMESTAMP(isAdjustedToUTC=true)` | `string`, `format: date-time`, `airbyte_type: timestamp_with_timezone` | `2026-10-18T13:14:15.123456Z` |
| `TIMESTAMP(isAdjustedToUTC=false)` | `string`, `format: date-time`, `airbyte_type: timestamp_without_timezone` | `2026-10-18T13:14:15.123` |
| `TIME` | `string`, `format: time` | `13:14:15.5`, with `Z` when adjusted to UTC |
| `INT96` | `string`, `format: date-time`, `airbyte_type: timestamp_with_timezone` | |
| other `BYTE_ARRAY`, `FIXED_LEN_BYTE_ARRAY` | `string` | Base64 |

Numbers keep their exact text: a CSV or JSONL `12345678901234567890.123456789` reaches the destination with
every digit, never through a float64.

Reading fits each row to the stream's schema in the catalog. A CSV value takes the first type of its property
it parses as (`boolean`, `integer`, `number`, then `string`); a JSONL or Parquet value must be of a type its
property allows. A value that fits none, or a field the schema does not have, is a bad row: discover again, or
add the field to the configured schema.

Every record carries three more fields:

| Field | Type | Value |
|---|---|---|
| `_synclet_source_file` | `string` | Object key |
| `_synclet_source_row` | `integer` | Row number in the file, from 1; header, skipped lines and blank lines are not counted, bad rows are |
| `_synclet_source_modified` | `string`, `date-time` | The object's LastModified (UTC) |

The primary key is `_synclet_source_file` + `_synclet_source_row`; the cursor is `_synclet_source_modified`.

## Bad rows

A row that cannot be parsed or does not fit the schema fails the read with the stream, the file and the row
(`stream "orders": file "orders/a.csv" row 2: line 3: 3 fields, the header has 2`). With `skip_bad_rows` the
row is skipped and a warning logs the count per file. A column missing from an inferred schema is not a bad
row (see [Schema and types](#schema-and-types)); with a configured `schema` it is.

## Sync modes and state

A full refresh reads every matching object in key order. An incremental read sorts the matching objects by
(LastModified, key) and reads those modified after the saved cursor, plus those modified exactly at it whose
key it has not read yet. After each object it emits a STATE:

```json
{"cursor": "2026-10-18T13:14:15Z", "keys": ["orders/a.csv", "orders/b.csv"]}
```

`cursor` is the greatest LastModified read, `keys` the objects read at exactly that time, so two objects with
the same timestamp, or one added later with the timestamp of the cursor, are each read once. An object
rewritten later gets a newer LastModified and is read again in full. An object rewritten within the same
second as the cursor under a key already in `keys` is not read again; S3 timestamps have no finer precision.

## Other storages

Listing and reading go through a small interface in [store.go](store.go):

```go
type objectStore interface {
	List(ctx context.Context, prefix string) ([]object, error)   // key, size, LastModified
	Open(ctx context.Context, key string) (io.ReadCloser, error)
}
```

Everything else (globs, formats, compression, schema inference, incremental state) only sees this interface.
GCS, Azure Blob Storage and SFTP sources are meant as sibling connectors that copy this connector and replace
`s3store.go` and `check.go` with their storage's implementation; there is no shared module across connectors.
