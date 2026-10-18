# source-airtable

[Airtable](https://airtable.com) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads every table of the bases a personal
access token can see, one stream per table, with a schema built from the table's fields. Tables with a
"Last modified time" field can sync incrementally.

```bash
task test CONNECTOR=source-airtable     # no network, no credentials
task build CONNECTOR=source-airtable
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-airtable:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `personal_access_token` | yes | [Personal access token](https://airtable.com/create/tokens) with the scopes `data.records:read` and `schema.bases:read` and access to the bases to sync. Sent only as `Authorization: Bearer <token>`, never in a URL |
| `base_ids` | no | Bases to sync, by the id in the base URL (`appXXXXXXXXXXXXXX`: `app` and 14 letters or digits). Empty syncs every base the token can see |

```json
{"personal_access_token": "<your token>", "base_ids": ["appAbCdEfGhIjKlMn"]}
```

### Authentication

This version authenticates with a personal access token only. OAuth is not supported: Airtable rotates
the refresh token on every refresh (`POST https://airtable.com/oauth2/v1/token` returns a new one and
invalidates the old), and persisting the new refresh token back into the connection's configuration
needs a protocol decision Synclet has not made yet. Without it an OAuth connection would stop working
after its first refresh.

### Check

`check` requests `GET /v0/meta/whoami`, then `GET /v0/meta/bases/{baseId}/tables` for each configured
base, or `GET /v0/meta/bases` when none is configured, and fails with:

| Cause | Error starts with |
|---|---|
| The token is wrong, expired or revoked (401) | `invalid personal access token` |
| The token lacks a scope (whoami lists the token's scopes) | `token lacks scope(s) data.records:read, schema.bases:read` (the missing ones) |
| A configured base is not shared with the token or does not exist (403 or 404) | `base <baseId> is not shared with the token or does not exist` |
| No base is configured and listing bases is refused (403), most likely because the token lacks `schema.bases:read` | `the token cannot list bases, most likely it lacks the scope schema.bases:read` |
| No base is configured and the token sees none | `no base is shared with the token` |

## Streams

Discover lists the bases (`GET /v0/meta/bases`, paged by `offset`, filtered to `base_ids` when set) and
the tables of each (`GET /v0/meta/bases/{baseId}/tables`). Each table is one stream named
`<base name>/<table name>`. When two of the synced bases have the same name, the streams of both are
named `<base name> (<baseId>)/<table name>`.

A record holds:

| Property | Value |
|---|---|
| `_airtable_id` | Record id (`rec...`), the primary key |
| `_airtable_created_time` | Record creation time |
| one per field, under the field's name | The field's value; a field without a value is absent. A field named `_airtable_id` or `_airtable_created_time` is emitted as `<name> (<field id>)`, such as `_airtable_id (fldAbCdEfGhIjKlMn)` |

The schema carries the ids it was built from: `x-airtable-base-id` and `x-airtable-table-id` at its top
level and `x-airtable-field-id` on each field property. Read requests records with
`returnFieldsByFieldId=true` and maps field ids to the catalog's property names, so a field renamed in
Airtable after discover keeps its column until the schema is refreshed. A field added after discover is
not emitted until then; a field deleted after discover is no longer emitted. A catalog without these hints
is matched by stream name and field names instead.

### Type mapping

Every property is nullable.

| Airtable field type | JSON schema |
|---|---|
| `singleLineText`, `multilineText`, `richText`, `email`, `url`, `phoneNumber`, `singleSelect`, `externalSyncSource` | `string` |
| `number`, `percent`, `currency`, `duration` | `number` (`duration` in seconds, `percent` as a fraction) |
| `autoNumber`, `count`, `rating` | `integer` |
| `checkbox` | `boolean` |
| `date` | `string`, format `date` |
| `dateTime`, `createdTime`, `lastModifiedTime` | `string`, format `date-time`, `airbyte_type` `timestamp_with_timezone` |
| `multipleSelects`, `multipleRecordLinks` | array of `string` (choice names, linked record ids) |
| `singleCollaborator`, `createdBy`, `lastModifiedBy` | object `{id, email, name}` |
| `multipleCollaborators` | array of `{id, email, name}` |
| `multipleAttachments` | array of `{id, url, filename, size, type, width, height, thumbnails}` |
| `barcode` | object `{text, type}` |
| `button` | object `{label, url}` |
| `aiText` | object `{state, value, isStale, errorType}` |
| `formula`, `rollup` | the mapping of the result type the API gives in `options.result` |
| `multipleLookupValues` | array of the mapping of `options.result` |
| a type not listed, or a formula, rollup or lookup without a result type | any value (`{}`) |

A formula, rollup or lookup whose computation fails returns an error value (`{"error": "#ERROR!"}`,
`{"specialValue": "NaN"}`). It is emitted as `null`, the record is kept, and the read logs one warning
per field.

## Incremental sync

A table with at least one "Last modified time" field supports incremental sync; its first such field is
the default cursor, and any other "Last modified time" field can be chosen as the cursor in the stream's
settings. A table without one is full refresh only, and choosing a cursor that is not a "Last modified
time" field fails the read with a configuration error.

An incremental read sorts by the cursor field and, once a state is saved, requests

```
IF({<cursor field>},
   DATETIME_DIFF({<cursor field>}, DATETIME_PARSE('<saved>'), 'milliseconds') >= 0,
   DATETIME_DIFF(CREATED_TIME(), DATETIME_PARSE('<saved>'), 'milliseconds') >= 0)
```

The `IF` compares a blank cursor field by the record's creation time and never passes it to
`DATETIME_DIFF`, so a blank value cannot turn the formula into an error that would exclude the record.

- The filter uses `>=`, so the records at exactly the saved value are read again; deduplicate on
  `_airtable_id`.
- A "Last modified time" field can watch all fields or only some. Only a field that watches all fields
  catches every change; one that watches some fields misses changes to the others. Records whose cursor
  field is blank (never modified in a watched field) are matched by their creation time, so new records
  are not missed.
- The state is `{"cursor": "<RFC 3339 time>"}`: the greatest cursor value emitted, taking the creation
  time of a record with a blank cursor. It is emitted once, at the end of the stream, because records
  with a blank cursor break the sort order.
- Deleted records are not detected by an incremental read; a full refresh removes them.

## Limits

- Requests to one base are spaced at least 200 ms apart (Airtable allows 5 requests per second per
  base); metadata requests count for their base.
- A 429 blocks the base for about 30 seconds and carries no `Retry-After`: the request waits 30 seconds
  and is repeated, at most 5 times.
- A 5xx response or a transport error is repeated 3 times with a delay doubling from 1 second.
- Records are read 100 per page, the most Airtable allows, following `offset`.
