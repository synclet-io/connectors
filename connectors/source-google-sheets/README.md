# source-google-sheets

[Google Sheets API](https://developers.google.com/sheets/api/reference/rest) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads every grid sheet of one or more
spreadsheets as a stream: the first row names the columns, every later non-empty row is a record. Streams
are full refresh; with `incremental: true` a sheet is read again only when its spreadsheet changed, by the
Drive `modifiedTime`.

```bash
task test CONNECTOR=source-google-sheets     # no network, no credentials
task build CONNECTOR=source-google-sheets
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-google-sheets:dev check --config /data/config.json
```

Requests go to `https://sheets.googleapis.com/v4/spreadsheets/{id}`: `GET ?fields=sheets.properties` for the
sheet list and row counts, and `GET /values:batchGet` for rows (all header rows of a spreadsheet in one call,
then `batch_size` rows per call). With `incremental: true`, `GET
https://www.googleapis.com/drive/v3/files/{id}?fields=modifiedTime&supportsAllDrives=true` reads the
spreadsheet's last change.

## Configuration

| Key | Required | Default | Meaning |
|---|---|---|---|
| `spreadsheet_id` | yes | | ID or full URL of the spreadsheet |
| `credentials` | yes | | One of the three `auth_type` options, see [Authentication](#authentication) |
| `batch_size` | no | `10000` | Rows per `values:batchGet` request; a number or a numeric string |
| `names_conversion` | no | `false` | Convert column names to snake_case; `true`/`false` or `"true"`/`"false"` |
| `column_naming` | no | `legacy` | `legacy` or `stable`, see [Column names](#column-names) |
| `value_render` | no | `formatted` | `formatted` or `unformatted`, see [Value rendering](#value-rendering-and-merged-cells) |
| `incremental` | no | `false` | Offer incremental sync by the spreadsheet's Drive `modifiedTime`, see [Incremental sync](#incremental-sync) |
| `additional_spreadsheet_ids` | no | `[]` | IDs or URLs of more spreadsheets; their streams have the spreadsheet ID as namespace |
| `requests_per_minute` | no | `60` | Most API requests per minute, see [Quotas](#quotas) |

```json
{
  "spreadsheet_id": "https://docs.google.com/spreadsheets/d/1AbC.../edit",
  "credentials": {
    "auth_type": "Service",
    "service_account_info": "{\"type\": \"service_account\", \"client_email\": \"...\", \"private_key\": \"...\"}"
  },
  "column_naming": "stable",
  "incremental": true
}
```

## Authentication

The access token travels only in the `Authorization` header; a 401 answer fetches a new one once.

- **`Client`** (OAuth): `client_id`, `client_secret` and a `refresh_token` of a Google user who can open the
  spreadsheet, authorized with `https://www.googleapis.com/auth/spreadsheets.readonly`, and with
  `https://www.googleapis.com/auth/drive.metadata.readonly` too for `incremental: true`.
- **`Service`**: `service_account_info` is the JSON key of a service account (`"type": "service_account"`),
  which requests both scopes above; share the spreadsheet with its `client_email`. The key's `token_uri`
  must be `https://oauth2.googleapis.com/token` (or the legacy `https://accounts.google.com/o/oauth2/token`,
  or absent) and its `universe_domain` `googleapis.com` (or absent), so the signed assertion goes only to
  Google.
- **`ApplicationDefault`**: Google Application Default Credentials of the environment, with both scopes.

`check` gets an access token and reads the sheet list of every configured spreadsheet. A failed `check`
names the cause:

| Message | Cause |
|---|---|
| `service_account_info is not a valid service account key` | The key is not JSON, is not of type `service_account`, has a `token_uri` other than Google's token endpoint or a `universe_domain` other than `googleapis.com`, or has no PEM private key |
| `service account key is invalid, disabled or deleted` | The token endpoint rejected the signed assertion (`invalid_grant`, `invalid_client`) |
| `refresh_token is invalid or revoked` | The token endpoint answered `invalid_grant` |
| `client_id or client_secret is invalid` | The token endpoint answered `invalid_client` or `unauthorized_client` |
| `the credentials lack the https://www.googleapis.com/auth/spreadsheets.readonly scope` | 403 with reason `ACCESS_TOKEN_SCOPE_INSUFFICIENT` |
| `spreadsheet <id> is not shared with service account <email>: share it with that account` (or `with the Google user of the refresh token`) | 403 `PERMISSION_DENIED` other than a rate limit |
| `spreadsheet <id> not found: check the spreadsheet ID` | 404 |
| `Google Sheets API is disabled in project <n>: enable sheets.googleapis.com` | 403 with reason `SERVICE_DISABLED` or `accessNotConfigured` |

With `incremental: true`, `check` also reads each spreadsheet's `modifiedTime`. When Drive does not return
it (Drive API disabled, missing `drive.metadata.readonly` scope, file not visible), `check` still succeeds
and logs a `WARN`; discover then offers full refresh only for that spreadsheet.

## Streams

One stream per sheet of type `GRID` with at least one row and a header row; object sheets (charts) and
sheets with an empty first row are skipped (`Skipping sheet "<title>": no headers found in header row`). The
stream name is the sheet title. Streams of `spreadsheet_id` have no namespace; streams of
`additional_spreadsheet_ids` have the spreadsheet ID as namespace, so two spreadsheets may have sheets of
the same title.

A read requests rows `2:1+batch_size`, then the next `batch_size`, up to the sheet's `rowCount`, so empty
rows in the middle of a sheet do not end the read. Rows whose values are all empty are skipped; a row
shorter than the header gives `null` for the missing columns.

A read fails, before emitting any record of the spreadsheet, when the configured schema has a column the
current header no longer has (`column "<name>" is no longer in the header of sheet "<title>"; refresh the
source schema`) or a configured sheet is gone.

Names taken from a column's position (a duplicate's `_<letter>`, `stable`'s `column_<letter>`) follow the
column, not its header: when columns move, such a name can come to hold another column's values. A current
header column named by position that the configured schema lacks is the sign of that: `stable` fails the
read (`column <letter> of sheet "<title>" is named "<name>" by its position and is not in the configured
schema ...; refresh the source schema`), `legacy` keeps reading as 0.3.0 did and logs a `WARN` per stream.
A move that keeps the same set of position names, such as two swapped duplicate columns, cannot be told
apart from an edit of their values.

## Column names

| Header | `legacy` (default) | `stable` |
|---|---|---|
| Empty cell | The header stops there, with a `WARN` naming the cell and the number of header cells right of it that get no column | `column_<letter>`, for every column up to the last non-empty header cell |
| `Name, Name, Age` | `Name_A, Name_B, Age` | `Name_A, Name_B, Age` |
| `First Name, first_name` with `names_conversion` | discover and read fail, naming both columns and `column_naming: stable` | `first_name_A, first_name_B` |
| Non-ASCII (`Prénom, 名前`) | kept (`prénom, 名前` with `names_conversion`) | kept |
| Value right of the last named column | one `WARN` per stream naming the first row and column; the value is not read | the read fails, naming the row and column |
| New column named by position, not in the configured schema | one `WARN` per stream; values may sit under the wrong duplicate name | the read fails |

In `legacy` mode duplicates get `_<letter>` before `names_conversion`; in `stable` mode after it, and a
suffix that would collide with another column gets the letter again. With `incremental: true` a header named
`_spreadsheet_modified_time` fails discover.

## Value rendering and merged cells

`formatted` (default) sends `valueRenderOption=FORMATTED_VALUE`: every value is the string the sheet
displays (`$1,234.50`, `TRUE`), and every column is `["null","string"]`. `unformatted` sends
`UNFORMATTED_VALUE` with `dateTimeRenderOption=FORMATTED_STRING`: numbers and booleans keep their JSON type,
dates and times stay formatted strings (no serial numbers), and columns are
`["null","string","number","boolean"]`. Header rows are always read formatted.

A merged range holds its value only in its top-left cell; the other cells of the range read as empty. A
merged header cell therefore names only its first column.

## Incremental sync

With `incremental: true`, each stream of a spreadsheet whose `modifiedTime` Drive returns offers
`full_refresh` and `incremental`, with the source-defined cursor `_spreadsheet_modified_time` (RFC 3339) in
every record. The state of a stream is:

```json
{"spreadsheet_modified_time": "2026-10-18T09:15:00Z"}
```

A read gets `modifiedTime` once per spreadsheet, before any rows. A stream whose saved time is not older is
skipped and its state emitted again; otherwise the whole sheet is read and the time taken before reading is
saved, so a change during the read is read again next time. `modifiedTime` belongs to the spreadsheet, so
an edit to any sheet re-reads all its incremental streams. Each read is a whole-sheet snapshot: use
`append` with deduplication on a key column, or `overwrite`, in the destination. A read of an incremental
stream fails when Drive does not return `modifiedTime`.

Drive moves `modifiedTime` only when someone (or an API client) edits the spreadsheet. Values that formulas
recalculate on their own, such as `NOW()`, `TODAY()`, `RAND()`, `IMPORTRANGE`, `IMPORTDATA`, `IMPORTXML`,
`GOOGLEFINANCE` or `GOOGLETRANSLATE`, change without moving it, so an incremental stream skips them until the
next edit. Sync sheets like that with full refresh.

## Quotas

The Sheets API allows 60 read requests per minute per user and 300 per project by default. Requests are
sent one at a time, at least `60s / requests_per_minute` apart (1 s by default). A 429, a 403 with reason
`rateLimitExceeded`, `userRateLimitExceeded` or `RATE_LIMIT_EXCEEDED`, a 5xx response or a transport error
is sent again after `Retry-After`, else after a doubling, jittered delay (2 s, 4 s, 8 s, 16 s, at most 1 min),
five times in all; then the read fails naming the quota: `Google Sheets API quota
ReadRequestsPerMinutePerUser (60) is still exceeded after 5 attempts: lower requests_per_minute`.

## Breaking-change switches

Configurations without the keys above keep the stream names, column names and types of 0.3.0.
Behaviour that would change them is opt-in:

- `column_naming: stable` reads columns after an empty header cell and names them `column_<letter>`;
- `value_render: unformatted` changes column types to `["null","string","number","boolean"]`;
- `incremental: true` adds the `_spreadsheet_modified_time` column;
- `additional_spreadsheet_ids` adds namespaced streams.

Defaults that changed without changing the records of a sheet that reads correctly: `batch_size` is 10000
(was 1000000), reads continue past empty batches up to `rowCount` (0.3.0 stopped at the first batch without
values, dropping the rows after it), and requests are paced at `requests_per_minute`.

Changes that make a sync fail where 0.3.0 kept going, so values never land silently under the wrong
column or get dropped (recorded in `meta.json` `breaking_changes` for 0.4.0):

- a column of the configured schema missing from the current header fails the read with `column "<name>" is
  no longer in the header of sheet "<title>"; refresh the source schema` (0.3.0 read the current header and
  left the column out of every record);
- two `legacy` columns whose names collide after `names_conversion` fail discover and read (0.3.0 kept one
  of the two values).

Refreshing the source schema after a header change makes the sync pass again.

## Tests

Tests run against an `httptest` server serving the synthetic fixtures in `testdata/` (see
CONTRIBUTING, "Testing API sources with recorded HTTP"): `sheets/<id>/spreadsheet.json` and
`sheets/<id>/values/<title>.json` (rows sliced by the requested range), `drive/<id>.json`, `token.json`
and `errors/`. The service account key is generated at test time; no credential is stored.
`TestFixturesClean` fails on a private key, a Google token or an email outside `example.com` in `testdata/`.

The large-sheet fixture is generated; regenerate it with:

```bash
go test -run TestGenerateFixtures -update ./...
```
