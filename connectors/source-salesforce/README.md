# source-salesforce

[Salesforce](https://www.salesforce.com/) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads every queryable object of an org
through SOQL queries of the REST API (`v62.0`), with a schema built from each object's describe. Objects
with `SystemModstamp` or `LastModifiedDate` read incrementally; the others read as full refresh.

```bash
task test CONNECTOR=source-salesforce     # no network, no credentials
task build CONNECTOR=source-salesforce
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-salesforce:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `client_id` | yes | Consumer key of the connected app |
| `client_secret` | yes | Consumer secret of the connected app |
| `refresh_token` | yes | OAuth refresh token of the integration user for that connected app |
| `instance_url` | yes | `https` URL of the org, as the token response's `instance_url` gives it, such as `https://acme.my.salesforce.com` |
| `is_sandbox` | no | `true` for a sandbox: tokens come from `test.salesforce.com` instead of `login.salesforce.com` |
| `start_date` | no | Earliest `SystemModstamp` (or `LastModifiedDate`) read, by full refresh and incremental reads alike: RFC 3339 time or `YYYY-MM-DD`, UTC. Empty reads every record |
| `streams_include` | no | Object name patterns in [`path.Match`](https://pkg.go.dev/path#Match) syntax (`Account`, `*__c`); empty includes every object |
| `streams_exclude` | no | Patterns of objects left out after `streams_include` |
| `api_usage_threshold_percent` | no | Share of the org's daily API request allocation, 1 to 100, at which a read stops; default 90 |

```json
{"client_id": "<consumer key>", "client_secret": "<consumer secret>", "refresh_token": "<refresh token>",
 "instance_url": "https://acme.my.salesforce.com", "start_date": "2026-01-01"}
```

### Authentication

The connector uses a connected app's refresh token (OAuth web server flow, scopes `api` and
`refresh_token`). It posts the `refresh_token` grant to `<login or test host>/services/oauth2/token`, keeps
the access token until the API rejects it with 401 `INVALID_SESSION_ID`, then gets a new one and repeats
the rejected request once. Credentials travel only in the token request's form body and the
`Authorization` header, never in a URL.

Set the connected app's refresh token policy to **Refresh token is valid until revoked**, so the token
does not rotate. If the token endpoint does return a new refresh token, `read` emits the configuration
with it as a CONTROL message, which Synclet saves; `check` and `discover` cannot save it and log a
warning, and the old token may stop working once the new one is issued.

### Check

`check` gets an access token, compares the token's `instance_url` with the configured one and lists the
org's objects (`GET /sobjects`). It fails with:

| Cause | Message contains |
|---|---|
| Wrong consumer key or secret (`invalid_client`, `invalid_client_id`) | `client_id or client_secret is invalid` |
| Refresh token rejected (`invalid_grant`) | `refresh_token is invalid, expired or revoked, or belongs to a sandbox org while is_sandbox is false` (or `a production org while is_sandbox is true`) |
| Token of another org | `instance_url <configured> does not match the org of the refresh token (<token's instance_url>)` |
| 403 `API_DISABLED_FOR_ORG` or `API_CURRENTLY_DISABLED` | `API access is not enabled for this user or org` |

## Streams

`discover` lists the org's objects (`GET /sobjects`), keeps those that are `queryable`, are not on the
denylist below and that `streams_include` / `streams_exclude` keep, and describes each one
(`GET /sobjects/<name>/describe`), one after another. Every stream is named after its object, with
primary key `Id`. The cursor is `SystemModstamp`, or `LastModifiedDate` where an object lacks it; an object
with neither supports full refresh only.

Denylist: objects that reject a query without a filter on a specific record or field — `AppTabMember`,
`CollaborationGroupRecord`, `ColorDefinition`, `ContentDocumentLink`, `ContentFolderItem`,
`ContentFolderMember`, `DataStatistics`, `DatacloudDandBCompany`, `EntityParticle`, `FieldDefinition`,
`FieldHistoryArchive`, `FlexQueueItem`, `FlowVariableView`, `FlowVersionView`, `IconDefinition`,
`IdeaComment`, `NetworkUserHistoryRecent`, `OwnerChangeOptionInfo`, `PicklistValueInfo`, `PlatformAction`,
`RelationshipDomain`, `RelationshipInfo`, `SearchLayout`, `SiteDetail`, `UserEntityAccess`,
`UserFieldAccess`, `Vote`. A pattern in `streams_include` does not bring them back.

### Types

Every property is nullable.

| Describe type | JSON schema | Values |
|---|---|---|
| `id`, `reference`, `string`, `picklist`, `multipicklist` (`;`-separated), `textarea`, `email`, `phone`, `url`, `combobox`, `encryptedstring` (masked as the user sees it) | `string` | as the API sends them |
| `boolean` | `boolean` | |
| `int`, `long` | `integer` | |
| `double`, `currency`, `percent` | `number` | the API's exact literal, such as `1250000.50`; never rounded through a float |
| `date` | `string`, `format: date` | `2026-10-01` |
| `datetime` | `string`, `format: date-time`, `timestamp_with_timezone` | RFC 3339 in UTC with milliseconds: the API's `2026-10-01T12:00:00.000+0000` becomes `2026-10-01T12:00:00.000Z` |
| `time` | `string`, `format: time` | `09:30:00.000Z` |
| `address`, `location` (compound fields) | `object` | the API's object; the component fields (`BillingCity`, ...) are columns of their own too |
| `anyType` and any type not listed | untyped | as the API sends them |
| `base64` | left out | the API returns a URL to the blob, not its content, and reading blobs takes a request per record |

## Reads

A stream is one SOQL query: `SELECT Id, <every schema field, sorted> FROM <Object>`, with
`WHERE <cursor> >= <start_date>` when `start_date` is set. The connector follows `nextRecordsUrl` page by
page (`queryMore`) and strips each record's `attributes`.

**Deleted records.** An object with an `IsDeleted` field is read through `/queryAll`, so records in the
recycle bin arrive with `IsDeleted: true`, in full refresh and incremental reads alike. A record purged
from the recycle bin (after 15 days, or emptied by hand) is not seen.

**Incremental.** The query adds `ORDER BY <cursor>, Id`; after every page the state is
`{"cursor": "<cursor of the page's last record>", "id": "<its Id>"}`. The next read continues with
`WHERE (<cursor> > :cursor OR (<cursor> = :cursor AND Id > :id))`, so records sharing a timestamp across a
page boundary are neither skipped nor repeated.

**Long SELECTs.** Salesforce rejects a REST URL longer than 16 384 characters. When the URL-encoded query
of all fields is longer than 14 000 characters, each page is read as `SELECT Id, <cursor>` first, then
the other fields in chunks with `WHERE Id IN (...)`, in groups of ids and fields that each fit into one
query; the chunks are merged by `Id` and emitted in page order. A record missing from a chunk (purged
between the queries) fails the read with the object and `Id`; no record is emitted incomplete.

**API limits.** Every API response's `Sforce-Limit-Info: api-usage=<used>/<allocation>` is read. Once
usage reaches `api_usage_threshold_percent` of the allocation, the read emits the page it has and that
page's state, then fails with a message naming the usage and the setting; the next sync continues from
the saved state. A 403 `REQUEST_LIMIT_EXCEEDED` fails at once, naming the daily limit.

**Retries.** A 5xx response, 429 or a transport error is retried up to 5 times with a doubling delay
from 2 seconds.

## Tests

Tests run against an `httptest` TLS server that plays the org and both OAuth hosts from hand-written,
synthetic JSON in `testdata/` (made-up org and record ids, `example.com` addresses); no test reaches the
network. The fixtures can be re-recorded from a free Developer Edition org: keep the bodies only and
replace every id, name, address and amount.

## Follow-ups

- Bulk API 2.0 for large objects, which costs far fewer API requests than `queryMore`.
- JWT bearer flow for server-to-server authentication without a refresh token.
