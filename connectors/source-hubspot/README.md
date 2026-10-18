# source-hubspot

[HubSpot](https://www.hubspot.com/) CRM source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads contacts, companies, deals, tickets,
line items, products and custom objects with every property, full refresh or incrementally through the CRM
search API; owners and the deal and ticket pipelines; and the associations between any two objects,
standard or custom, with their labels.

```bash
task test CONNECTOR=source-hubspot     # no network, no credentials
task build CONNECTOR=source-hubspot
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-hubspot:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `access_token` | yes | Access token of a HubSpot private app (Settings > Integrations > Private Apps). Sent only as `Authorization: Bearer <access_token>`, never in a URL |
| `start_date` | no | Earliest last modification the first incremental sync of an object reads: RFC 3339 time or `YYYY-MM-DD`, UTC. Empty reads everything |

```json
{"access_token": "<your private app token>", "start_date": "2026-01-01"}
```

This version authenticates with a private app token only; OAuth apps are not supported.

### Scopes

Give the private app the read scope of every stream to sync:

| Stream | Scope |
|---|---|
| `contacts` | `crm.objects.contacts.read` |
| `companies` | `crm.objects.companies.read` |
| `deals`, `deal_pipelines` | `crm.objects.deals.read` |
| `tickets`, `ticket_pipelines` | `tickets` |
| `line_items` | `crm.objects.line_items.read` |
| `products` | `crm.objects.products.read` (`e-commerce` on older portals) |
| `owners` | `crm.objects.owners.read` |
| custom objects | `crm.schemas.custom.read` and `crm.objects.custom.read` |
| `associations_<a>_<b>` | the scopes of `a` and `b` |

### Check

`check` requests `GET /crm/v3/objects/contacts?limit=1` first, then every stream's endpoint: each standard
object, owners, both pipelines, `GET /crm/v3/schemas` and each custom object it lists. It fails with:

| Cause | Error |
|---|---|
| Wrong or revoked token (401) | `invalid private-app token (HTTP 401): <HubSpot's message>` |
| A 403 `MISSING_SCOPES` on any endpoint | `missing scope for stream deals: crm.objects.deals.read`, one clause per stream, joined by `; ` |
| The schemas API refused (403 `MISSING_SCOPES`) | `missing scope for custom objects: crm.schemas.custom.read` |

The scopes named are HubSpot's `requiredGranularScopes`. A missing scope fails the check; it never hides
the stream.

## Streams

| Stream | Route | Sync modes | Cursor | Primary key |
|---|---|---|---|---|
| `contacts`, `companies`, `deals`, `tickets`, `line_items`, `products` | `/crm/v3/objects/<object>` | full refresh, incremental | `updatedAt` | `id` |
| one per custom object, named after its schema `name` | `/crm/v3/objects/<objectTypeId>` | full refresh, incremental | `updatedAt` | `id` |
| `owners` | `/crm/v3/owners` | full refresh | | `id` |
| `deal_pipelines` | `/crm/v3/pipelines/deals` | full refresh | | `id` |
| `ticket_pipelines` | `/crm/v3/pipelines/tickets` | full refresh | | `id` |
| `associations_<a>_<b>` | `/crm/v4/associations/<a>/<b>/batch/read` | full refresh | | `from_id`, `to_id`, `type_id` |

Deal and ticket pipelines are two streams because their ids collide: both have a pipeline `default`.
Archived custom object schemas are left out.

### Object records

A record carries HubSpot's top-level `id`, `createdAt`, `updatedAt` and `archived`, plus one
`properties_<name>` field per property, flattened so that a property never collides with a top-level field.
`discover` builds each object's schema from the properties API (`GET /crm/v3/properties/<object>`), so
custom properties and custom objects appear without a code change; a property added later appears after the
next discover. HubSpot sends every property value as a string; the connector converts it to its type:

| HubSpot type | JSON schema | Value |
|---|---|---|
| `string`, `phone_number`, `enumeration`, `json`, `object_coordinates`, any other | `["null","string"]` | as sent; `""` stays `""`; a multi-select enumeration stays one `;`-separated string |
| `number` | `["null","number"]` | the digits as sent, unrounded; `""` is null |
| `bool` | `["null","boolean"]` | `"true"` or `"false"`; `""` is null |
| `datetime` | `["null","string"]`, format `date-time` | RFC 3339 in UTC; Unix milliseconds are converted; `""` is null |
| `date` | `["null","string"]`, format `date` | `YYYY-MM-DD`; a midnight UTC datetime or Unix milliseconds become the day; `""` is null |

A value that does not parse as its type fails the read with the stream, the record id and the property; no
value is dropped or nulled to make a record pass.

### Associations

Associations are separate streams, one per unordered pair of objects (standard or custom) for which
`GET /crm/v4/associations/<a>/<b>/labels` answers 200; pairs it answers 400 or 404 for are left out. A
row is one association type between two records:

| Field | Meaning |
|---|---|
| `from_id` | id of the `a` record |
| `to_id` | id of the `b` record |
| `type_id` | association type id |
| `category` | `HUBSPOT_DEFINED`, `USER_DEFINED` or `INTEGRATOR_DEFINED` |
| `label` | the label, null for the unlabeled default association |

Why rows and not id arrays on the object records: a pair of records can carry several labels at once
(one row each); the search API that incremental reads use returns no associations; and changing an
association does not reliably move a record's modification date, so arrays on incremental records would go
stale without anyone noticing. Association streams are full refresh: use `overwrite` to drop removed
associations.

The read lists the ids of `a`, sends each page of 100 to the v4 batch read, and follows a record's
`paging.next.after` through `GET /crm/v4/objects/<a>/<id>/associations/<b>`. A 207 error with
`subCategory` `crm.associations.NO_ASSOCIATIONS_FOUND` means the record has none; any other error entry
fails the read.

### Full refresh

Object streams list the ids with `GET /crm/v3/objects/<object>?limit=100&archived=false`, following
`paging.next.after`, and read each page of ids with all declared properties through
`POST /crm/v3/objects/<object>/batch/read`: hundreds of property names do not fit a GET URL. A record
deleted between the list and the batch read comes back as an `OBJECT_NOT_FOUND` error; the read logs a
warning with the stream and the count. Any other batch error fails the read.

## Incremental sync

Object streams read `POST /crm/v3/objects/<object>/search` with the filters `<modified> GTE from`,
`<modified> LT to` and `hs_object_id GT <last id>`, sorted by `hs_object_id` ascending, 200 per page.
`<modified>` is `lastmodifieddate` for contacts and `hs_lastmodifieddate` for every other object. Pages
follow each other by the last id, never by the `after` offset, so records with the same modification time
are neither skipped nor repeated.

`from` is the saved cursor, or `start_date`; `to` is one minute before now, since the search index trails
writes. One search query returns at most 10,000 results: when the first page of a window reports a `total`
above that, the window is halved and each half is read in turn, the earlier first. A window of one
millisecond that still holds more than 10,000 records fails the read, naming the stream and the window. After
each window the connector saves its end:

```json
{"cursor": "2026-10-18T11:59:00.000Z"}
```

A record modified during the read moves past `to` and comes with the next sync. A full refresh of an object
stream uses the list and batch read endpoints and saves no state.

### Archived and deleted records

Archived (deleted) records are never emitted: the search API leaves them out and the lists request
`archived=false`. Deletions therefore reach the destination only through a full refresh with `overwrite`.

## Rate limits

Requests go one at a time. General requests are spaced by `X-HubSpot-RateLimit-Interval-Milliseconds`
divided by `X-HubSpot-RateLimit-Max` of the last response (10 s / 100 until a response says otherwise), and
search requests 250 ms apart. A 429 waits its `Retry-After` (10 s without one) and repeats the request, up
to 5 times; a 429 with `policyName` `DAILY` fails at once, since the account's daily limit resets only the
next day. A 5xx response or a network error is retried three times, waiting 1 s, 2 s and 4 s. 401 and 403
are configuration errors and are not retried.

## Tests

Tests replay HTTP from hand-written fixtures; they need no network and no credentials (see
[CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)).

- **Layout:** `testdata/` mirrors the API paths. `GET /<p>` is answered by `testdata/<p>.json`, with
  `after=X` by `testdata/<p>/after-X.json`; `POST <p>/search` by `testdata/<p>/search/<GTE value>-<hs_object_id GT value>.json`;
  `POST <p>/batch/read` by `testdata/<p>/batch/read/<first input id>.json`. A pair's labels without a fixture
  answer 404. Error bodies live in `testdata/errors/`. All values are synthetic: `example.com` emails,
  made-up ids, the token `test-private-app-token`. The fixtures can be re-recorded from a free HubSpot
  developer test account.
- **Helper:** `fixtures_test.go` holds `fixtureServer`, which serves `testdata/`, answers 401 to any
  request without the test token, fails the test when a request carries a token in its URL, lets a test
  replace the answer to one fixture, and records the requests with their bodies. `testSource` points a
  `Source` at it through its unexported fields (`baseURL`, `now` fixed at 2026-10-18 12:00 UTC, `client`,
  `retryDelay`, `wait`), which are zero in `main`.
- `TestDiscover` covers the schemas and the type mapping, `TestCheck` each check failure, `TestFullRefresh`
  the list and batch reads and value conversion, `TestIncremental` the search windows and state,
  `TestAssociations` the v4 reads, `TestRateLimit` pacing and retries, `TestConformance` the SDK
  conformance suite for full refresh and incremental.
