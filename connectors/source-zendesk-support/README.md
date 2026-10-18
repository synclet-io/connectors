# source-zendesk-support

[Zendesk Support](https://www.zendesk.com/service/) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads tickets, users and ticket events
incrementally from the incremental export API, and organizations, groups, ticket fields and satisfaction
ratings as full refresh lists.

```bash
task test CONNECTOR=source-zendesk-support     # no network, no credentials
task build CONNECTOR=source-zendesk-support
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-zendesk-support:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `subdomain` | yes | Account name: `acme` for `https://acme.zendesk.com`. Letters, digits and hyphens, lowercased before use; a full host or URL is rejected with the name to use instead |
| `email` | yes | Email of the admin the API token belongs to |
| `api_token` | yes | API token (Admin Center > Apps and integrations > Zendesk API). Sent only as `Authorization: Basic base64("<email>/token:<api_token>")`, never in a URL |
| `start_date` | yes | Earliest update the first incremental read requests: RFC 3339 time or `YYYY-MM-DD`, UTC |

```json
{"subdomain": "acme", "email": "admin@example.com", "api_token": "<your token>", "start_date": "2026-01-01"}
```

### Authentication

This version authenticates with an API token only. OAuth is not supported yet: Zendesk rotates the
refresh token on every refresh, and the connector has no way yet to persist the new refresh token back
into its configuration, so an OAuth connection would stop working after the first refresh. A plain
OAuth access token without refresh would work but expires; it will be added together with refresh
token persistence.

### Check

`check` requests `GET /api/v2/users/me.json` and fails with:

| Cause | Error starts with |
|---|---|
| The subdomain has no Zendesk account (404 or unknown host) | `wrong subdomain "<subdomain>"` |
| Wrong email or token (401) | `invalid email or API token` |
| The user is not an admin; incremental exports of tickets and users need one | `missing admin role for user export (role "<role>")` |

## Streams

| Stream | Route | Sync modes | Cursor | Primary key |
|---|---|---|---|---|
| `tickets` | `incremental/tickets/cursor.json` | full refresh, incremental | `updated_at` | `id` |
| `users` | `incremental/users/cursor.json` | full refresh, incremental | `updated_at` | `id` |
| `ticket_events` | `incremental/ticket_events.json` | full refresh, incremental | `created_at` | `id` |
| `organizations` | `organizations.json` | full refresh | | `id` |
| `groups` | `groups.json` | full refresh | | `id` |
| `ticket_fields` | `ticket_fields.json` | full refresh | | `id` |
| `satisfaction_ratings` | `satisfaction_ratings.json` | full refresh | | `id` |

Schemas list the fields of the Zendesk API reference; a record carries only those fields, so a field
Zendesk adds later shows up once the schema lists it. Custom fields are in `custom_fields` (tickets),
`user_fields` (users) and `organization_fields` (organizations). Nested objects are typed `object` or
`array` without a schema of their own.

The full refresh lists use cursor pagination: `page[size]=100`, then `page[after]=<meta.after_cursor>`
while `meta.has_more`.

Not read in this version: `ticket_comments`, which has no export endpoint and needs one request per
ticket.

## Incremental sync

`tickets` and `users` use the cursor-based export. The first request carries `start_time` (from
`start_date`), every later one `cursor=<after_cursor>` of the page before, until `end_of_stream`. After
each page the connector saves its `after_cursor` in STREAM state, and the next sync starts from it:

```json
{"cursor": "MTU3NjYxMzUzOS4wfHw0Njd8"}
```

`ticket_events` uses the time-based export: each response's `end_time` is the next request's
`start_time`, until `end_of_stream`. Zendesk returns every item of one second in a single page, so a
page with `end_of_stream` false whose `end_time` equals its `start_time` is not expected; if one
comes, the connector moves on one second (`start_time + 1`) and logs a warning. The state is the next `start_time`, a Unix time; an empty state (`{}`) starts from
`start_date`:

```json
{"start_time": 1791244800}
```

Zendesk rejects a `start_time` less than a minute old, so `start_time` is never later than one minute
before now. A saved state takes priority over `start_date`. A full refresh of an incremental stream
reads everything from `start_date` and saves no state.

## Rate limits

Zendesk limits incremental exports to 10 requests per minute, on top of the plan's limit (200 to 2,500
requests per minute). A read waits so that export requests are at least 6 seconds apart, across all
streams of the read. Any 429 waits for its `Retry-After` (60 seconds without one) and repeats the
request, up to 5 times; then the read fails naming the path. A 5xx response or a network error is
retried three times, waiting 1s, 2s and 4s. 401 and 403 are configuration errors and are not retried.

## Tests

Tests replay HTTP from hand-written fixtures; they need no network and no credentials (see
[CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)).

- **Layout:** `GET /api/v2/<p>.json` is answered by `testdata/<p>.json`; with `cursor=X` by
  `testdata/<p>/cursor-X.json`; with `page[after]=X` by `testdata/<p>/after-X.json`; `ticket_events`
  with `start_time=T` by `testdata/incremental/ticket_events/T.json`. Error bodies live in
  `testdata/errors/`. All values are synthetic.
- **Helper:** `fixtures_test.go` holds `fixtureServer`, which serves `testdata/`, answers 401 to any
  request without the Basic header of `tester@example.com` / `test-api-token`, fails the test when a
  request carries a token in its URL, lets a test override the status and file of one path, and records
  the requests. `testSource` points a `Source` at it through its unexported fields (`baseURL`, `now`
  fixed at 2026-10-18 12:00 UTC, `client`, `retryDelay`, `exportInterval`, `wait`), which are zero in
  `main`.
- `TestReadTickets` and `TestResume` cover the export requests and state, `TestRateLimit` the 429 waits
  and export spacing, `TestCheck` each check failure, `TestConformance` the SDK conformance suite for
  full refresh and incremental.
