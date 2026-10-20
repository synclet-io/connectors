# source-freshdesk

[Freshdesk](https://www.freshworks.com/freshdesk/) source for Synclet, a manifest connector run by the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON schema per
stream, no Go. Reads tickets and their conversations, contacts, companies, agents, groups, satisfaction
ratings, time entries and surveys through the [Freshdesk API v2](https://developers.freshdesk.com/api/);
tickets, conversations, contacts and satisfaction ratings incrementally.

```bash
task test CONNECTOR=source-freshdesk                  # no network, no credentials
go run ./cmd/manifest-lint ../source-freshdesk        # from connectors/source-declarative
task build CONNECTOR=source-freshdesk
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `domain` | yes | Host name of the helpdesk, such as `yourcompany.freshdesk.com`, without `https://`. |
| `api_key` | yes | API key of an agent (Profile settings), sent only as basic auth `<api_key>:X`. The agent needs to see the tickets to read; lists such as agents and time entries may need an administrator's key. |
| `start_date` | yes | RFC 3339 time; tickets, contacts and conversations updated, and satisfaction ratings created, before it are not read. |

```json
{"domain": "yourcompany.freshdesk.com", "api_key": "<key>", "start_date": "2026-01-01T00:00:00Z"}
```

`check` requests the first page of `GET /api/v2/tickets`.

## Streams

| Stream | Route | Sync modes | Primary key | Cursor |
|---|---|---|---|---|
| `tickets` | `tickets?updated_since=&order_by=updated_at&order_type=asc` | full refresh, incremental | `id` | `updated_at` |
| `conversations` | `tickets/<ticket>/conversations` | full refresh, incremental | `id` | `updated_at` |
| `contacts` | `contacts?_updated_since=` | full refresh, incremental | `id` | `updated_at` |
| `companies` | `companies` | full refresh | `id` | |
| `agents` | `agents` | full refresh | `id` | |
| `groups` | `groups` | full refresh | `id` | |
| `satisfaction_ratings` | `surveys/satisfaction_ratings?created_since=` | full refresh, incremental | `id` | `created_at` |
| `time_entries` | `time_entries` | full refresh | `id` | |
| `surveys` | `surveys` | full refresh | `id` | |

Every list is paged by the `Link` header with `per_page=100`. `conversations` runs once per ticket that
`tickets` lists; a ticket deleted in between answers 404 and is skipped. Records are passed through as
Freshdesk sends them; tickets come without `description` and `description_text` on accounts created after
2018-11-30 (the list endpoint's `include=description` costs extra API credits per ticket).

`tickets` lists what the `tickets` endpoint lists: deleted and spam tickets are left out, as Freshdesk does
without a filter.

## Incremental sync

`tickets`, `contacts` and `satisfaction_ratings` keep one cursor each, the greatest value read:

```json
{"updated_at": "2026-10-01T10:00:00Z"}
```

The next read sends the filter with `<cursor minus 1h>` and drops older records; the boundary records are
read again and deduplicated by `id`. `conversations` is read for every ticket updated since the older of the `tickets` and `conversations`
cursors (a new reply updates its ticket) and keeps its own `updated_at` cursor. `satisfaction_ratings` filters on
creation time, so a rating edited after it was read is picked up only by a full refresh. A full refresh
reads from `start_date` and saves no state.

## Errors and rate limits

- 401 (bad key) and 403 (no access) fail `check` and every read as configuration errors with Freshdesk's
  `message`.
- 429 waits for `Retry-After` (60s when absent), up to five times. The per-minute budget that
  `X-RateLimit-Remaining` reports is not paced against: the engine sends one request at a time and waits
  only when Freshdesk answers 429. Reading conversations costs one request per ticket, so a large first
  sync of `conversations` takes about as many minutes as the plan's per-minute limit divides the ticket count.
- 5xx responses and network errors are retried three times with a doubling delay from 1s.

Known limits:

- Freshdesk serves at most 300 pages (30,000 records) of one `tickets` listing. A first sync with more
  tickets updated since `start_date` fails past that page; set a later `start_date` and move it back in steps,
  or let a Go connector restart the listing from the last `updated_at`. The API has no `updated_before`
  filter, so `incremental.window` cannot split the listing.
- Not read in this version: roles, skills, products, ticket fields, business hours, SLA policies, email
  configs, canned responses, discussions and solutions.

## Tests

`manifest_test.go` runs the engine against hand-written synthetic fixtures in `testdata/` (two `Link`-paged
pages per list, three tickets, one ticket's conversations on two pages, a ticket without conversations)
without network or credentials: the SDK conformance suite for full refresh and for each incremental stream,
`check` with 401 and 403, a one-time 429, conversations per ticket, the ticket order, and the filters sent on
a first and on a resumed read.
