# source-stripe

[Stripe](https://stripe.com) source: a manifest connector run by the
[source-declarative](../source-declarative) engine. Reads customers, charges, invoices, subscriptions,
payment intents, refunds, products, prices, balance transactions and events, every stream incrementally
by `created`.

```bash
task test CONNECTOR=source-stripe                                 # no network, no credentials
(cd ../source-declarative && go run ./cmd/manifest-lint ../source-stripe)
task build CONNECTOR=source-stripe
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-stripe:dev check --config /data/config.json
```

The image is `FROM` the pinned `source-declarative` image with `manifest.yaml` and `schemas/` baked in.

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `api_key` | yes | Secret key (`sk_...`) or restricted key (`rk_...`). Sent only as `Authorization: Bearer <key>`, never in a URL. A restricted key needs read access to every selected stream |
| `start_date` | yes | RFC 3339 time; objects and events created before it are not read |

```json
{"api_key": "<your secret or restricted key>", "start_date": "2026-01-01T00:00:00Z"}
```

Every request carries `Stripe-Version: 2025-08-27.basil`, so the shape of the records does not follow the
account's default API version. `check` lists one customer.

## Streams

| Stream | Route | Sync modes | Primary key | Cursor |
|---|---|---|---|---|
| `customers` | `GET /v1/customers` | full refresh, incremental | `id` | `created` |
| `charges` | `GET /v1/charges` | full refresh, incremental | `id` | `created` |
| `invoices` | `GET /v1/invoices` | full refresh, incremental | `id` | `created` |
| `subscriptions` | `GET /v1/subscriptions?status=all` | full refresh, incremental | `id` | `created` |
| `payment_intents` | `GET /v1/payment_intents` | full refresh, incremental | `id` | `created` |
| `refunds` | `GET /v1/refunds` | full refresh, incremental | `id` | `created` |
| `products` | `GET /v1/products` | full refresh, incremental | `id` | `created` |
| `prices` | `GET /v1/prices` | full refresh, incremental | `id` | `created` |
| `balance_transactions` | `GET /v1/balance_transactions` | full refresh, incremental | `id` | `created` |
| `events` | `GET /v1/events` | full refresh, incremental | `id` | `created` |

Records are passed through as Stripe sends them; each schema declares the common fields and allows the
rest. Lists are read 100 objects a page, each page `starting_after` the last record's `id`, until
`has_more` is false. Nested lists (subscription items, invoice lines) are read only as far as Stripe
embeds them in the parent object.

## Updates and the events stream

Stripe lists filter on `created` only, so the object streams catch new objects only: an object changed
after it was synced (an invoice paid, a customer's email edited, a subscription canceled) is not read
again by an incremental sync. The `events` stream carries those changes: every `*.updated` event holds
the object as it is after the change in `data.object` and the changed fields' old values in
`data.previous_attributes`. Sync `events` incrementally next to the object streams to follow updates,
or run a full refresh of an object stream to re-read its current state.

Stripe keeps events for 30 days: an `events` sync that pauses longer loses the events in between, and a
`start_date` older than 30 days reads only the last 30 days of events.

## Incremental sync

Every stream sends `created[gte]` and saves the greatest `created` it read in STREAM state:

```json
{"created": 1767330000}
```

The next read starts at the saved `created` minus one hour (never before `start_date`), so an object that
becomes visible in a list a little after its `created` time is not missed; the overlap is deduplicated
by `id` downstream. Stripe lists newest first, so the state is saved after a stream's last page; a read
that stops halfway starts that stream over. A full refresh reads from `start_date` and saves no state.

## Errors

Stripe answers errors with `{"error": {"type": ..., "message": ...}}`; the message is appended to every
failure, after `(HTTP <status>)`.

| Status | Meaning | Result |
|---|---|---|
| 401 | invalid or revoked key | configuration error, `check` fails |
| 403 | restricted key without permission for the resource | configuration error, `check` fails |
| 429 | rate limited | waits for `Retry-After` (60s without it), up to 5 times |
| 500, 502, 503, 504, network errors | transient | tried 3 times in all, waiting 1s, then 2s |

Any other status fails the read with the request and Stripe's message.

## Tests

Tests replay HTTP from synthetic fixtures in `testdata/` (made-up ids, `example.com` emails, invented
amounts); they need no network and no credentials. `manifest_test.go` runs the engine's
`declarativetest` conformance suite for full refresh and once per incremental stream, plus checks of
the 401 and 403 messages, of `starting_after` paging over two pages per stream, of the resumed
`created[gte]` and of the update events. The fixtures can be re-recorded in Stripe test mode; scrub
them as [CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http) says.
