# source-chargebee

[Chargebee](https://www.chargebee.com) source, a manifest connector on the
[source-declarative](../source-declarative) engine: `manifest.yaml` and `schemas/` baked into the engine's
image. Reads the [API v2](https://apidocs.chargebee.com/docs/api) lists of a site. Every stream supports
full refresh and incremental on `updated_at`.

```bash
task test CONNECTOR=source-chargebee      # no network, no credentials
task build CONNECTOR=source-chargebee
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-chargebee:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `site` | yes | The site name, the subdomain of `<site>.chargebee.com` (a test site is `<name>-test`). Lowercase letters, digits and `-` |
| `site_api_key` | yes | An API key of the site (Settings > Configure Chargebee > API keys), full-access or read-only. Sent only as the Basic auth username with an empty password, never in a URL |
| `start_date` | yes | RFC 3339 time; objects last updated before it are not read |

```json
{"site": "example-test", "site_api_key": "<your api key>", "start_date": "2026-01-01T00:00:00Z"}
```

## Streams

| Stream | Endpoint | Records | `sort_by[asc]` | Deleted objects |
|---|---|---|---|---|
| `customers` | `GET /customers` | `$.list[*].customer` | `updated_at` | read, `deleted: true` |
| `subscriptions` | `GET /subscriptions` | `$.list[*].subscription` | `updated_at` | read, `deleted: true` |
| `invoices` | `GET /invoices` | `$.list[*].invoice` | `updated_at` | read, `deleted: true` |
| `credit_notes` | `GET /credit_notes` | `$.list[*].credit_note` | `date` | read, `deleted: true` |
| `transactions` | `GET /transactions` | `$.list[*].transaction` | `updated_at` | read, `deleted: true` |
| `items` | `GET /items` | `$.list[*].item` | `updated_at` | not read |
| `item_prices` | `GET /item_prices` | `$.list[*].item_price` | `updated_at` | not read |
| `coupons` | `GET /coupons` | `$.list[*].coupon` | `created_at` | read, `status: "deleted"` |

Each stream sends only the parameters its list endpoint documents: `updated_at[after]=<since>` in Unix
seconds, the `sort_by[asc]` above, and `include_deleted=true` on the five streams whose endpoint takes it.
Pages hold `limit=100` objects and follow the response's `next_offset` as `offset` until it is absent. The
primary key is `id`. Records pass through as the API sends them; each schema declares the common fields and
allows the rest.

Deletions of items and item prices cannot be read: their list endpoints take no `include_deleted`, so a
deleted item or item price stops appearing and its last record stays downstream as it was. A deleted coupon
arrives with `status: "deleted"`.

The sort is for stable paging, not for the cursor. The engine saves the greatest `updated_at` it saw only
when a stream's last page is read, and drops records older than `since` itself, so records in any order give
the same state. Offset paging over a stable order (`created_at`, `date` or `updated_at`) neither skips nor
repeats objects that do not change during the read. An object updated during the read may be skipped by
that read if its old position was already passed; its new `updated_at` is later than the read's start,
and the next read starts at the greatest `updated_at` seen minus one hour, so a read shorter than an hour
leaves it for the next one.

Incremental streams save the greatest `updated_at` seen (`{"updated_at": 1767400000}`) and resume from it
minus one hour, never earlier than `start_date`; the overlap is deduplicated on `id` downstream. Full
refresh reads from `start_date`.

Not covered: the Product Catalog 1.0 lists (`plans`, `addons`) and the other lists of the API (`orders`,
`events`, `quotes`, `payment_sources`, and so on). The `items` and `item_prices` streams need
a site on Product Catalog 2.0.

## Check

`check` requests one customer (`GET /customers?limit=1`) and fails with the API's message:

| Cause | Error contains |
|---|---|
| Wrong or revoked key, or a key of another site (401) | `Sorry, authentication failed. Invalid api key` |
| The key lacks the permission (403) | `The API key does not have the permission to access this resource` |

A read fails with the same messages on a 401 or 403. A 429 waits for `Retry-After` (default 60 s) and
repeats up to five times.

## Tests

`manifest_test.go` runs `declarativetest` against fixtures in `testdata/`: the SDK conformance suite for
full refresh and per stream incrementally, the 401 and 403 check messages, two pages per stream through
`next_offset`, each stream's exact query parameters, and a resumed read's `updated_at[after]` and deleted
objects. Fixtures are synthetic,
in the shape of a free Chargebee test site's responses; re-record them from such a site, never from a live
one.
