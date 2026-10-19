# source-chargebee

[Chargebee](https://www.chargebee.com) source, a manifest connector on the
[source-declarative](../source-declarative) engine: `manifest.yaml` and `schemas/` baked into the engine's
image. Reads the [API v2](https://apidocs.chargebee.com/docs/api) lists of a site, deleted objects
included. Every stream supports full refresh and incremental on `updated_at`.

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

| Stream | Endpoint | Records |
|---|---|---|
| `customers` | `GET /customers` | `$.list[*].customer` |
| `subscriptions` | `GET /subscriptions` | `$.list[*].subscription` |
| `invoices` | `GET /invoices` | `$.list[*].invoice` |
| `credit_notes` | `GET /credit_notes` | `$.list[*].credit_note` |
| `transactions` | `GET /transactions` | `$.list[*].transaction` |
| `items` | `GET /items` | `$.list[*].item` |
| `item_prices` | `GET /item_prices` | `$.list[*].item_price` |
| `coupons` | `GET /coupons` | `$.list[*].coupon` |

Every request carries `sort_by[asc]=updated_at`, `include_deleted=true` and `updated_at[after]=<since>` in
Unix seconds; pages hold `limit=100` objects and follow the response's `next_offset` as `offset` until it is
absent. The primary key is `id`; a deleted object arrives with `deleted: true`. Records pass through as the
API sends them; each schema declares the common fields and allows the rest.

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
`next_offset`, and a resumed read's `updated_at[after]` and deleted objects. Fixtures are synthetic,
in the shape of a free Chargebee test site's responses; re-record them from such a site, never from a live
one.
