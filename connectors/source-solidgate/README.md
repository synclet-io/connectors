# source-solidgate

[Solidgate](https://solidgate.com) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads card and APM orders,
subscriptions and their invoices, chargebacks, PayPal disputes, card fraud alerts, products and
financial entries from one or more merchant accounts, the reports incrementally.

```bash
task test CONNECTOR=source-solidgate     # no network, no credentials
task build CONNECTOR=source-solidgate
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-solidgate:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `accounts` | yes | Merchant accounts to read, at least one. Each has `name`, `public_key` (`api_pk_...`) and `secret_key` (`api_sk_...`) |
| `accounts[].name` | yes | Unique name of the account; every record carries it in `solid_account` |
| `start_date` | no | First UTC day to read, `YYYY-MM-DD`. Empty reads the last two years |
| `lookback_days` | no | Days before the last day read that an incremental read requests again. Default 2 |

```json
{
  "accounts": [
    {"name": "main", "public_key": "api_pk_...", "secret_key": "api_sk_..."},
    {"name": "eu", "public_key": "api_pk_...", "secret_key": "api_sk_..."}
  ],
  "start_date": "2026-01-01"
}
```

Every request carries two headers: `merchant` with the public key and `signature`, which is
Base64 of the hex HMAC-SHA512 of `public_key + body + public_key` keyed by the secret key (an empty
body for a GET). The secret key never leaves the connector.

`check` asks every account's card orders report for one order of the last hour, so a wrong key pair
or a blocked host fails `check`, not the first sync.

## Streams

| Stream | API | Sync modes | Primary key | Cursor |
|---|---|---|---|---|
| `subscriptions` | `POST reports/subscriptions` | full refresh, incremental | `id`, `solid_account` | `updated_at` |
| `subscription_invoices` | `POST reports/subscriptions` (invoices of each subscription) | full refresh, incremental | `id`, `solid_account` | `updated_at` |
| `products` | `GET subscriptions/products` | full refresh | `id`, `solid_account` | |
| `card_orders` | `POST reports/card-orders` | full refresh, incremental | `id`, `solid_account` | `updated_at` |
| `apm_orders` | `POST reports/apm-orders` | full refresh, incremental | `id`, `solid_account` | `updated_at` |
| `chargebacks` | `POST reports/card-orders/chargebacks` (chargebacks of each order) | full refresh, incremental | `id`, `solid_account` | `updated_at` |
| `paypal_disputes` | `POST reports/apm-orders/paypal-disputes` | full refresh, incremental | `id`, `solid_account` | `updated_at` |
| `card_fraud_alerts` | `POST reports/card-orders/fraud-alerts` | full refresh, incremental | `order_id`, `created_at`, `solid_account` | `updated_at` |
| `financial_entries` | `POST reports/finance/financial_entries` (CSV file) | full refresh, incremental | `id`, `solid_account` | `created_at` |

The reports API is `https://reports.solidgate.com/api/v1`, products come from
`https://subscriptions.solidgate.com/api/v1`.

- **Orders.** `card_orders` and `apm_orders` hold one record per order, with its amounts in minor
  units, status, customer, product (`product_id`, `product_name`, `product_type`), subscription
  (`subscription_id`, which joins `subscriptions.id`), settled and refunded totals (`settled_*`,
  `refunded_*`, `refund`) and, as nested JSON, its `transactions`, `order_metadata` and (card orders)
  `routing`. Card orders also carry `payment_method`, `traffic_source` and `fraudulent`, APM orders
  `merchant_account_id`. `id` is the order id.
- **Subscriptions.** `subscriptions` flattens the customer, product and pause of each subscription
  into `customer_*`, `product_*` and `pause_*` fields. `subscription_invoices` has one record per
  invoice of those subscriptions, with `subscription_id` and its orders as a list in `orders`.
- **Disputes.** `chargebacks` has one record per chargeback, with the `order_id` it belongs to and
  its stages in `flows`; `paypal_disputes` one record per PayPal dispute (`id` is the dispute id,
  `represented_by` who defended it);
  `card_fraud_alerts` one record per fraud alert of a card order. An order can have several alerts,
  so the key is the order and the alert's `created_at`.
- **Products.** Subscription products, paged by offset until a page comes back empty, with
  `term_length`, the retry settings and, as nested JSON, `billing_period`, `trial`, `tax` and
  `metadata`. `tax_profile_id`, `tax_category_id` and `tax_is_included` keep the flat tax fields of
  the earlier connector; Solidgate no longer documents the profile and `is_included`, so those two are
  null unless it still sends them.
- **Financial entries.** Sales, refunds, chargebacks and fees as Solidgate books them, one record per
  CSV row. Amounts stay strings as the file has them.

A field Solidgate does not send is null, never `""`, `0` or `false`. Every field is declared nullable
except the primary key and cursor fields the earlier connector declares non-null.

Report timestamps are UTC; records carry them as `2026-10-16T10:00:00+00:00`. In
`financial_entries`, `transaction_datetime_utc` carries `+00:00`, while `created_at` and
`transaction_datetime_provider` carry no zone (`2026-10-16T10:00:00`, format `date-time`, airbyte_type
`timestamp_without_timezone`): Solidgate does not write them in UTC on every row, and `created_at`
lags the transaction by hours. `accounting_date` is a date.

## Incremental sync

The report streams request one UTC day per request (`date_from` 00:00:00 inclusive to `date_to` the
next 00:00:00 exclusive, `filter` `updated_at`, `limit` 2000) and follow
`metadata.next_page_iterator` until it comes back empty. A read covers every day from the last day
read minus `lookback_days` (or from `start_date` on the first read, and never before it) up to today.

Once every page of a day is out, an incremental read saves that day as the account's last day read,
whether the day had records or not, in STREAM state:

```json
{"accounts": {"main": {"day": "2026-10-18"}, "eu": {"day": "2026-10-18"}}}
```

So each read requests the last `lookback_days` days and today again, however rarely a stream has
records, and a day that fails is requested again by the next read.

To read a past period again, set `day` of the accounts concerned to the day before it (in the
connection's state of the stream) or reset the stream's state; `start_date` must not be later than the
period. With append_dedup, records read again replace their earlier copies by the primary key; with
append, they are added again.

`subscription_invoices` is requested through the subscriptions report, which selects subscriptions by
their `updated_at` and lists every invoice of each, also invoices that changed long ago. Its records
are all emitted, so an incremental read can emit invoices whose `updated_at` is older than the days it
reads; deduplication by `id` and `solid_account` keeps the latest copy. The conformance test states
this with a `Lookback` that sets no lower bound for this stream.

A full refresh reads every day from `start_date` and saves no state.

### financial_entries

Per account and UTC day the connector creates a report (`filter` `created_at`) and polls the
`report_url` it gets back, which must be on the reports host because polling is signed: 204 means not
ready yet (polled again after 2s, doubling up to 60s, for 15 minutes at most), 302 points to the
file, 404 (not found) and 410 (expired) fail the read. The file is downloaded from the redirect
without Solidgate headers. The whole CSV is parsed before any of its records is emitted: a row with
a different number of values than the header, or a date column that does not match its format
(`YYYY-MM-DD HH:MM:SS`, `accounting_date` `YYYY-MM-DD`), fails the read naming the column, account,
day and entry id, never the value; the day is then not saved in state. Columns the schema does not
list are dropped.

## Errors and limits

| Response | Error starts with | Retried |
|---|---|---|
| 401, 403 with a JSON error, or error code `1.01` in a 200 | `invalid key pair` | no |
| 403 with an HTML body | `blocked by Solidgate WAF; check the API host` | no |
| 429 or error code `5.07` | `rate limited by Solidgate` | yes |
| 5xx, timeout, connection error | the request and status | yes |
| another error object in a 200 | the request, code and messages | no |

Messages name the account, the request and its status, such as
`account "main": invalid key pair: ... (POST /api/v1/card-orders (HTTP 401, code 1.01): Authentication failed)`.
Retried requests are sent five times at most, waiting 2s, 4s, 8s and 16s (capped at 60s).

Solidgate allows 25 requests per second per merchant account live and 10 in the sandbox. The
connector sends at most 10 per second per account.

## Differences from the earlier private connector

This connector replaces an earlier private Airbyte connector; the stream names, keys, cursors, fields
and their types of its version 3.7.0 are kept, so the records land in the same destination columns
(`TestCatalogParity` pins them against `testdata/expected_catalog.json`: the 3.7.0 schemas without
descriptions, nullable as below). It differs in:

- fields the earlier connector declares non-null, although it emits null when Solidgate does not
  send them, are declared nullable; only its non-null primary key and cursor fields stay non-null;
- configuration: `accounts[]` with `name`, `public_key` and `secret_key`; `start_date` is optional
  and there is no `end_date`;
- `lookback_days` (default 2) replaces `sync_last_days`: an incremental read restarts from the
  last day read minus the lookback;
- state is the last day read per account, not the set of handled days, so a `start_date` moved
  earlier reads the days before it only after the stream's state is reset;
- report-stream timestamp values gain an explicit UTC offset (`2026-10-16T10:00:00+00:00` where the
  earlier connector emitted `2026-10-16T10:00:00`); their schema is unchanged (`format: date-time`,
  no airbyte_type), and the `financial_entries` timestamps are emitted as before;
- `financial_entries` emits schema columns only; CSV columns the schema does not list are dropped,
  although its schema keeps `additionalProperties: true`;
- `subscription_invoices` can emit invoices whose `updated_at` is older than the days it reads, as the
  earlier connector did; the conformance test declares this with a `Lookback` without a lower bound
  for that stream (see [Incremental sync](#incremental-sync));
- report requests send `filter: updated_at` explicitly;
- the auth headers are lowercase `merchant` and `signature`;
- field descriptions are left out of the stream schemas, nested ones included.

## Tests

Tests replay HTTP from hand-written fixtures; they need no network and no credentials. This is the
recorded-HTTP pattern every API source in this repository follows (see
[CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)). All values in
`testdata/` are synthetic.

- **Layout:** `testdata/<report path>/<day>.json` answers a report request whose `date_from` is that
  day, `<day>_<iterator>.json` its next page and `<report path>/empty.json` any other day;
  `testdata/products/offset_<n>.json` the products page at offset n; `testdata/financial_entries/<day>.csv`
  the financial entries file of a day; `testdata/errors/` the error bodies.
- **Helper:** `fixtures_test.go` holds `fixtureServer`, which checks the `merchant` and `signature`
  headers of every API request against `api_pk_test` / `api_sk_test` (recomputing the HMAC itself) and
  answers 401 `1.01` otherwise, fails the test when the file download carries them, plays the
  report flow (report_url, 302 to the file), lets a test replace the responses to one request in
  turn, and records the requests. `testSource` points a `Source` at it through unexported fields
  (base URLs, `now` fixed at 2026-10-18 12:00 UTC, `client`, short delays, no rate limit).
- `TestSignature` checks the signature against a vector computed with `openssl`; `TestCheckErrors`
  the invalid key pair, WAF and 429 `5.07` messages; `TestRead*` resume, window, pagination and
  records; `TestFinancialEntries*` the report statuses and that a malformed file saves no state;
  `TestConformance` runs the SDK conformance suite for full refresh and incremental. The
  suite needs synclet-go-sdk v0.3.1 or later, which accepts zone-less values for
  `timestamp_without_timezone`.
