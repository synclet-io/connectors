# source-openexchangerates

[Open Exchange Rates](https://openexchangerates.org) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads the currency list, the latest
rates and one set of rates per day since a start date, the daily rates incrementally by date.

```bash
task test CONNECTOR=source-openexchangerates     # no network, no credentials
task build CONNECTOR=source-openexchangerates
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-openexchangerates:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `app_id` | yes | App ID of the account. Sent only as `Authorization: Token <app_id>`, never in a URL, so it does not show up in logged URLs or errors |
| `start_date` | yes | First day of `historical_rates`, `YYYY-MM-DD` in UTC |
| `base` | no | Base currency of every rate; empty means USD. Needs a paid plan |
| `symbols` | no | Currencies to read rates for, such as `["EUR","GBP"]`; empty reads all. Needs a paid plan |

```json
{"app_id": "<your app id>", "start_date": "2026-01-01", "base": "USD"}
```

`check` requests `latest.json` with the configured `base` and `symbols`, so a plan that lacks either
fails `check`, not the first sync.

## Streams

| Stream | Route | Sync modes | Primary key | Fields |
|---|---|---|---|---|
| `currencies` | `currencies.json` | full refresh | `code` | `code`, `name` |
| `latest_rates` | `latest.json` | full refresh | `base`, `currency` | `base`, `currency`, `rate`, `timestamp` |
| `historical_rates` | `historical/YYYY-MM-DD.json` | full refresh, incremental | `date`, `base`, `currency` | `date`, `base`, `currency`, `rate`, `timestamp` |

Each rates response becomes one record per currency, sorted by currency. `rate` keeps every digit the
API sends; `timestamp` is the Unix time the rates were published. The API has no pagination.

Not read in this version: `time-series.json` (Enterprise plan only), `convert`, `ohlc` and
`show_alternative`.

## Incremental sync

`historical_rates` uses the source-defined cursor `date`. A read requests one day at a time, from the
day after the saved date (or `start_date` on the first read) up to **yesterday in UTC**; today's rates
are still changing and are read once the day is over. After each day's records the connector saves
the day in STREAM state:

```json
{"date": "2026-10-17"}
```

A saved state takes priority over `start_date`: moving `start_date` later does not skip days
after the saved date, and moving it earlier does not re-read days before it; reset the stream to
start over from the new `start_date`.

A read that stops halfway resumes at the first day it did not finish. A full refresh reads every day
from `start_date` and saves no state. Each day costs one request against the plan's monthly quota, so
an early `start_date` on a small plan can use up the quota on the first sync.

## Errors

Open Exchange Rates answers errors with `{"error": true, "status": ..., "message": ..., "description": ...}`.
These are configuration errors (failure type `config_error`) and are not retried:

| Status | `message` | Error starts with |
|---|---|---|
| 401 | `invalid_app_id`, `missing_app_id` | `invalid app_id` |
| 429 | `not_allowed` | `plan does not allow this route` |
| 403 | `access_restricted` | `access restricted` |
| 400 | `invalid_base` | `invalid base currency` |

The rest of the message is `(HTTP <status> <message>): <description>`. 429 here means the plan does
not include the route or parameter (for example `base` on the free plan), not throttling, so it is
never retried. A 5xx response or a network error is retried three times, waiting 1s, 2s and 4s.

## Tests

Tests replay HTTP from hand-written fixtures; they need no network and no credentials. This is the
recorded-HTTP pattern every API source in this repository follows (see
[CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)).

- **Choice: an `httptest` fixture server with hand-written JSON**, not go-vcr cassettes. Recording
  cassettes needs a real account on a paid plan, scrubbing hooks for the app_id and account data, and
  re-recording whenever a request changes; it adds a dependency, and the errors that matter most
  (401, 403, 429 `not_allowed`) cannot be recorded on purpose. The cost of fixtures is that their
  shape may drift from the real API; they are copied from the response examples in the
  [API docs](https://docs.openexchangerates.org/reference/api-introduction) to keep that small.
- **Layout:** `testdata/` mirrors the API paths (`testdata/latest.json` answers `GET /api/latest.json`,
  `testdata/historical/2026-10-16.json` answers `GET /api/historical/2026-10-16.json`); error bodies
  live in `testdata/errors/`. All values are synthetic.
- **Helper:** `fixtures_test.go` holds `fixtureServer`, which serves `testdata/`, answers 401 with
  `errors/invalid_app_id.json` to any request without `Authorization: Token test-app-id`, fails the
  test when a request carries `app_id` in its URL, lets a test override the status and file of one
  path, and records the paths it was asked for. `testSource` points a `Source` at it through its
  unexported fields (`baseURL`, `now` fixed at 2026-10-18 12:00 UTC, `client`, `retryDelay`), which
  are zero in `main`.
- `TestCheckErrors` covers 401, 429 and 403; `TestRetry` a 503 followed by 200;
  `TestIncrementalResume` that a read from a saved date requests only the days after it up to
  yesterday; `TestConformance` runs the SDK conformance suite for full refresh and incremental.
