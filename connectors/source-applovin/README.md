# source-applovin

[AppLovin](https://www.applovin.com) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads the advertiser (Axon) report
and the MAX revenue report of the
[Reporting API](https://support.applovin.com/en/growth/promoting-your-apps/api/reporting-api) and the
[Revenue Reporting API](https://support.applovin.com/en/max/reporting-apis/revenue-reporting-api/),
one day per request, incrementally by day.

```bash
task test CONNECTOR=source-applovin     # no network, no credentials
task build CONNECTOR=source-applovin
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-applovin:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `api_key` | yes | Report Key of the account (Keys page of the AppLovin dashboard) |
| `start_date` | yes | First day to read, `YYYY-MM-DD` in UTC |
| `lookback_days` | no | Days before the saved day an incremental read requests again; default 3, not negative |

```json
{"api_key": "<your report key>", "start_date": "2026-09-01"}
```

**The key travels in the URL.** AppLovin accepts the Report Key only as the `api_key` query
parameter, unlike the header the [CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)
rules ask for. The connector never logs a request URL, takes the URL out of transport errors and
replaces the key with `REDACTED` in every error message.

`check` requests one row of yesterday from each report type (`day,impressions`). A 401 fails with
`invalid report key`. A report type the key may not read (403) is logged as a warning and `check`
passes; reading that stream then fails with a `config_error`. When neither report type is readable,
`check` fails with `report types advertiser and MAX revenue are not available to the report key`.

## Streams

Both streams are in namespace `applovin`, support full refresh and incremental, and use the
source-defined cursor `day`.

| Stream | Request | Primary key |
|---|---|---|
| `advertiser_report` | `GET https://r.applovin.com/report?report_type=advertiser` | `day`, `campaign_id_external`, `country`, `platform`, `device_type` |
| `max_revenue_report` | `GET https://r.applovin.com/maxReport` | `day`, `package_name`, `platform`, `country`, `device_type`, `ad_format`, `max_ad_unit_id`, `network` |

`advertiser_report` has 147 columns: 37 dimensions and metrics (`day`, `hour`, campaign, ad and
creative set fields, `platform`, `device_type`, `country`, `impressions`, `clicks`, `ctr`,
`conversions`, `cost`, ...) and 110 cohort columns generated from three span lists:

| Metrics | Spans |
|---|---|
| `ad_rev`, `ad_roas`, `iap_rev`, `iap_roas`, `roas`, `total_rev` | `0d 1d 2d 3d 7d 14d 28d 30d 90d 1y` |
| `cost_per_target_event`, `cpp`, `sales`, `target_event_count`, `unique_purchasers` | `0d 1d 2d 3d 7d 14d 30d 90d 1y` |
| `ret` | `1d 3d 7d 14d 28d` |

`max_revenue_report` has the dimensions `day`, `application`, `package_name`, `store_id`,
`platform`, `country`, `device_type`, `ad_format`, `max_ad_unit_id`, `max_ad_unit`,
`max_ad_unit_test`, `network`, `custom_network_name` and the metrics `impressions`,
`estimated_revenue`, `ecpm`, `attempts`, `responses`, `fill_rate`. Not read, because of the API's
column-combination rules or because they split rows finer than the primary key: `requests`,
`max_placement`, `network_placement`, `hour`, `ad_unit_waterfall_name` and `has_idfa`.

**Columns.** Each request asks only for the columns of the configured stream's schema, plus `day`
and the primary key columns, which are always requested. A configured schema without properties
requests every column. `hour` exists for the last 30 days only: for older days it is left out of
the request, with a warning.

**Values.** The API sends numbers as quoted strings. Number columns become JSON numbers; a null or
empty value leaves the field out of the record. A value that is not a number fails the read with
the stream, day and column named; no row is dropped.

**Paging and retries.** Each day is requested with `limit=10000` and growing `offset` until a page
comes back shorter. A 429, a 5xx or a network error is retried up to five attempts in all, waiting
2s, 4s, 8s and 16s. 401 and 403 are not retried.

## Incremental sync

The API serves the last 45 days, today (UTC) included. A read requests one day at a time from the
start day up to **today**, and emits only rows whose `day` is not before the start day. The start
day is the latest of:

- `start_date`;
- in an incremental read with a saved day: the saved day minus `lookback_days`;
- the first day of the request window (today minus 44 days). A start before it is moved to it with a
  warning.

After each day an incremental read saves that day in STREAM state, so today is re-read by the next
sync through the lookback:

```json
{"day": "2026-10-17"}
```

A full refresh reads from `start_date` (moved into the window) and saves no state.

## Differences from the earlier private connector

This connector replaces an earlier private Airbyte connector. Its `advertiser_report` stream name,
namespace, columns, types, primary key and cursor are kept, as is the schema style (properties
only, one type per property in an array, no formats); `TestCatalogParity` pins both streams against
`testdata/expected_catalog.json`. It differs in:

- `max_revenue_report` is new;
- `lookback_days` (default 3) replaces resuming on the day after the saved day; the read goes up to
  today instead of yesterday;
- the start is moved into the API's 45-day request window instead of requesting days the API
  rejects;
- `day` and the primary key columns are always requested; `hour` is requested only for the last
  30 days;
- number columns are emitted as JSON numbers instead of the strings the API sends, and null or empty
  values are left out;
- `check` probes both report types and tells an invalid key from a report type the key may not read;
- 429, 5xx and network errors are retried;
- field descriptions are new.

## Tests

Tests replay HTTP from hand-written fixtures; they need no network and no credentials (see
[CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)).

- **Layout:** `testdata/report/advertiser/<day>.json` answers `GET /report?report_type=advertiser&start=<day>`,
  `testdata/maxReport/<day>.json` answers `GET /maxReport?start=<day>`; error bodies live in
  `testdata/errors/`. All values are synthetic (`com.example.*` apps, made-up campaign and ad unit
  ids, round numbers). AppLovin's error bodies are not documented: the fixtures assume 401 for an
  invalid key and 403 for a report type the key may not read.
- **Helper:** `fixtures_test.go` holds `fixtureServer`, which serves `testdata/`, answers 401 to any
  request whose `api_key` query parameter is not `test-report-key` (the one exception to the
  header rule, since AppLovin has no other way to send the key), lets a test override one file or a
  whole report type, and records each request with its query. `testSource` points a `Source` at it
  through its unexported fields (`baseURL`, `now` fixed at 2026-10-18 12:00 UTC, `client`,
  `retryDelay`), which are zero in `main`.
- `TestCatalogParity` pins the catalog; `TestCohortColumns` the generated cohort columns;
  `TestReadRequestsSelectedColumns` the `columns` parameter; `TestCheck*` the check causes;
  `TestReadResume`, `TestReadWindow` and `TestReadEmitsOnlyDaysFromStart` the start day;
  `TestConformance` runs the SDK conformance suite for full refresh and incremental.
