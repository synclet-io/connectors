# source-facebook-marketing

[Facebook Marketing API](https://developers.facebook.com/docs/marketing-api) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads Meta ad accounts, their
campaigns, ad sets, ads and creatives, business extended credits, and daily ad insights through the
asynchronous report API, the insights incrementally by `date_start`.

```bash
task test CONNECTOR=source-facebook-marketing     # no network, no credentials
task build CONNECTOR=source-facebook-marketing
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-facebook-marketing:dev check --config /data/config.json
```

Requests go to Graph API `v26.0` (the `apiVersion` constant in `client.go`). Meta releases a version
every few months and retires each about two years later; bump the constant and re-run the tests.

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `access_token` | yes | Long-lived user or system-user token with `ads_read`. Sent only as `Authorization: Bearer <token>`, never in a URL |
| `account_ids` | yes | Ad accounts to read, with or without the `act_` prefix |
| `start_date` | yes | First day of the insights streams, `YYYY-MM-DD` |
| `end_date` | no | Last day of the insights streams; empty reads up to today |
| `lookback_days` | no | Days before the saved `date_start` an incremental read starts again (default 28) |
| `business_ids` | no | Businesses whose extended credits `business_extended_credits` reads |
| `graph_api_base_url` | no | Replaces `https://graph.facebook.com`, for example a proxy; `/v26.0/` is appended |
| `graph_api_headers` | no | Extra headers for every request, `[{"name": "...", "value": "..."}]` |

```json
{"access_token": "<token>", "account_ids": ["act_1234567890"], "start_date": "2026-01-01"}
```

`check` requests `me` (an invalid or expired token fails as such), `me/permissions` (fails with
`missing ads_read permission` unless `ads_read` is granted) and every ad account (fails with
`ad account act_<id> not accessible` for an unknown account or one the token cannot read).

A proxy host in `graph_api_base_url` is not in the image's `allowed_hosts` (`graph.facebook.com`); a
platform that enforces it must allow the proxy host.

## Streams

Every stream is in the namespace `facebook_marketing`. Accounts and businesses are read one after
another.

| Stream | Request | Sync modes | Primary key |
|---|---|---|---|
| `ad_insights` | insights, level `ad`, `time_increment=1` | full refresh, incremental | `account_id`, `ad_id`, `date_start` |
| `ad_insights_age` | same, breakdown `age` | full refresh, incremental | + `age` |
| `ad_insights_age_gender` | same, breakdowns `age,gender` | full refresh, incremental | + `age`, `gender` |
| `ad_insights_country` | same, breakdown `country` | full refresh, incremental | + `country` |
| `ad_insights_platform` | same, breakdowns `publisher_platform,platform_position` | full refresh, incremental | + `publisher_platform`, `platform_position` |
| `ad_insights_hourly` | same, breakdown `hourly_stats_aggregated_by_advertiser_time_zone` | full refresh, incremental | + that field |
| `account_spend_daily` | insights, level `account` | full refresh, incremental | `account_id`, `date_start` |
| `business_extended_credits` | `<business_id>/extendedcredits` | full refresh | `id` |
| `ad_accounts` | `act_<id>` | full refresh | `id` |
| `campaigns` | `act_<id>/campaigns` | full refresh | `id` |
| `adsets` | `act_<id>/adsets` | full refresh | `id` |
| `ads` | `act_<id>/ads` | full refresh | `id` |
| `ad_creatives` | `act_<id>/adcreatives` | full refresh | `id` |

The ad-level insights streams carry ids and names of the campaign, ad set and ad, `spend`, `clicks`,
`unique_clicks`, `impressions`, `inline_link_clicks`, `unique_inline_link_clicks`, and the action
arrays `actions`, `cost_per_thruplay`, `unique_outbound_clicks` and the video watch actions.
`account_spend_daily` carries `account_id`, `account_currency`, `date_start` and `spend`.

Numbers the API sends as strings are emitted as JSON numbers, and Graph times
(`2026-10-01T08:30:00+0000`) as RFC 3339. `business_extended_credits` flattens each amount
(`balance`, `credit_available`, `online_max_balance`, `max_balance`, `allocated_amount`) into
`<name>_amount` (a number; a comma decimal separator is accepted), `<name>_currency`,
`<name>_amount_in_hundredths` and `<name>_offsetted_amount`; `date_created` is the time of the read.
Lists are paged 500 at a time by `paging.cursors.after`. The entity streams read everything on each
sync: filtering by `updated_time` would miss deleted objects.

Meta still changes insights for about 28 days, and breakdowns older than 13 months lose `reach` and
`frequency`.

## Insights reports

Each insights stream creates one asynchronous report job per account (`POST act_<id>/insights` with
`level`, `time_increment=1`, `time_range`, `fields` and `breakdowns`), polls it every 2 seconds until
`async_status` is `Job Completed`, then pages its rows. A `Job Failed` or `Job Skipped` job, or one
still unfinished after 1800 polls, is created again up to 3 times; after that the stream fails with
an error that names the stream, the account and the range, such as
`ad_insights: account act_1234567890 2026-09-20..2026-10-18: report job ended with "Job Failed" 4 times`.
Rows outside the requested range are dropped.

## Incremental sync

The insights streams use the source-defined cursor `date_start` and keep one entry per account:

```json
{"accounts": {"1234567890": {"date_start": "2026-10-17"}}}
```

A read of an account requests from its saved `date_start` minus `lookback_days`, never before
`start_date` (or from `start_date` without a saved entry), up to `end_date` or today, and saves the
state of every account after each account. The saved `date_start` is the latest one emitted.

To re-sync a past range, set an account's `date_start` (plus the lookback) and `end_date` in the state
by hand: the next read stops at that `end_date`, and the state it saves no longer has one.

## Throttling

Meta reports rate limiting with Graph error codes 4, 17, 32, 613 and 80000-80014, sent with HTTP 400
and sometimes 200, not 429. Such a response is retried up to 5 times after the longest wait the
headers name: `estimated_time_to_regain_access` (minutes) in `X-Business-Use-Case-Usage` and
`reset_time_duration` (seconds) in `X-Ad-Account-Usage`, at most an hour. Without them, and for 5xx
responses, transient errors (codes 1, 2) and transport errors, the delay starts at 5 seconds and
doubles. HTTP 429 without one of those codes and every other error fail at once; error 190 is
reported as an invalid or expired access token.

## Differences from the earlier private connector

This connector replaces an earlier private Airbyte connector; its stream names, keys, cursors and
fields are kept (`TestCatalogParity` pins them). It differs in:

- `lookback_days` (default 28) applies to every insights stream; the earlier connector re-read 30 days
  on `account_spend_daily` only;
- `start_date` is required;
- numeric strings are emitted as numbers, and only schema fields are emitted (no `date_stop`);
- the token travels in the `Authorization` header;
- state is saved after each account;
- the streams `ad_accounts`, `campaigns`, `adsets`, `ads` and `ad_creatives` are added.
