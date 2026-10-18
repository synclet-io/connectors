# source-tiktok-marketing

[TikTok Business API](https://business-api.tiktok.com/portal/docs) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads TikTok advertisers, their
campaigns, ad groups and ads, and daily BASIC reports at ad, ad group, campaign and advertiser level,
the reports incrementally by `stat_time_day`.

```bash
task test CONNECTOR=source-tiktok-marketing     # no network, no credentials
task build CONNECTOR=source-tiktok-marketing
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-tiktok-marketing:dev check --config /data/config.json
```

Requests go to API version `v1.3`.

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `access_token` | yes | Long-lived access token of a TikTok Business API app, or a sandbox token. Sent only in the `Access-Token` header, never in a URL |
| `advertiser_ids` | yes | Advertisers to read, as digits; each must be authorised for the token |
| `start_date` | yes | First day of the report streams, `YYYY-MM-DD` |
| `end_date` | no | Last day of the report streams; empty reads up to today (UTC) |
| `lookback_days` | no | Days, 0 to 30, before the saved `stat_time_day` an incremental read starts again (default 3) |
| `environment` | no | `production` (`https://business-api.tiktok.com/open_api/v1.3/`, default) or `sandbox` (`https://sandbox-ads.tiktok.com/open_api/v1.3/`) |

```json
{"access_token": "<token>", "advertiser_ids": ["<advertiser id>"], "start_date": "2026-01-01"}
```

The token is the long-lived one TikTok issues when an advertiser authorises your app; a sandbox
token and sandbox advertiser id work with `"environment": "sandbox"`. The advertiser ids are
configured by hand: listing the advertisers of a token (`oauth2/advertiser/get/`) needs the app id
and app secret as well, which this connector does not ask for.

`check` requests `advertiser/info/` for the configured advertisers, 100 per request. Codes 40102,
40104 and 40105 fail with `access token is invalid or expired (code <n>)`. Code 40001, or an
advertiser missing from the answer, fails with `advertiser <id> is not authorised for this access
token`; when the API refuses a batch with 40001, each advertiser is checked on its own to name it.

## Streams

Every stream is in the namespace `tiktok_marketing`. Advertisers are read one after another.

| Stream | Request | Sync modes | Primary key |
|---|---|---|---|
| `advertisers` | `advertiser/info/` | full refresh | `advertiser_id` |
| `campaigns` | `campaign/get/` | full refresh | `campaign_id` |
| `ad_groups` | `adgroup/get/` | full refresh | `adgroup_id` |
| `ads` | `ad/get/` | full refresh | `ad_id` |
| `ads_report_daily` | `report/integrated/get/`, `data_level=AUCTION_AD` | full refresh, incremental | `ad_id`, `stat_time_day` |
| `ad_groups_report_daily` | same, `AUCTION_ADGROUP` | full refresh, incremental | `adgroup_id`, `stat_time_day` |
| `campaigns_report_daily` | same, `AUCTION_CAMPAIGN` | full refresh, incremental | `campaign_id`, `stat_time_day` |
| `advertisers_report_daily` | same, `AUCTION_ADVERTISER` | full refresh, incremental | `advertiser_id`, `stat_time_day` |

The entity streams ask for an explicit `fields` list and emit only those fields; their times
(`create_time`, `modify_time`) stay strings as the API sends them. They read everything on each sync:
the get endpoints filter by creation time, not `modify_time`, so they have no cursor.

The report streams request `report_type=BASIC` with the dimensions `[<level id>, "stat_time_day"]`
and the metrics `spend`, `impressions`, `clicks`, `ctr`, `cpc`, `cpm`, `reach`, `frequency`,
`conversion`, `cost_per_conversion` and `conversion_rate`. A record carries `advertiser_id`, the
level id, `stat_time_day` as `YYYY-MM-DD` and the metrics as numbers; a metric the API sends as `-`
is null. Rows outside the requested range are dropped.

Lists are paged 1000 at a time by `page` and `page_size` until `page_info.total_page`.

## Report windows

A report stream reads each advertiser in ascending windows of 30 days. When the API refuses a window
with code 40067 (date range too long), the window is halved (30, 15, 7, 3, 1 days) and stays at that
size for the rest of that advertiser; 40067 on a one-day window fails the stream.

## Incremental sync

The report streams use the source-defined cursor `stat_time_day` and keep one entry per advertiser:

```json
{"advertisers": {"<advertiser id>": {"stat_time_day": "2026-10-17"}}}
```

A read of an advertiser requests from its saved `stat_time_day` minus `lookback_days`, never before
`start_date` (or from `start_date` without a saved entry), up to `end_date` or today (UTC), and saves
the state after each window with that window's last day. TikTok reports arrive about 11 hours late
and are still updated for a few days, which the default lookback of 3 days re-reads.

## Errors and throttling

TikTok answers errors with HTTP 200 and a non-zero `code` in the body. The throttling codes 40016,
40100 and 40133 and the transient codes 50000, 51002, 51004, 51041 and 60001, as well as HTTP 429,
5xx responses and transport errors, are retried up to 5 times after a delay that starts at 1 second
and doubles, at most a minute. Requests are sent one at a time. Every other code fails the stream at
once with `tiktok <path>: code <code>: <message>`, as do retries that run out.
