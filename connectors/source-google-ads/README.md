# source-google-ads

[Google Ads API](https://developers.google.com/google-ads/api/docs/start) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads Google Ads accounts, their
campaigns, ad groups and ads, daily performance, and any GAQL query you configure. Every stream is a
GAQL query sent to the REST `googleAds:search` method; daily streams are incremental by `segments.date`.

```bash
task test CONNECTOR=source-google-ads     # no network, no credentials
task build CONNECTOR=source-google-ads
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-google-ads:dev check --config /data/config.json
```

Requests go to Google Ads API `v26` (the `apiVersion` constant in `client.go`). Google releases a major
version about every three months and sunsets each about a year later (see the
[sunset dates](https://developers.google.com/google-ads/api/docs/sunset-dates)); bump the constant, check
the release notes for renamed fields of the built-in queries in `streams.go`, and re-run the tests.

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `developer_token` | yes | Developer token of your manager account. A token with test access reads test accounts only |
| `client_id`, `client_secret` | yes | The Google Cloud OAuth client the refresh token was issued to |
| `refresh_token` | yes | OAuth refresh token with the `https://www.googleapis.com/auth/adwords` scope |
| `customer_ids` | yes | Accounts to read, with or without dashes. A manager account reads its client accounts |
| `start_date` | yes | First day of the daily streams, `YYYY-MM-DD` |
| `end_date` | no | Last day of the daily streams; empty reads up to today (UTC) |
| `lookback_days` | no | Days before the saved date an incremental read starts again (default 14) |
| `login_customer_id` | no | Manager account the credentials reach the customers through (the `login-customer-id` header) |
| `child_customer_ids` | no | Client accounts of the managers in `customer_ids` to read; empty reads all enabled ones |
| `custom_queries` | no | `[{"name": "...", "query": "SELECT ... FROM ..."}]`, each read as its own stream |

```json
{
  "developer_token": "<developer token>",
  "client_id": "<client id>.apps.googleusercontent.com",
  "client_secret": "<client secret>",
  "refresh_token": "<refresh token>",
  "customer_ids": ["111-111-1111"],
  "start_date": "2026-01-01",
  "custom_queries": [
    {
      "name": "keyword_daily",
      "query": "SELECT customer.id, ad_group.id, ad_group_criterion.keyword.text, metrics.clicks, segments.date FROM keyword_view"
    }
  ]
}
```

## Authentication

The connector exchanges the refresh token at `https://oauth2.googleapis.com/token` for an access token,
keeps it until a minute before it expires and fetches a new one when the API answers 401. The access
token travels in the `Authorization` header and the developer token in the `developer-token` header,
never in a URL.

To get the credentials: create an OAuth client (type *Desktop app* or *Web application*) in a Google
Cloud project with the Google Ads API enabled, authorize it once with the `adwords` scope for a Google
user that can see the accounts (for example with the
[OAuth Playground](https://developers.google.com/oauthplayground) using your own client), and copy the
refresh token. The developer token is in the API Center of your manager account.

A failed `check` names the cause:

| Message | Cause |
|---|---|
| `refresh_token is invalid or revoked` | The token endpoint answered `invalid_grant` |
| `client_id or client_secret is invalid` | The token endpoint answered `invalid_client` |
| `developer token is not approved for this account (test-only token?)` | `DEVELOPER_TOKEN_NOT_APPROVED` or `DEVELOPER_TOKEN_PROHIBITED` |
| `customer <id> is not accessible; if it is managed through a manager account, set login_customer_id` | `USER_PERMISSION_DENIED` |

## Manager accounts

Each customer in `customer_ids` is first listed with
`SELECT customer_client.id, customer_client.manager, customer_client.status, customer_client.level FROM customer_client`.
A manager account contributes every `ENABLED` account below it that is not a manager itself, narrowed to
`child_customer_ids` when that is set; any other account is read as it is. Requests for an account found
under a manager send `login-customer-id`: `login_customer_id` when it is set, else that manager.

`customer_client` reads the configured customers, so for a manager it lists its whole hierarchy. Every
other stream reads each account resolved above.

## Streams

Namespace `google_ads`. Record keys are the GAQL field names with `.` replaced by `_`
(`ad_group_ad.ad.id` becomes `ad_group_ad_ad_id`). 64-bit integers, which the REST API sends as strings,
become JSON numbers. Every built-in query selects `customer.id`.

| Stream | Resource | Primary key | Sync modes |
|---|---|---|---|
| `customer` | `customer` | `customer_id` | full refresh |
| `customer_client` | `customer_client` | `customer_id`, `customer_client_id` | full refresh |
| `campaign` | `campaign` | `campaign_id` | full refresh |
| `ad_group` | `ad_group` | `ad_group_id` | full refresh |
| `ad_group_ad` | `ad_group_ad` | `ad_group_id`, `ad_group_ad_ad_id` | full refresh |
| `campaign_performance_daily` | `campaign` | `campaign_id`, `segments_date` | full refresh, incremental |
| `ad_group_performance_daily` | `ad_group` | `ad_group_id`, `segments_date` | full refresh, incremental |
| `ad_performance_daily` | `ad_group_ad` | `ad_group_id`, `ad_group_ad_ad_id`, `segments_date` | full refresh, incremental |
| `geographic_view_daily` | `geographic_view` | `ad_group_id`, `geographic_view_country_criterion_id`, `geographic_view_location_type`, `segments_date` | full refresh, incremental |

The daily streams select `metrics.impressions`, `clicks`, `cost_micros`, `conversions`,
`conversions_value`, `all_conversions`, `ctr` and `average_cpc`. Amounts in `*_micros` are millionths of
the account currency.

## Custom queries

Each entry of `custom_queries` becomes a stream named `name` (lowercase letters, digits and `_`, not the
name of a built-in stream) with one property per selected field. Field types come from a built-in table
for the fields the built-in streams use, and from one `googleAdsFields:search` request for the rest at
discover and read; a field Google does not know fails the configuration. A custom stream has no primary
key; select `customer.id` to tell the accounts of a manager apart.

A query that selects `segments.date` is a daily stream: it is incremental with cursor `segments_date`, and
the connector AND-s `segments.date BETWEEN '<since>' AND '<until>'` into its `WHERE` clause (before
`ORDER BY`, `LIMIT` and `PARAMETERS`), so the query itself should not limit `segments.date`.

## Incremental sync

The state of a daily stream holds, per account, the last day of the last range read:

```json
{"customers": {"1234567890": {"date": "2026-10-18"}}}
```

A read covers, for each account, from the saved date minus `lookback_days` (default 14, since Google keeps
attributing conversions to past days), never before `start_date` and never before 37 months ago (the
history Google Ads keeps), up to `end_date` or today (UTC). Rows outside that range are not emitted. The
state is emitted after each account. A full refresh of a daily stream reads from `start_date` (or 37
months ago) without state.

## Rate limits

Google Ads limits requests per customer and per developer token. A request that fails with HTTP 429 or
`RESOURCE_TEMPORARILY_EXHAUSTED`, a 5xx response or a transport error is sent again after a doubling,
jittered delay (2 s, 4 s, 8 s, 16 s), five times in all. `RESOURCE_EXHAUSTED` (the daily operations quota
of the developer token) and authorization errors fail the read at once. `search` returns pages of 10,000
rows; each page is one request, so a failure repeats one page only.
