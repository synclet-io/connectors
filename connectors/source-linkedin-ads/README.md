# source-linkedin-ads

[LinkedIn Marketing API](https://learn.microsoft.com/en-us/linkedin/marketing/) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads LinkedIn ad accounts, their
campaign groups, campaigns and creatives, and daily ad analytics by campaign and by creative. The
analytics streams are incremental by `date`.

```bash
task test CONNECTOR=source-linkedin-ads     # no network, no credentials
task build CONNECTOR=source-linkedin-ads
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-linkedin-ads:dev check --config /data/config.json
```

Requests go to the versioned REST API (`https://api.linkedin.com/rest/`) with
`LinkedIn-Version: 202609` (the `apiVersion` constant in `client.go`) and
`X-Restli-Protocol-Version: 2.0.0`. LinkedIn releases a version every month and supports each for at
least a year; bump the constant, check the
[Marketing API changelog](https://learn.microsoft.com/en-us/linkedin/marketing/integrations/recent-changes)
for the fields in `streams.go`, and re-run the tests.

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `client_id`, `client_secret` | yes | The LinkedIn app the refresh token was issued to. The app needs the Advertising API product |
| `refresh_token` | yes | OAuth refresh token with the `r_ads` and `r_ads_reporting` scopes |
| `account_ids` | no | Ad accounts to read; empty reads every account the authorizing user has a role on |
| `start_date` | yes | First day of the analytics streams, `YYYY-MM-DD` |
| `end_date` | no | Last day of the analytics streams; empty reads up to today (UTC) |
| `lookback_days` | no | Days, 0 to 30, before the saved date an incremental read starts again (default 7) |

```json
{
  "client_id": "<client id>",
  "client_secret": "<client secret>",
  "refresh_token": "<refresh token>",
  "account_ids": ["500000001"],
  "start_date": "2026-01-01"
}
```

## Authentication

The connector exchanges the refresh token at `https://www.linkedin.com/oauth/v2/accessToken` for an
access token, keeps it until a minute before it expires, and when the API answers 401 fetches a new one
and repeats the request once. The access token travels in the `Authorization` header, never in a URL.

LinkedIn refresh tokens live 365 days and do not rotate; access tokens live 60 days. When the refresh
token expires, authorize the app again and put the new token in the configuration.

To get the credentials: create an app in the [LinkedIn developer portal](https://www.linkedin.com/developers/apps),
request the **Advertising API** product (LinkedIn reviews the request; it can take weeks), and authorize
the app once with the `r_ads` and `r_ads_reporting` scopes as a user with a role on the ad accounts.

## Check

`check` names the first cause that fails:

| Cause | Message |
|---|---|
| Refresh token invalid, expired or revoked | `refresh_token is invalid or expired; authorize the app again for a new one` |
| Wrong client id or secret | `client_id or client_secret is invalid` |
| App without the Advertising API product, or token without `r_ads` | `the LinkedIn app lacks the Advertising API product or the token lacks the r_ads scope` |
| A configured account the user cannot see (403 or 404) | `ad account <id> is not accessible to the user who authorized the app` |
| No `account_ids` and the user has no account | `the user who authorized the app has no ad account` |

## Streams

Namespace `linkedin_ads`.

| Stream | Request | Primary key | Sync modes |
|---|---|---|---|
| `ad_accounts` | `adAccounts/{id}`, or `adAccounts?q=search` without `account_ids` | `id` | full refresh |
| `campaign_groups` | `adAccounts/{id}/adCampaignGroups?q=search`, every status | `id` | full refresh |
| `campaigns` | `adAccounts/{id}/adCampaigns?q=search`, every status | `id` | full refresh |
| `creatives` | `adAccounts/{id}/creatives?q=criteria` | `id` (creative URN) | full refresh |
| `ad_analytics_by_campaign` | `adAnalytics?q=analytics&pivot=CAMPAIGN&timeGranularity=DAILY` | `campaign_id`, `date` | full refresh, incremental |
| `ad_analytics_by_creative` | `adAnalytics?q=analytics&pivot=CREATIVE&timeGranularity=DAILY` | `creative_id`, `date` | full refresh, incremental |

Entity records keep LinkedIn's field names; amounts sent as strings become numbers. The search finders
have no modified-since filter, so the entity streams are full refresh only. Paged finders follow
`metadata.nextPageToken` with `pageSize=100`.

An analytics record has `account_id`, the pivot's numeric id (`campaign_id` or `creative_id`), the pivot
URN as `pivot_value`, the day as `date` and the metrics `impressions`, `clicks`, `costInLocalCurrency`,
`costInUsd`, `landingPageClicks`, `externalWebsiteConversions`, `externalWebsitePostClickConversions`,
`externalWebsitePostViewConversions`, `conversionValueInLocalCurrency`, `oneClickLeads`,
`totalEngagements`, `likes`, `comments`, `shares`, `follows`, `videoViews`, `videoCompletions`.
With `dateRange` and `pivotValues` that is 19 fields, within LinkedIn's limit of 20.

## Analytics requests

Analytics use Rest.li 2.0 finder parameters, written without escaping their structure and with every
value inside percent-encoded:

```
adAnalytics?q=analytics&pivot=CAMPAIGN&timeGranularity=DAILY
  &dateRange=(start:(year:2026,month:8,day:1),end:(year:2026,month:8,day:30))
  &accounts=List(urn%3Ali%3AsponsoredAccount%3A500000001)
  &fields=dateRange,pivotValues,impressions,...
```

Each account is read on its own, in windows of up to 30 days. The finder is not paged; a window that
would return more rows than LinkedIn allows fails with LinkedIn's error rather than returning a partial
result.

## Incremental sync

The state of an analytics stream holds, per account, the last day of the last window read:

```json
{"accounts": {"500000001": {"date": "2026-10-18"}}}
```

It is saved after every window. The next read starts `lookback_days` before the saved date, never
before `start_date`, because LinkedIn still updates the most recent days. An account without a saved
date starts at `start_date`.

## Errors and throttling

HTTP 429, 5xx responses and transport errors are retried five times after a doubling delay starting at
one second. Any other error fails the read at once with the request, status, LinkedIn's error code and
message. A row outside the requested range, without a day or pivot, or a field whose type does not match
its schema fails the stream.

## Tests

The tests run against an `httptest` server that serves hand-written, synthetic responses from
`testdata/` shaped like the Marketing API's: made-up accounts, ids, names and amounts. The server checks
the version and protocol headers and fails a test when a credential appears in a URL.

`TestLive` runs check and a full refresh of `ad_accounts` against the real API when
`LINKEDIN_ADS_LIVE_CONFIG` names a config file; it is skipped otherwise:

```bash
LINKEDIN_ADS_LIVE_CONFIG=/path/to/config.json go test -run TestLive -v ./...
```
