# source-google-search-console

[Google Search Console API](https://developers.google.com/webmaster-tools/v1/api_reference_index) source built
on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads the sites, sitemaps and daily search
analytics of one or more Search Console properties: seven built-in reports and any report of dimensions and a
search type you configure. Every search analytics stream is a `searchAnalytics.query` per site, day and
search type, and is incremental by `date`.

```bash
task test CONNECTOR=source-google-search-console     # no network, no credentials
task build CONNECTOR=source-google-search-console
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-google-search-console:dev check --config /data/config.json
```

Requests go to `https://searchconsole.googleapis.com/webmasters/v3/` (the `apiVersion` constant in
`client.go`): `GET sites/{site}`, `GET sites/{site}/sitemaps` and `POST sites/{site}/searchAnalytics/query`.
The site URL is escaped as one path segment, `:` and `/` included (`sites/https%3A%2F%2Fwww.example.com%2F`).

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `credentials` | yes | `{"auth_type": "service_account", "service_account_key": "<JSON key>"}` or `{"auth_type": "oauth", "client_id": "...", "client_secret": "...", "refresh_token": "..."}` |
| `site_urls` | yes | Search Console properties, exactly as Search Console names them: URL-prefix properties `https://www.example.com/` or `http://www.example.com/` (ending with `/`) and domain properties `sc-domain:example.com`. Listed once each; every stream reads each of them |
| `start_date` | no | First day read, `YYYY-MM-DD`; empty reads the last 16 months, all Search Console keeps |
| `end_date` | no | Last day read; empty reads up to today (UTC) |
| `lookback_days` | no | Days before the saved date an incremental read starts again (default 3) |
| `data_state` | no | `final` (default) reads finalized data only; `all` also reads fresh data that may still change |
| `custom_reports` | no | `[{"name": "...", "dimensions": [...], "search_type": "..."}]`, each read as its own stream |

```json
{
  "credentials": {
    "auth_type": "service_account",
    "service_account_key": "{\"type\": \"service_account\", \"client_email\": \"...\", \"private_key\": \"...\"}"
  },
  "site_urls": ["https://www.example.com/", "sc-domain:example.org"],
  "start_date": "2026-01-01",
  "custom_reports": [
    {"name": "discover_pages", "dimensions": ["page"], "search_type": "discover"},
    {"name": "image_queries_by_country", "dimensions": ["query", "country"], "search_type": "image"}
  ]
}
```

## Authentication

Both modes exchange their credential at `https://oauth2.googleapis.com/token` for an access token with the
`https://www.googleapis.com/auth/webmasters.readonly` scope, keep it until shortly before it expires and fetch
a new one when the API answers 401. The access token travels only in the `Authorization` header.

- **Service account**: in a Google Cloud project, enable the *Google Search Console API*
  (`searchconsole.googleapis.com`), create a service account and a JSON key for it, and paste the whole key
  as `service_account_key`. In Search Console, *Settings > Users and permissions*, add the service account's
  email as a user (*Restricted* is enough) of every site.
- **Google OAuth**: create an OAuth client (type *Desktop app* or *Web application*) in a Google Cloud
  project with the Search Console API enabled, authorize it once with the `webmasters.readonly` scope for a
  Google user who is a verified user of the sites (for example with the
  [OAuth Playground](https://developers.google.com/oauthplayground) using your own client), and copy the
  refresh token. The refresh token is reused as is; Google does not rotate it.

`check` gets an access token and reads every site with `sites.get`. A failed `check` names the cause:

| Message | Cause |
|---|---|
| `service_account_key is not a valid service account key` | The key is not JSON of type `service_account` with a PEM private key, or its `token_uri` is not `https://oauth2.googleapis.com/token` (the legacy `https://accounts.google.com/o/oauth2/token` and an absent one are accepted) or its `universe_domain` is not `googleapis.com` |
| `service account key is invalid, disabled or deleted` | The token endpoint rejected the signed assertion (`invalid_grant`) |
| `refresh_token is invalid or revoked` | The token endpoint answered `invalid_grant` |
| `client_id or client_secret is invalid` | The token endpoint answered `invalid_client` or `unauthorized_client` |
| `site <url> is not verified for service account <email>: add it as a user in Search Console` (or `for the Google user of the refresh token`) | 403 (other than a rate limit) or 404 for the site, or permission level `siteUnverifiedUser` |
| `Google Search Console API is not enabled in Google Cloud project <n>: enable searchconsole.googleapis.com` | 403 with reason `SERVICE_DISABLED` or `accessNotConfigured` |
| `the credentials lack the https://www.googleapis.com/auth/webmasters.readonly scope` | 403 with reason `ACCESS_TOKEN_SCOPE_INSUFFICIENT` |

## Streams

Namespace `google_search_console`.

| Stream | Sync modes | Primary key | Source |
|---|---|---|---|
| `sites` | full refresh | `site_url` | `sites.get` of each configured site: `site_url`, `permission_level` |
| `sitemaps` | full refresh | `site_url`, `path` | `sitemaps.list` of each configured site |
| `search_analytics_by_date` | full refresh, incremental | `site_url`, `search_type`, `date` | dimensions `date` |
| `search_analytics_by_query` | full refresh, incremental | ... , `query` | `date`, `query` |
| `search_analytics_by_page` | full refresh, incremental | ... , `page` | `date`, `page` |
| `search_analytics_by_country` | full refresh, incremental | ... , `country` | `date`, `country` |
| `search_analytics_by_device` | full refresh, incremental | ... , `device` | `date`, `device` |
| `search_analytics_by_search_appearance` | full refresh, incremental | ... , `search_appearance` | `date`, `searchAppearance` |
| `search_analytics_by_query_page` | full refresh, incremental | ... , `query`, `page` | `date`, `query`, `page` |

The built-in search analytics streams read the search types `web`, `image`, `video` and `news`, one request
each per site and day. `discover` and `googleNews` reject the `query` dimension, so they are read only
through custom reports.

A search analytics record has `site_url`, `search_type`, one key per dimension (`search_appearance` for
`searchAppearance`, `date` as `YYYY-MM-DD`), `clicks` and `impressions` (integers) and `ctr` and `position`
(numbers, the decimal literal the API sent, unchanged). The primary key is `site_url`, `search_type` and the
dimensions, so re-read days deduplicate.

A `sitemaps` record has `site_url`, `path`, `last_submitted` and `last_downloaded` (RFC 3339 timestamps or
`null`), `is_pending`, `is_sitemaps_index`, `type`, `warnings` and `errors` (integers; the API sends them as
strings) and `contents` (`[{"type", "submitted", "indexed"}]`, counts as integers, or `null`).

## Custom reports

Each entry of `custom_reports` becomes a stream named `name` (lowercase letters, digits and `_`, unique, not
the name of another stream). `dimensions` are any of `date`, `query`, `page`, `country`, `device` and
`searchAppearance`, each once; `date` is added as the first dimension when it is missing. `search_type` is
one of `web` (default), `image`, `video`, `news`, `discover` and `googleNews`; `discover` and `googleNews`
cannot use `query`. Filters are not supported.

## Incremental sync

The state of a search analytics stream holds, per site, the last day read:

```json
{"sites": {"https://www.example.com/": {"date": "2026-10-16"}, "sc-domain:example.org": {"date": "2026-10-16"}}}
```

A read covers, for each site, from the saved date minus `lookback_days` (default 3, since Search Console
keeps updating recent days), never before the start, up to `end_date` or today (UTC). The state is emitted
after each site-day, once every search type of the day is read; it never goes below the saved date, so a
sync that fails during the lookback days does not widen the next window. A full refresh reads from the start without
state.

Each site, day and search type is one query with `rowLimit` 25000, paged with `startRow` until a page shorter
than `rowLimit`. A read fails loudly, never skipping a row, on a page with more rows than `rowLimit`, a row
whose keys are not the requested dimensions, a `date` other than the requested day, a key set repeated
within the day, or `clicks` or `impressions` that are not whole numbers.

## Data Search Console leaves out

- **Anonymized queries.** Search Console omits queries issued by few users, to protect their privacy. Rows
  of reports grouped by `query` therefore add up to fewer clicks and impressions than
  `search_analytics_by_date` or `search_analytics_by_page` for the same day.
- **Row cap.** The API returns at most 50,000 rows per site, day and search type. A day that reaches it logs
  a `WARN` (`day hit the 50,000-row cap of Search Console; rows past it are not available`) naming the
  site, report, day and search type; the rows past the cap cannot be read through the API.
- **`data_state`.** With `final`, the last two or three days usually have no data yet; the lookback picks
  them up on a later sync. With `all`, fresh data is read and later replaced through the lookback.

## Quotas

Search Console allows 1,200 queries per minute per site and per user, plus daily limits. Requests are sent
one at a time, at least 50 ms apart. A 429, a 403 with reason `rateLimitExceeded`, `userRateLimitExceeded` or `quotaExceeded`, a 5xx
response or a transport error is sent again after a
doubling, jittered delay (2 s, 4 s, 8 s, 16 s), five times in all. An exhausted daily quota (reason
`dailyLimitExceeded`, or a 429 whose message names a per-day limit) fails the read at once: `daily quota of
Search Console API is exhausted; the next sync continues from the saved state`.
