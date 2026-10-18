# source-google-analytics-data-api

[Google Analytics Data API](https://developers.google.com/analytics/devguides/reporting/data/v1) source built
on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads daily Google Analytics 4 (GA4)
reports of one or more properties: eight built-in reports and any report of dimensions and metrics you
configure. Every stream is a `runReport` request per property and day; every stream is incremental by
`date`.

```bash
task test CONNECTOR=source-google-analytics-data-api     # no network, no credentials
task build CONNECTOR=source-google-analytics-data-api
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-google-analytics-data-api:dev check --config /data/config.json
```

Requests go to Data API `v1beta` (the `apiVersion` constant in `client.go`):
`POST /v1beta/properties/{id}:runReport` and `GET /v1beta/properties/{id}/metadata`.

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `credentials` | yes | `{"auth_type": "service_account", "service_account_key": "<JSON key>"}` or `{"auth_type": "oauth", "client_id": "...", "client_secret": "...", "refresh_token": "..."}` |
| `property_ids` | yes | GA4 property ids, with or without the `properties/` prefix. Every report reads each of them |
| `start_date` | yes | First day read, `YYYY-MM-DD` |
| `end_date` | no | Last day read; empty reads up to today (UTC) |
| `lookback_days` | no | Days before the saved date an incremental read starts again (default 3) |
| `custom_reports` | no | `[{"name": "...", "dimensions": [...], "metrics": [...]}]`, each read as its own stream |
| `data_quality_fields` | no | Add the sampling and thresholding fields to every record (default `true`) |

```json
{
  "credentials": {
    "auth_type": "service_account",
    "service_account_key": "{\"type\": \"service_account\", \"client_email\": \"...\", \"private_key\": \"...\"}"
  },
  "property_ids": ["123456789"],
  "start_date": "2026-01-01",
  "custom_reports": [
    {
      "name": "revenue_by_plan",
      "dimensions": ["customEvent:plan_tier", "country"],
      "metrics": ["totalRevenue", "sessions"]
    }
  ]
}
```

## Authentication

Both modes exchange their credential at `https://oauth2.googleapis.com/token` for an access token with the
`https://www.googleapis.com/auth/analytics.readonly` scope, keep it until shortly before it expires and fetch
a new one when the API answers 401. The access token travels only in the `Authorization` header.

- **Service account**: in a Google Cloud project, enable the *Google Analytics Data API*
  (`analyticsdata.googleapis.com`), create a service account and a JSON key for it, and paste the whole key
  as `service_account_key`. In Google Analytics, *Admin > Property access management*, add the service
  account's email with the *Viewer* role on every property.
- **Google OAuth**: create an OAuth client (type *Desktop app* or *Web application*) in a Google Cloud
  project with the Data API enabled, authorize it once with the `analytics.readonly` scope for a Google user
  with *Viewer* on the properties (for example with the
  [OAuth Playground](https://developers.google.com/oauthplayground) using your own client), and copy the
  refresh token. The refresh token is reused as is; Google does not rotate it.

`check` gets an access token and reads the metadata of every property. A failed `check` names the cause:

| Message | Cause |
|---|---|
| `service_account_key is not a valid service account key` | The key is not JSON of type `service_account` with a PEM private key |
| `service account key is invalid, disabled or deleted` | The token endpoint rejected the signed assertion (`invalid_grant`) |
| `refresh_token is invalid or revoked` | The token endpoint answered `invalid_grant` |
| `client_id or client_secret is invalid` | The token endpoint answered `invalid_client` or `unauthorized_client` |
| `Google Analytics Data API is not enabled in Google Cloud project <n>: enable analyticsdata.googleapis.com` | 403 with reason `SERVICE_DISABLED` |
| `the credentials lack the https://www.googleapis.com/auth/analytics.readonly scope` | 403 with reason `ACCESS_TOKEN_SCOPE_INSUFFICIENT` |
| `no access to property <id>: grant the service account or user Viewer on it` | 403 `PERMISSION_DENIED` without a reason, or 404, for the property |
| `permission denied for property <id>` | Any other 403, with Google's reason and message |
| `custom report <r>: property <id> has no dimension <x>` (or `metric`) | A configured field the property's metadata does not list |

## Streams

Namespace `google_analytics`. `date` is the first dimension of every report. Every stream supports full
refresh and incremental with the source-defined cursor `date`, and its primary key is `property_id` plus
every dimension, so re-read days deduplicate.

| Stream | Dimensions | Metrics |
|---|---|---|
| `website_overview` | `date` | `totalUsers`, `newUsers`, `sessions`, `sessionsPerUser`, `averageSessionDuration`, `screenPageViews`, `screenPageViewsPerSession`, `bounceRate` |
| `daily_active_users` | `date` | `active1DayUsers` |
| `traffic_sources` | `date`, `sessionSource`, `sessionMedium` | as `website_overview` |
| `pages` | `date`, `hostName`, `pagePathPlusQueryString` | `screenPageViews`, `bounceRate` |
| `events` | `date`, `eventName` | `eventCount`, `totalUsers`, `eventCountPerUser` |
| `devices` | `date`, `deviceCategory`, `operatingSystem`, `browser` | as `website_overview` |
| `locations` | `date`, `region`, `country`, `city` | as `website_overview` |
| `key_events` | `date`, `eventName` | `keyEvents` |

Each record has `property_id`, one key per dimension and metric, and, with `data_quality_fields`, the four
fields of [Sampling and thresholding](#sampling-and-thresholding). Record keys are the API names with every
character outside `[A-Za-z0-9_]` replaced by `_` (`customEvent:plan_tier` becomes `customEvent_plan_tier`);
two fields of one report that end up with the same key fail the configuration. `date` is emitted as
`YYYY-MM-DD`; other dimensions are strings.

## Custom reports

Each entry of `custom_reports` becomes a stream named `name` (lowercase letters, digits and `_`, unique, not
the name of a built-in stream). `date` is added as the first dimension when it is missing; a report has at
most 9 dimensions including `date`, and 1 to 10 metrics. Custom dimensions and metrics
(`customEvent:...`, `customUser:...`) work like built-in ones. Filters are not supported.

## Number types

Field types come from the `metadata` of every configured property, never from a table in the connector. A
metric of `TYPE_INTEGER` is an `integer` and is emitted as a 64-bit integer; every other metric type
(`TYPE_FLOAT`, `TYPE_CURRENCY`, `TYPE_SECONDS` and the rest) is a `number` and is emitted as the decimal
literal the API sent, unchanged, so no precision is lost. A metric whose type differs between two
configured properties fails discover. A read fails when a response's metric type disagrees with the schema
or a value does not parse as its type.

## Incremental sync

The state of a stream holds, per property, the last day read:

```json
{"properties": {"123456789": {"date": "2026-10-16"}}}
```

A read covers, for each property, from the saved date minus `lookback_days` (default 3, since Google
Analytics keeps processing recent days), never before `start_date`, up to `end_date` or today (UTC). Each
day is one `runReport` with `limit` 250000, paged with `offset` until all `rowCount` rows are read; an empty
page before that, or a `rowCount` that changes between pages, fails the read. The state is emitted after
each day. A full refresh reads from `start_date` without state.

## Sampling and thresholding

Google Analytics may compute a report from a sample of the data, withhold rows with few users
(thresholding), or bucket rows into `(other)`. The data of such a day may be incomplete. With
`data_quality_fields` (the default) every record carries:

| Key | Meaning |
|---|---|
| `sampled` | The report was computed from a sample |
| `sampling_ratio` | Samples read divided by the sampling space, as an exact decimal (rounded to 18 places when it has no finite expansion); `null` when not sampled |
| `subject_to_thresholding` | Rows may be withheld by thresholding |
| `data_loss_from_other_row` | Some rows were bucketed into `(other)` |

Whatever `data_quality_fields` says, a day with any of these set logs a `WARN` naming the property, the
report and the day.

## Quotas

Requests are sent one at a time with `returnPropertyQuota: true`. When a response shows fewer hourly
property tokens remaining than it consumed, the connector logs a `WARN` and waits for the next full hour
before the property's next request; when that happens a third time in a row, the read fails. When the daily
tokens are exhausted (fewer remaining than consumed, or a 429 naming the daily tokens), the connector logs a
`WARN` and the read fails before the property's next request, naming the property and day; the next sync continues from the saved
state. Any other 429, a 5xx response or a transport error is sent again after a doubling, jittered delay
(2 s, 4 s, 8 s, 16 s), five times in all.
