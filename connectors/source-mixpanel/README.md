# source-mixpanel

[Mixpanel](https://mixpanel.com) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads one project's raw events from the
[Export API](https://developer.mixpanel.com/reference/raw-event-export) and its user profiles, cohorts and
their members, annotations, saved funnels and revenue from the
[Query API](https://developer.mixpanel.com/reference/query-api).

```bash
task test CONNECTOR=source-mixpanel     # no network, no credentials
task build CONNECTOR=source-mixpanel
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-mixpanel:dev check --config /data/config.json
```

## Configuration

| Key | Required | Default | Meaning |
|---|---|---|---|
| `credentials` | yes | | `{"option_title": "Service Account", "username", "secret", "project_id"}` or `{"option_title": "Project Secret", "api_secret"}` |
| `region` | no | `US` | `US` (mixpanel.com, data.mixpanel.com) or `EU` (eu.mixpanel.com, data-eu.mixpanel.com) |
| `project_timezone` | no | `US/Pacific` | IANA zone of the project. `from_date` and `to_date` are days of this zone, and so is "today" |
| `start_date` | no | a year before today | First day read by `export`, `funnels`, `revenue` and `annotations`; `YYYY-MM-DD` or `YYYY-MM-DDTHH:MM:SSZ` (the date is used) |
| `end_date` | no | today | Last day read; a later day reads to today |
| `attribution_window` | no | `5` | Days before the saved day an incremental read of `funnels` and `revenue` starts again |
| `export_lookback_window` | no | `0` | Seconds before the saved event time from which an incremental read of `export` emits events again |
| `date_window_size` | no | `30` | Days per request of the date-ranged endpoints; a state is emitted after each window |
| `page_size` | no | `1000` | Profiles per `engage` request, 1 to 1000 |

```json
{
  "credentials": {"option_title": "Service Account", "username": "etl.1a2b3c.mp-service-account", "secret": "<secret>", "project_id": 2195193},
  "region": "US",
  "project_timezone": "US/Pacific",
  "start_date": "2026-01-01"
}
```

Credentials travel only as HTTP Basic auth (`username:secret`, or `api_secret:` for a project secret), never
in a URL. A service account sends `project_id` as a query parameter on every request and needs a project
role of Analyst or above. The keys match Airbyte's Mixpanel source, so an
Airbyte configuration works unchanged; Airbyte's `select_properties_by_default` is not read. A `credentials` object without `option_title`, which Airbyte accepts, is a project secret
when it has `api_secret` and a service account otherwise.

### Check

`check` validates the configuration and lists the cohorts (`POST /api/2.0/cohorts/list`), one Query API
request. It fails with `invalid credentials` on a 401, `the credentials have no access to this project` on a
403, and `check the project_id and region` on a 400, which Mixpanel returns for a project the service account
is not a member of or a project in the other region.

## Streams

| Stream | Endpoint | Modes | Cursor | Primary key |
|---|---|---|---|---|
| `export` | `GET data.mixpanel.com/api/2.0/export` | full refresh, incremental | `time` | none |
| `engage` | `POST /api/2.0/engage` | full refresh, incremental | `last_seen` | `distinct_id` |
| `cohorts` | `POST /api/2.0/cohorts/list` | full refresh, incremental | `created` | `id` |
| `cohort_members` | `POST /api/2.0/engage` with `filter_by_cohort`, per cohort | full refresh, incremental | `last_seen` | `cohort_id`, `distinct_id` |
| `annotations` | `GET /api/2.0/annotations` | full refresh | | `id` |
| `funnels` | `GET /api/2.0/funnels/list`, then `GET /api/2.0/funnels?unit=day` per funnel | full refresh, incremental | `date` | `funnel_id`, `date` |
| `revenue` | `GET /api/2.0/engage/revenue?unit=day` | full refresh, incremental | `date` | `date` |

Schemas set `additionalProperties: true`; a field the API adds later is kept.

### Property names and types

`discover` asks `GET /api/2.0/events/properties/top?limit=1000` for the event property names and
`GET /api/2.0/engage/properties` for the profile property names with their types; they become the fields of
`export`, `engage` and `cohort_members`. A property name loses its leading `$` (`$browser` becomes
`browser`), unless the project also has the name without it, which keeps it as `_browser`. A property
whose name equals a fixed field of its stream gets the prefix `property_` (a custom `event` property becomes
`property_event`). A read maps names as the configured catalog's schema does, so a record lacking `browser`
still writes `$browser` to `_browser`.

- `export` records are `event`, `time`, `distinct_id`, `insert_id` (from `$insert_id`) and every other event
  property. `time` becomes an RFC 3339 timestamp in UTC (seconds, or milliseconds for values of 10^11 and
  above). Mixpanel properties have no fixed type, so every other property is a nullable string: strings as
  sent, numbers and booleans as their text, lists and objects as JSON.
- `engage` and `cohort_members` records are `distinct_id`, `last_seen` (from `$last_seen`) and the profile
  properties, typed from `engage/properties`: `number`, `boolean`, `list` (array), `object` and `datetime`
  (`YYYY-MM-DDTHH:MM:SS` in the project zone, a timestamp without time zone). A `string` or unknown type is a
  nullable string, and a value of another JSON type becomes its JSON text. `cohort_members` adds `cohort_id`.
- `cohorts` and `annotations` records are the API's objects as sent. `funnels` records are `funnel_id`,
  `name`, `date`, `steps` and `analysis` of one day. `revenue` records are `date`, `amount`, `count` and
  `paid_count` of one day; the `$overall` total is left out.

### Incremental reads

- `export` saves the latest event `time`. The next read requests from the project day of that time minus
  `export_lookback_window` seconds, sends `where=properties["$time"]>=datetime(<that moment>)` and emits
  only events at or after it. With the default `0` only the events at the saved time itself are emitted
  again; a larger window re-reads late events, which a destination can deduplicate on `insert_id`
  (Mixpanel deduplicates on `event`, `distinct_id`, `time` and `insert_id`).
- `funnels` saves the last day per funnel and `revenue` the last day; both start again `attribution_window`
  days before it.
- These three stream their date range in windows of `date_window_size` days and emit the state after each
  window, so an interrupted read resumes at the last finished window. The state never goes below the saved
  cursor.
- `engage`, `cohort_members` and `cohorts` cannot be filtered by change time: every read pages through all
  profiles or cohorts, and an incremental read emits only those whose `last_seen` (`created`) is at or after
  the saved one, plus profiles without `$last_seen`. `engage` and `cohorts` emit the state once the scan is
  done. `cohort_members` saves the latest `last_seen` of each cohort (`{"cohorts": {"<id>": ...}}`, with the
  earliest of them as `last_seen`) and emits the state after each cohort; a cohort missing from the state is
  read in full, and a state without `cohorts` applies its `last_seen` to every cohort. A user who joins a
  cohort without a newer `$last_seen` is not re-emitted; use full refresh to track membership exactly.

`engage` pages with `page_size` and the `session_id` of the first page, raising `page` until a page holds
fewer profiles than its `page_size`.

## Rate limits

Mixpanel allows a project 60 Export API and 60 Query API requests per rolling hour, at most 3 to 5 at once.
The connector sends one request at a time and keeps a rolling hour of each API: a 61st request waits until the
first of the last 60 is an hour old. Plan for it: the default year of `export` is 13 requests, but `funnels`
costs one request per funnel and window, and `engage` one per 1,000 profiles, so a large project's first sync
can take hours.

A 429 waits for its `Retry-After`, else one minute doubling up to 15 minutes, and is repeated up to 8 times
(other clients of the project share the budget); then the read fails naming the endpoint. A 5xx response or a
network error is retried three times after 1s, 2s and 4s. 400, 401 and 403 are not retried; 401 and 403 are
configuration errors.

## Tests

Tests replay HTTP from hand-written fixtures; they need no network and no credentials (see
[CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)). All values are synthetic
(`user-001`, `example.com` emails, project `1234567`).

- **Layout:** the Query API path `<p>` is answered by `testdata/<p>.json`. `export` is served from
  `testdata/export.jsonl`, `funnels` from `testdata/funnels/<funnel_id>.json` and `engage/revenue` from
  `testdata/engage/revenue.json`, each filtered to the requested `from_date`..`to_date` (days in US/Pacific).
  `engage` pages are `testdata/engage/page-<n>.json`, or `cohort-<id>-page-<n>.json` with `filter_by_cohort`.
  Error bodies live in `testdata/errors/`.
- **Helper:** `fixtures_test.go` holds `fixtureServer`, which serves both APIs, answers 401 to other
  credentials and 400 to a service account request without the test `project_id`, fails the test when a
  secret appears in a URL, lets a test override one path's status, file and headers (optionally for its
  first requests only), and records every request with its query and form values. `testSource` points a
  `Source` at it through its unexported fields (`queryURL`, `exportURL`, `client`, `now`, `retryDelay`,
  `wait`) on a fake clock whose `wait` records each duration and moves the clock instead of sleeping.
- `TestConformance` runs the SDK conformance suite in full refresh and incremental mode (with the
  attribution window as the lookback of `funnels` and `revenue`); the read, discover, client and check
  tests cover every stream and mode, the property renaming, the windows, pagination, retries and the
  hourly budget.
- `TestLive` runs check, discover and a full refresh of `cohorts` and `annotations` against a real project
  when `MIXPANEL_TEST_CONFIG` names a config file, and is skipped otherwise.
