# source-amplitude

[Amplitude](https://amplitude.com) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads raw events through the
[Export API](https://amplitude.com/docs/apis/analytics/export) and daily metrics, cohorts,
annotations and the event list through the
[Dashboard REST API](https://amplitude.com/docs/apis/analytics/dashboard-rest).

```bash
task test CONNECTOR=source-amplitude     # no network, no credentials
task build CONNECTOR=source-amplitude
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-amplitude:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `api_key` | yes | API key of the project (Settings > Projects) |
| `secret_key` | yes | Secret key of the project |
| `start_date` | yes | UTC time `YYYY-MM-DDTHH:mm:ssZ` to read from; a plain `YYYY-MM-DD` is midnight |
| `data_region` | no | `Standard Server` (default, `amplitude.com`) or `EU Residency Server` (`analytics.eu.amplitude.com`) |
| `request_time_range` | no | Hours of events per Export API request, 1 to 8760; default 24 |
| `active_users_group_by_country` | no | Break `active_users` down by country; default `true` |

```json
{"api_key": "<api key>", "secret_key": "<secret key>", "start_date": "2026-01-01T00:00:00Z"}
```

The keys are sent as HTTP basic credentials. `check` lists the project's annotations; a 401 fails
with `invalid api_key or secret_key, or a data_region the project is not in`, since a project of the
other region answers 401 too.

## Streams

| Stream | Request | Sync modes | Primary key | Cursor |
|---|---|---|---|---|
| `events` | `GET /api/2/export` | full refresh, incremental | `uuid` | `server_upload_time` |
| `active_users` | `GET /api/2/users?m=active&i=1[&g=country]` | full refresh, incremental | `date` | `date` |
| `average_session_length` | `GET /api/2/sessions/average` | full refresh, incremental | `date` | `date` |
| `cohorts` | `GET /api/3/cohorts` | full refresh | `id` | |
| `annotations` | `GET /api/2/annotations` | full refresh | `id` | |
| `events_list` | `GET /api/2/events/list` | full refresh | `name` | |

Schemas declare the documented fields and allow undeclared ones, because Amplitude adds fields
without notice. Numbers are passed on as the API sends them.

### events

The Export API selects events by `server_upload_time` and answers a zip archive with one gzipped
file of JSON lines per hour (`<project>_<YYYY-MM-DD>_<H>#<N>.json.gz`). The connector downloads the
archive to a temporary file, reads its files in hour order and emits one record per line.

- **Windows.** Requests cover `request_time_range` hours (`start` and `end` are inclusive hours,
  `YYYYMMDDTHH`), from the hour of `start_date` up to the last hour that ended at least two hours
  ago. Amplitude may add events to the export of an hour for up to about two hours, so later hours
  are left for the next sync instead of being read incomplete.
- **404** means the window has no events; the read moves on.
- **400** is how the API rejects an export that would be too large: the window is split in half and
  each half requested, down to single hours. A single hour that is still rejected fails the read.
- **Timestamps.** `client_event_time`, `client_upload_time`, `event_time`, `processed_time`,
  `server_received_time`, `server_upload_time` and `user_creation_time` come as
  `YYYY-MM-DD HH:MM:SS.ffffff` in UTC and are emitted as RFC 3339 (`2026-10-16T08:15:00.123Z`);
  a value in another form is kept as it is.
- **State.** After each window an incremental read saves the hour the next window starts at;
  every event uploaded before it has been read. The next read starts there, or at `start_date` if
  that is later:

  ```json
  {"server_upload_time": "2026-10-18T10:00:00Z"}
  ```

### active_users and average_session_length

One request per 30 days (`active_users`) or 15 days (`average_session_length`), `start` and `end`
as `YYYYMMDD`, from the day of `start_date` up to today (UTC). A response holds `series[j][i]`, the
value of series `j` on day `xValues[i]`:

- `active_users` emits `{"date": "2026-10-17", "statistics": {"United States": 75, "Germany": 55}}`:
  each series under its label (the country with `g=country`, the last element of a list label such
  as `[0, "Germany"]`, the series index when there is no label).
- `average_session_length` emits `{"date": "2026-10-17", "length": 330}` from the first series.

Days before the start are not emitted. After each window an incremental read saves its last day,
`{"date": "2026-10-18"}`. The counts of that day may still change, so the next read starts on the
saved day again (or on `start_date` if that is later); `date` is the primary key, so deduplicated
destinations keep the latest counts.

### Rate limits and retries

A 429, a 5xx or a network error is retried up to six attempts in all, waiting 2s, 4s, 8s, 16s and
32s, or the `Retry-After` the response asks for (at most 10 minutes). 400, 401, 403 and 404 are not
retried; 401 and 403 fail with a `config_error`.

## Compared with Airbyte's source-amplitude

The streams, their primary keys, cursors and the configuration keys follow Airbyte's connector.
These choices are this connector's own:

- events are read only up to the last hour that ended two hours ago, and the state is the window
  boundary rather than the greatest `server_upload_time` read, so an hour Amplitude has not
  finished is never saved as read;
- a window the Export API rejects with 400 is split in half instead of failing;
- event timestamps are emitted as RFC 3339 instead of Amplitude's `YYYY-MM-DD HH:MM:SS.ffffff`;
- `active_users` and `average_session_length` restart on the saved day, so a day read while it was
  still running is corrected;
- `data_region` takes exactly the two documented values and is checked before any request.

## Tests

Tests replay HTTP from hand-written fixtures; they need no network and no credentials (see
[CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)).

- **Layout:** `testdata/<path>.json` answers `GET /<path>` (`testdata/3/cohorts.json` answers
  `GET /3/cohorts`). `testdata/2/users.json`, `users_country.json` and `2/sessions/average.json`
  hold five days that the fixture server cuts to the requested `start` and `end`.
  `testdata/2/export/<YYYYMMDDTHH>.jsonl` holds the events uploaded in one hour; the fixture server
  zips and gzips the hours of a request into an archive named like Amplitude's, and answers 404
  when there is none. Error bodies live in `testdata/errors/`. All values are synthetic.
- **Helper:** `fixtures_test.go` holds `fixtureServer`, which answers 401 to a request without the
  basic credentials `test-api-key` / `test-secret-key`, fails the test when a credential appears in
  a query, lets a test override one path (or one export window), and records each request.
  `testSource` points a `Source` at it through its unexported fields (`baseURL`, `now` fixed at
  2026-10-18 12:00 UTC, `client`, `retryDelay`), which are zero in `main`.
- `TestConformance` runs the SDK conformance suite on every stream for full refresh and incremental.
- `TestIntegration` runs `check` and an incremental read of every stream against a real project when
  `AMPLITUDE_TEST_CONFIG` names a config file, and is skipped otherwise. It reads events from the
  config's `start_date`, so point that at a recent hour.
