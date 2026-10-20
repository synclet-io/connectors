# source-twilio

[Twilio](https://www.twilio.com) source, a manifest connector on the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON
schema per stream, no Go. Reads accounts, incoming phone numbers, calls, messages, recordings, conferences
and daily usage records from the [Twilio REST API](https://www.twilio.com/docs/usage/api) (`2010-04-01`),
calls, messages, recordings, conferences and usage incrementally.

```bash
task test CONNECTOR=source-twilio     # fixtures, no network, no credentials
task build CONNECTOR=source-twilio
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-twilio:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `account_sid` | yes | SID of the account (`AC` and 32 hex digits), the basic auth username |
| `auth_token` | yes | Auth token of that account, the basic auth password |
| `start_date` | yes | RFC 3339 time; calls that ended, messages sent, recordings and conferences created and usage days before it are not read |

## Streams

| Stream | Endpoint | Sync modes | Cursor |
|---|---|---|---|
| `accounts` | `GET /Accounts.json`: the account and its subaccounts | full refresh | |
| `incoming_phone_numbers` | `GET /Accounts/{sid}/IncomingPhoneNumbers.json` | full refresh | |
| `calls` | `GET /Accounts/{sid}/Calls.json?EndTime%3E=<since>` | full refresh, incremental | `end_time` |
| `messages` | `GET /Accounts/{sid}/Messages.json?DateSent%3E=<since>&DateSent%3C=<until>`, in 30-day windows | full refresh, incremental | `date_sent` |
| `recordings` | `GET /Accounts/{sid}/Recordings.json?DateCreated%3E=<since>` | full refresh, incremental | `date_created` |
| `conferences` | `GET /Accounts/{sid}/Conferences.json?DateCreated%3E=<since day>` | full refresh, incremental | `date_created` |
| `usage_records` | `GET /Accounts/{sid}/Usage/Records/Daily.json?StartDate=<day>&EndDate=<day>`, in 30-day windows | full refresh, incremental | `start_date` |

`{sid}` is the configured `account_sid`. Records are Twilio's resources as sent, primary key `sid`
(`usage_records`: `account_sid`, `category`, `start_date`). Pages follow `next_page_uri` with
`PageSize=1000`. The filter parameters are Twilio's `EndTime>`, `DateSent>`, `DateSent<` and `DateCreated>` (sent URL-encoded), which mean "on or after" and "on or before".

Times are Twilio's RFC 2822 strings (`Fri, 06 Mar 2026 12:00:00 +0000`) and are compared as times. An
incremental read filters from `start_date` on the first sync and from the saved cursor minus one hour
afterwards; conferences filter by day, as the API accepts only a date there. Messages and usage records
are read in 30-day windows and the state is saved after each window, so a long first sync resumes where it
stopped. Calls are read by `end_time`, so a call still in progress is read by the sync after it ends;
the `EndTime` filter keeps calls without an end time out of the response. Usage days are re-read from the
last saved day, which Twilio keeps updating until the day is over.

`check` reads the first page of `accounts`. A rejected SID or token (401) and an account without access
(403) fail it with Twilio's `message`.

## Differences from Airbyte's Twilio source

- Calls, messages, recordings, conferences, usage records and phone numbers are read for the configured
  account only. Reading them for every subaccount needs a stream per parent account with a cursor per
  parent, which the manifest format has only for a full-refresh parent; the account's own data stays
  incremental instead.
- `alerts` (`monitor.twilio.com`) is not read: a manifest has one base URL, and the Monitor API is on another
  host.
- Records keep Twilio's RFC 2822 date strings; nothing is converted to RFC 3339.

## Schemas

Each schema declares the fields Twilio documents for the resource, every field nullable except the
primary key, with `additionalProperties: true` on every object so a field Twilio adds passes through.

## Tests

`manifest_test.go` runs `declarativetest.Run` (manifest lint, SDK conformance for full refresh and each
incremental stream, the 401 check, at least two pages per stream, one 429) over the synthetic fixtures in
`testdata/`, plus the 403 check, the filter value of the first and the resumed read, the message and
usage windows with a state per window, open schemas and a kept unknown field. Fixtures are routed by path
(`testdata/Accounts/<sid>/Calls.json`), later pages by `PageToken`, and a message or usage window outside
the fixtures' span gets `empty.json`. Everything in them is synthetic.
