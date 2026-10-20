# source-typeform

[Typeform](https://www.typeform.com) source for Synclet, a manifest connector run by the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON schema per
stream, no Go. Reads forms, their responses and webhooks, workspaces, images and themes through the
[Typeform APIs](https://www.typeform.com/developers/), responses incrementally.

```bash
task test CONNECTOR=source-typeform                   # no network, no credentials
go run ./cmd/manifest-lint ../source-typeform         # from connectors/source-declarative
task build CONNECTOR=source-typeform
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `access_token` | yes | Personal access token, or an OAuth access token, sent only as `Authorization: Bearer <token>`. Needs the scopes `forms:read`, `responses:read`, `webhooks:read`, `workspaces:read`, `images:read` and `themes:read` for the selected streams. |
| `start_date` | yes | RFC 3339 time; responses submitted before it are not read. |

```json
{"access_token": "<token>", "start_date": "2026-01-01T00:00:00Z"}
```

`check` requests the first page of `GET /workspaces`. Accounts on Typeform's EU data center
(`api.eu.typeform.com`) are not supported in this version.

## Streams

| Stream | Route | Sync modes | Primary key | Cursor |
|---|---|---|---|---|
| `forms` | `forms?page_size=200` | full refresh, incremental | `id` | `_read_at` |
| `responses` | `forms/<form>/responses?page_size=1000&since=` | full refresh, incremental | `response_id` | `submitted_at` |
| `webhooks` | `forms/<form>/webhooks` | full refresh | `id` | |
| `workspaces` | `workspaces?page_size=200` | full refresh | `id` | |
| `images` | `images` | full refresh | `id` | |
| `themes` | `themes?page_size=200` | full refresh | `id` | |

`responses` and `webhooks` run once per form listed by `forms`; every response carries `form_id`. Responses
are paged backwards from the newest with `before=<token of the last response>` until a page is empty; the
list pages are numbered (`page`) and stop at `total_items`. A form deleted between the listing and the read
of its responses answers 404 and is skipped.

Records are passed through as Typeform sends them, with one addition: every `forms` record carries
`_read_at`, the time of the read that listed it. It is the cursor of `forms`, and the reason `responses` can
be incremental at all: the engine runs a child stream incrementally only below an incremental parent, and a
form's own `last_updated_at` does not move when it receives a response, so it cannot gate which forms are
re-read. Every incremental read therefore lists every form and re-emits it (deduplicated by `id`).

## Incremental sync

`responses` keeps one cursor across all forms, the newest `submitted_at` it has read:

```json
{"submitted_at": "2026-10-01T09:30:05Z"}
```

The next read sends `since=<cursor minus 1h>` to every form and drops older responses. The pass over the
forms starts at the older of the `forms` and the `responses` cursors, so select `forms` incrementally too
(or leave it out of the catalog); with `forms` in full refresh every read requests responses from
`start_date` and filters them locally. A full refresh reads from `start_date` and saves no state.

The boundary responses read twice are deduplicated by `response_id`.

## Errors and rate limits

- 401 (bad token) and 403 (missing scope) fail `check` and every read as configuration errors with
  Typeform's `description`.
- 429 waits for `Retry-After`, or 5s when the header is absent, up to five times; Typeform allows two
  requests per second per account, and the engine sends one request at a time.
- 5xx responses and network errors are retried three times with a doubling delay from 1s.

## Tests

`manifest_test.go` runs the engine against hand-written synthetic fixtures in `testdata/` (three forms on two
pages, responses of one form across two `before` pages and an empty last page, a form without responses)
without network or credentials: the SDK conformance suite for full refresh and for each incremental stream,
`check` with 401 and 403, a one-time 429, responses and webhooks per form, `_read_at` on forms, and the
`since` sent on a first and on a resumed read.
