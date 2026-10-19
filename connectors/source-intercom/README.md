# source-intercom

[Intercom](https://www.intercom.com) source, a manifest connector run by the
[source-declarative](../source-declarative) engine. Reads a workspace's admins, teams, tags, segments, data
attributes, contacts and conversations from the
[REST API](https://developers.intercom.com/docs/references/rest-api/api.intercom.io), version 2.11. Every
stream is full refresh.

```bash
task test CONNECTOR=source-intercom      # no network, no credentials
task build CONNECTOR=source-intercom     # on the engine of the working tree
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-intercom:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `access_token` | yes | Access token of an Intercom app (Developer Hub > your app > Authentication). Sent only as `Authorization: Bearer <access_token>`, never in a URL |

```json
{"access_token": "<your access token>"}
```

Every request carries `Intercom-Version: 2.11`, so a change of the app's default API version does not change
what the connector reads. Requests go to `api.intercom.io`, the US data region; workspaces hosted in the EU
or Australia are not supported by this version.

### Permissions

The app needs read permissions for the streams it syncs: "Read admins" (admins, teams), "Read tags",
"Read and list users and companies" (contacts, segments, data attributes) and "Read conversations".

### Check

`check` lists the admins (`GET /admins`). A 401 (invalid token) or 403 (a token without "Read admins") fails
with the API's own message, such as `Access Token Invalid`.

## Streams

| Stream | Endpoint | Primary key | Paging |
|---|---|---|---|
| `admins` | `GET /admins` | `id` | one response |
| `teams` | `GET /teams` | `id` | one response |
| `tags` | `GET /tags` | `id` | one response |
| `segments` | `GET /segments?include_count=true` | `id` | one response |
| `data_attributes` | `GET /data_attributes` | `model`, `full_name` | one response |
| `contacts` | `GET /contacts` | `id` | `starting_after` cursor from `pages.next`, `per_page=150` |
| `conversations` | `GET /conversations` | `id` | `starting_after` cursor from `pages.next`, `per_page=150` |

Records are passed through as the API sends them; the schemas in `schemas/` declare the documented fields and
allow the rest.

`segments` carries each segment's `count` of members. `data_attributes` reads the attributes of contacts and
companies without `include_archived`, so archived attributes are not read.

### Not read

- `companies`: `GET /companies` sorts by `last_request_at`, which moves while the list is paged, so page
  boundaries skip or repeat companies; it lists at most 10,000 companies and leaves out companies without
  users. The unbounded `GET /companies/scroll` allows one open scroll per workspace and cannot resume after a
  server error.
- Conversation parts (one request per conversation, a fan-out under conversations), articles, help center,
  tickets and the data export API.

## No incremental sync

Contacts and conversations are full refresh only. Their list endpoints (`GET /contacts`,
`GET /conversations`) have no `updated_at` filter; Intercom filters them only through the search API
(`POST /contacts/search`, `POST /conversations/search`), which takes the filter and the page cursor in a JSON
request body. The manifest format describes GET requests only, so an incremental version of these streams is
a Go connector's job. The other streams are small configuration lists with no change
filter.

## Rate limits

A 429 is waited out by `Retry-After` when the response has it, else until `X-RateLimit-Reset` (Unix
seconds), else 10 s, five times before the read fails. 500, 502, 503 and 504 are retried three times with a doubling delay from 1 s.

## Tests

`manifest_test.go` is the engine's test entry: it loads `manifest.yaml`, serves `testdata/` from an
`httptest` server and runs the SDK conformance suite over every stream, the 401 check, a 429 once, and that
contacts and conversations each span two pages. `TestCheckForbidden` answers `GET /admins` with
`testdata/errors/403.json` and checks the message. The fixtures are synthetic; they can be re-recorded from
a free Intercom developer workspace.
