# source-pipedrive

[Pipedrive](https://www.pipedrive.com) source, a manifest connector on the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON
schema per stream, no Go. Reads deals, persons, organizations, activities, products, pipelines, stages,
notes, files, leads and users from the [Pipedrive API](https://developers.pipedrive.com/docs/api/v1),
deals, persons, organizations and activities incrementally.

```bash
task test CONNECTOR=source-pipedrive     # fixtures, no network, no credentials
task build CONNECTOR=source-pipedrive
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-pipedrive:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `api_token` | yes | Personal API token, sent as the `api_token` query parameter (left out of every logged URL). The connector reads what its user can see |
| `replication_start_date` | no | RFC 3339 time, default `2010-01-01T00:00:00Z`; deals, persons, organizations and activities last updated before it are not read |

## Streams

| Stream | Endpoint | Sync modes | Cursor |
|---|---|---|---|
| `deals` | `GET /api/v2/deals?updated_since=<since>&sort_by=update_time&sort_direction=asc` | full refresh, incremental | `update_time` |
| `persons` | `GET /api/v2/persons`, the same parameters | full refresh, incremental | `update_time` |
| `organizations` | `GET /api/v2/organizations`, the same parameters | full refresh, incremental | `update_time` |
| `activities` | `GET /api/v2/activities`, the same parameters | full refresh, incremental | `update_time` |
| `products` | `GET /api/v2/products` | full refresh | |
| `pipelines` | `GET /api/v2/pipelines` | full refresh | |
| `stages` | `GET /api/v2/stages` | full refresh | |
| `notes` | `GET /api/v1/notes` | full refresh | |
| `files` | `GET /api/v1/files` | full refresh | |
| `leads` | `GET /api/v1/leads` | full refresh | |
| `users` | `GET /api/v1/users` | full refresh | |

Records are Pipedrive's objects as sent, primary key `id`. API v2 pages follow
`additional_data.next_cursor` with `limit=500`; v1 pages are `start`/`limit=500` and stop when
`additional_data.pagination.more_items_in_collection` is false; leads pages are 100 long and stop at a
shorter page; users answer in one response.

An incremental read sends `updated_since=replication_start_date` on the first sync and the saved cursor minus one hour
afterwards; records older than it are dropped by the engine too. Deleted objects are not listed by the
API and are not read.

`check` reads `users`. A rejected token (401) and a token without access (403) fail it with Pipedrive's
`error`.

## Differences from Airbyte's Pipedrive source

- Deals, persons, organizations, activities, products, pipelines and stages come from API v2, so their
  records have the v2 shape (`owner_id` and `person_id` as ids, custom fields under `custom_fields`), and
  incremental reads use v2's `updated_since` instead of v1 `/recents?since_timestamp`.
- `notes` and `files` stay on v1 (they have no v2 endpoint) and are full refresh. A v1 list with nothing in
  it answers `"data": null`, which reads as no records.
- API token only. OAuth would need a second auth method chosen by the configuration, which the manifest
  format does not have.
- The company domain (`<company>.pipedrive.com`) is not configurable; requests go to `api.pipedrive.com`.

## Schemas

Each schema declares the fields Pipedrive documents for the object, every field nullable except `id`,
with `additionalProperties: true` on every object so a field or custom field Pipedrive adds passes through.

## Tests

`manifest_test.go` runs `declarativetest.Run` (manifest lint, SDK conformance for full refresh and each
incremental stream, the 401 check, at least two pages per stream, one 429) over the synthetic fixtures in
`testdata/`, plus the 403 check, empty v1 lists (`"data": null`), the `updated_since` of the first and the
resumed read, the token in the query of every request, open schemas and a kept unknown field. A request is
answered by `testdata/<path>.json`, a later page by `testdata/<path>/cursor=<c>.json` (v2) or
`testdata/<path>/start=<n>.json` (v1). Everything in them is synthetic.