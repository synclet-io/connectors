# source-sentry

[Sentry](https://sentry.io) source, a manifest connector on the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON
schema per stream, no Go. Reads the projects the token sees, the configured project, its issues and
events, and the organization's releases from the [Sentry API](https://docs.sentry.io/api/), issues,
events and releases incrementally. Works with sentry.io, its region hosts and self-hosted Sentry.

```bash
task test CONNECTOR=source-sentry     # fixtures, no network, no credentials
task build CONNECTOR=source-sentry
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-sentry:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `auth_token` | yes | Auth token with `project:read`, `event:read` and `org:read`, sent as `Authorization: Bearer <token>` |
| `hostname` | yes | Host of the Sentry instance, default `sentry.io`; requests go to `https://<hostname>/api/0/` |
| `organization` | yes | Organization slug |
| `project` | yes | Project slug; issues and events are read from it |
| `start_date` | yes | RFC 3339 time; issues last seen, events received and releases created before it are not read |

## Streams

| Stream | Endpoint | Sync modes | Cursor |
|---|---|---|---|
| `projects` | `GET /projects/` | full refresh | |
| `project_detail` | `GET /projects/{organization}/{project}/` | full refresh | |
| `issues` | `GET /projects/{organization}/{project}/issues/?query=lastSeen:>=<since>` | full refresh, incremental | `lastSeen` |
| `events` | `GET /projects/{organization}/{project}/events/?full=true&start=<since>&end=<until>` | full refresh, incremental | `dateCreated` |
| `releases` | `GET /organizations/{organization}/releases/` | full refresh, incremental | `dateCreated` |

Records are Sentry's objects as sent, primary key `id`. Pages follow the `Link` header's `rel="next"`.
`<since>` is `start_date` on the first sync and the saved cursor minus one hour afterwards (none for
releases), in UTC without a zone (`2026-01-01T00:00:00`); records older than it are dropped by the engine
too. The issue query replaces Sentry's default `is:unresolved`, so resolved and ignored issues are read.
An issue is matched by when it was last seen, so a status change without a new event is picked up only by
a full refresh. Releases have no time filter: every sync lists them and keeps those created since the
cursor.

`check` reads `project_detail`. A rejected token (401) and a token without access to the project (403)
fail it with Sentry's `detail`. A 429 waits for `Retry-After` or `X-Sentry-Rate-Limit-Reset`.

## Differences from Airbyte's Sentry source

- Sentry always sends a `rel="next"` link and marks the end with `results="false"`; the manifest format
  follows `rel="next"` only, so each stream sends one more request, which answers an empty page.
- Records keep the API's field names and values; nothing is renamed or converted.

## Schemas

Each schema declares the fields Sentry documents for the object, every field nullable except `id`, with
`additionalProperties: true` on every object so a field Sentry adds passes through.

## Tests

`manifest_test.go` runs `declarativetest.Run` (manifest lint, SDK conformance for full refresh and each
incremental stream, the 401 check, at least two pages per stream, one 429) over the synthetic fixtures in
`testdata/`, plus the 403 check, the issue query and event bounds of the first and the resumed read, the
releases a resumed read keeps, open schemas and a kept unknown field. A request is answered by
`testdata/<path>.json` (without the trailing slash), a later page by `testdata/<path>/cursor=<cursor>.json`,
with the `Link` header in `<file>.headers.json`. Everything in them is synthetic.
