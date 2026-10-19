# source-klaviyo

[Klaviyo](https://www.klaviyo.com) source, a manifest connector on the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON
schema per stream, no Go. Reads profiles, events, campaigns, flows, lists, metrics and segments from the
[Klaviyo API](https://developers.klaviyo.com/en/reference/api_overview), profiles and events incrementally.

```bash
task test CONNECTOR=source-klaviyo     # fixtures, no network, no credentials
task build CONNECTOR=source-klaviyo
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-klaviyo:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `api_key` | yes | Private API key, sent only as `Authorization: Klaviyo-API-Key <key>`. Needs the read scopes `profiles`, `events`, `campaigns`, `flows`, `lists`, `metrics` and `segments` |
| `start_date` | yes | RFC 3339 time; profiles updated and events that happened before it are not read |

Every request carries the pinned API revision `revision: 2025-07-15`.

## Streams

| Stream | Endpoint | Sync modes | Cursor |
|---|---|---|---|
| `profiles` | `GET /api/profiles` with `additional-fields[profile]=subscriptions,predictive_analytics` | full refresh, incremental | `attributes.updated` |
| `events` | `GET /api/events` | full refresh, incremental | `attributes.datetime` |
| `campaigns` | `GET /api/campaigns`, once per channel (`email`, `sms`, `mobile_push`), as the API requires a channel filter | full refresh | |
| `flows` | `GET /api/flows` | full refresh | |
| `lists` | `GET /api/lists` | full refresh | |
| `metrics` | `GET /api/metrics` | full refresh | |
| `segments` | `GET /api/segments` | full refresh | |

Records are the JSON:API resources as Klaviyo sends them (`type`, `id`, `attributes`, `relationships`,
`links`), primary key `id`. Pages follow `links.next`.

An incremental read sends `filter=greater-than(updated,<since>)` (profiles) or
`filter=greater-than(datetime,<since>)` (events), where `<since>` is `start_date` on the first sync and
the saved cursor minus one hour afterwards; records older than `<since>` are dropped by the engine too.

`check` reads the first page of `metrics`. A rejected key (401) and a key without a scope (403) fail it
with Klaviyo's error `detail`.

## Schemas

Each schema declares every attribute Klaviyo documents for the pinned revision and sets
`additionalProperties: true` on every object, so an attribute a later revision adds passes through
instead of being dropped. `TestSchemasKeepEveryAttribute` holds the documented list.

## Tests

`manifest_test.go` runs `declarativetest.Run` (manifest lint, SDK conformance for full refresh and each
incremental stream, the 401 check, at least two pages per stream, one 429) over the synthetic fixtures in
`testdata/`, plus the 403 check, the resumed `filter` value and the schema rules. The fixtures are
synthetic and can be re-recorded from a free Klaviyo account; campaigns are routed by the channel of their
filter (`testdata/campaigns/<channel>.json`).
