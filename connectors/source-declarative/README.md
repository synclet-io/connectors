# source-declarative

The engine of Synclet's declarative HTTP sources: one Go image, built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk), that runs a `manifest.yaml`. A manifest
connector is its own directory, image, version and registry entry, `FROM` this image plus its baked-in
manifest (see [CONTRIBUTING](../../CONTRIBUTING.md#a-manifest-connector)). The released image ships no
manifest: it is the generic image of section 13, which runs the manifest a config carries in
`__manifest`. It is hidden from the catalog (`"tombstone": true` in `meta.json`).

```bash
task test CONNECTOR=source-declarative                 # no network, no credentials
go run ./cmd/manifest-lint <dir>                         # every problem of <dir>/manifest.yaml, with its path
go run ./cmd/manifest-lint -spec <dir>                   # the spec.json the manifest maps to
```

[`example/source-acme`](example/source-acme) is a complete two-stream manifest connector (one schema in a file, one inline) with fixtures and its `declarativetest` entry. `testdata/source-openexchangerates` and `testdata/source-revenuecat` re-express two Go connectors and are diffed against them by `proof_test.go`. The format's JSON Schema is
[`internal/manifest/manifest.schema.json`](internal/manifest/manifest.schema.json); editors that read
`yaml-language-server` comments validate a manifest with
`# yaml-language-server: $schema=../source-declarative/internal/manifest/manifest.schema.json`.

## The manifest format, version 1

Approved by the user on 2026-10-19.

Where the SDK has no field for it, a spec property's `enum` and `pattern` stay out of `spec.json`; the
engine still validates every config against them.

### 1. Scope and rules

The DSL describes **GET requests that return JSON**, how to page through them, which part of the body is the records, how to ask for records changed since a time, and how to retry. It is for the long tail of simple SaaS APIs (Stripe, Chargebee, Jira, Klaviyo, Intercom, GitHub, exchange-rate feeds).

Rules:

1. Anything a manifest cannot say is written in Go on the SDK. The engine never grows for one connector; a feature joins the DSL when at least two connectors in the catalog or its candidates need it.
2. A manifest fails at load on any key the format does not know, any value of the wrong type and any reference that does not resolve (a config key not in the spec, a parent stream that does not exist, a field not in the schema). Errors name the path (`streams[2].pagination.cursor_from`).
3. Records are passed through as the API sends them. The engine decodes with exact numbers, adds the fields `records.add` names and nothing else. Schemas are hand-written JSON Schema, in a file or inline (3.5); the engine does not rewrite them, and a published connector never infers them. Only the builder's infer mode (section 14) drafts a schema, for a human to edit.
4. The engine is loud about loops: a cursor or next URL that repeats, a page identical to the previous one, or more than `max_pages` pages fails the stream with the request that caused it.
5. Templates are a fixed set of substitutions, not a language (section 4).
6. Requests are sequential: one stream at a time, one request in flight. A rate limit is honoured by waiting for what the API says, never by pacing.

### 2. A manifest at a glance

```yaml
version: 1
connector:
  name: source-stripe
  documentation_url: https://github.com/synclet-io/connectors/tree/main/connectors/source-stripe
base_url: https://api.stripe.com/v1/
spec:
  title: Stripe Source Spec
  required: [api_key, start_date]
  properties:
    api_key:    {type: string, title: API key, secret: true, order: 0}
    start_date: {type: string, title: Start date, format: date-time, examples: ["2026-01-01T00:00:00Z"], order: 1}
auth: {type: bearer, token: "{{ config.api_key }}"}
headers: {Stripe-Version: "2025-08-27.basil"}
errors: {message: $.error.message}
check: {stream: customers}
streams:
  - name: charges
    path: charges
    params: {limit: 100, "created[gte]": "{{ since }}"}
    primary_key: [id]
    schema: schemas/charges.json
    records: {path: $.data}
    pagination: {type: cursor, cursor_from: "last:id", cursor_param: starting_after, has_more: $.has_more}
    incremental: {cursor_field: created, cursor_format: unix_s, start: "{{ config.start_date }}", lookback: 1h}
```

### 3. Reference: every key

Types: `string`, `int`, `bool`, `duration` (Go duration such as `30s`, `1h`, or `Nd` days), `template` (a string with `{{ }}` substitutions, section 4), `path` (a response path, section 5), `field` (a dotted record field name such as `attributes.updated`), `list<T>`, `map<string,T>`.

#### 3.1 Top level

| Key | Type | Required | Meaning |
|---|---|---|---|
| `version` | int | yes | Format version. Only `1` is accepted. |
| `connector.name` | string | yes | Image and directory name, `source-<name>`; checked against the directory by the test entry. |
| `connector.documentation_url` | string | no | `documentationUrl` of the spec. |
| `base_url` | template | yes | Prefix of every stream `path`. May use `config` (`https://{{ config.site }}.chargebee.com/api/v2/`). Must end with `/`. |
| `spec` | object | yes | The connector's configuration form, section 3.2. |
| `auth` | object | yes | Section 3.3. |
| `headers` | map<string,template> | no | Headers sent on every request. Defaults added when absent: `Accept: application/json`, `User-Agent: synclet/<connector.name>`. |
| `params` | map<string,template> | no | Query parameters sent on every request. |
| `timeout` | duration | no | Per-request timeout, default `60s`. |
| `errors.message` | path | no | Where the API's error text sits in an error body; it is appended to every failure message. Default: the first 200 bytes of the body. |
| `errors.when` | list<rule> | no | Rules that make a 2xx response an error by a body value (Slack `ok: false`), section 3.4. |
| `retry` | object | no | Section 3.4. |
| `check` | object | yes | `check.stream: <stream name>`: `check` sends that stream's first request (page size 1 where the paginator has a size, first partition) and succeeds on a 2xx response that no `errors.when` rule turns into an error. |
| `streams` | list<stream> | yes | Section 3.5, at least one. |

#### 3.2 `spec`

`spec` is written as the connection specification's JSON Schema, restricted to what Synclet's form renders.

| Key | Type | Meaning |
|---|---|---|
| `title`, `description` | string | Form title and text. |
| `required` | list<string> | Required property names. |
| `properties.<name>` | object | One property. Allowed keys: `type` (`string`, `integer`, `number`, `boolean`, `array`), `title`, `description`, `default`, `examples`, `enum`, `format` (`date`, `date-time`), `pattern`, `secret` (bool, becomes `airbyte_secret`), `order` (int), `items` (`{type: string}` for arrays). Nothing else. |

The engine validates every incoming config against this schema before `check`, `discover` and `read`; a violation is a config error with the schema's message. Before that, a property with a `default` that the config lacks or holds as null takes the default, so templates and `check` see it and an Airbyte config saved without an optional property runs unchanged; Airbyte's manifests write the same fallback inline (`config.get('x') or '...'`). The lint rejects a default that fails its own property's `type`, `enum`, `pattern` or `format`. `supportsIncremental`, the protocol version and the destination sync modes are fixed by the engine. Spec, catalog and records of a manifest connector therefore need no Go.

#### 3.3 `auth`

| `type` | Keys | Request |
|---|---|---|
| `none` | | nothing added |
| `api_key` | `in` (`header` or `query`), `name`, `value` (template) | `name: value` as a header or query parameter. Open Exchange Rates: `{in: header, name: Authorization, value: "Token {{ config.app_id }}"}`; Klaviyo: `value: "Klaviyo-API-Key {{ config.api_key }}"`. |
| `bearer` | `token` (template) | `Authorization: Bearer <token>` |
| `basic` | `username`, `password` (templates; `password` may render empty) | `Authorization: Basic base64(user:pass)`. Jira: email and API token; Chargebee: the site key as username, empty password. |
| `oauth2_refresh` | `token_url`, `client_id`, `client_secret`, `refresh_token` (templates), `scopes` (list, optional), `body` (`form` default or `json`), `access_token_path` (default `$.access_token`), `expires_in_path` (default `$.expires_in`) | POST `grant_type=refresh_token` to `token_url`; `Authorization: Bearer <access_token>`. Refreshed before the first request, when `expires_in` minus 60 s has elapsed, and once more after a 401. The refresh token never rotates: a new `refresh_token` in the response is ignored and logged at WARN; APIs that rotate it are written in Go. |

A credential travels only in the header or parameter the manifest names, never in a logged URL: the engine logs request paths without query strings when `auth.in` is `query`.

#### 3.4 `retry`

```yaml
retry:
  transient: {statuses: [500, 502, 503, 504], attempts: 3, backoff: 1s}
  rate_limit:
    statuses: [429]
    also_when: {status: 403, header: x-ratelimit-remaining, equals: "0"}
    wait: ["header:Retry-After", "header-epoch:x-ratelimit-reset", 60s]
    attempts: 5
```

| Key | Type | Meaning |
|---|---|---|
| `transient.statuses` | list<int> | Statuses repeated with a doubling delay from `backoff` (default as shown). Network errors and timeouts are transient too. After `attempts` failures the request fails. |
| `rate_limit.statuses` | list<int> | Statuses that mean "wait, then repeat" (default `[429]`). |
| `rate_limit.also_when` | object | One more status that counts as a rate limit only when a header equals a value (GitHub's primary limit: 403 with `x-ratelimit-remaining: 0`). Optional. |
| `rate_limit.wait` | list | Tried in order, first value wins: `header:<name>` (seconds or an HTTP date, as `Retry-After` is), `header-epoch:<name>` (a Unix-seconds reset time), `body:<path>` (milliseconds, RevenueCat's `backoff_ms`), or a duration literal as the fallback. Default `["header:Retry-After", 60s]`. |
| `rate_limit.attempts` | int | Waits before the request fails (default 5). |

Status classification, not configurable: 401 and 403 are config errors (`failure_type: config_error`, `check` FAILED) with the API message; 429 and `also_when` wait; `transient.statuses` retry; every other non-2xx status fails the read with the method, path and API message; a stream-level `on_404: skip` (3.5) turns a 404 into a WARN log and skips the stream or partition.

Errors in a 2xx body. Some APIs answer errors with HTTP 200 (Slack's `{"ok": false, "error": "invalid_auth"}`). `errors.when` lists rules that make a 2xx response an error; a stream's own `errors.when` (3.5) is checked before the top-level list, and the first matching rule wins:

```yaml
errors:
  message: $.error
  when:
    - {path: $.error, equals: [invalid_auth, token_revoked, missing_scope], action: config_error}
    - {path: $.error, equals: ratelimited, action: rate_limit}
    - {path: $.ok, equals: false}
```

| Key | Type | Meaning |
|---|---|---|
| `path` | path | Body value the rule compares; without `[*]`. A body without it does not match. |
| `equals` | scalar or list of scalars | The rule matches when the value equals one of them; numbers compare by value. |
| `action` | `fail`, `config_error`, `retry`, `rate_limit`, `ignore` | `fail` (default): the request fails with the method, path, `HTTP 200` and the API message of `errors.message`. `config_error`: the same as a config error (`check` FAILED), for a bad credential or missing scope. `retry`: as a `transient.statuses` status. `rate_limit`: as a `rate_limit.statuses` status, waiting per `rate_limit.wait`. `ignore`: the response reads as no records and ends the pagination of that request (that stream, partition or parent record) with a WARN log; `check` passes on it. |

Rules apply to every API request, `check` included, and never to the OAuth token request or a non-2xx response. This is Airbyte's `error_handler.response_filters` (`predicate`, `action`, `failure_type`, `error_message`) reduced to one comparison.

#### 3.5 `streams[]`

| Key | Type | Required | Meaning |
|---|---|---|---|
| `name` | string | yes | Stream name in the catalog. |
| `path` | template | yes | Appended to `base_url`. May use `config`, `partition`, `parent`, `since`, `until`. Path segments are URL-escaped after rendering, so a value `owner/repo` stays two segments. A path that renders empty, or with a `.`, `..` or empty segment before the last, fails. |
| `params` | map<string,template> | no | Query parameters, merged over the top-level `params` (stream wins). A value that renders empty is omitted, so an optional config value such as `base` or `environment` needs no condition. A YAML list value is sent as a repeated parameter (`types[]`). |
| `headers` | map<string,template> | no | Merged over the top-level `headers`. |
| `primary_key` | list<field> | no | Becomes `source_defined_primary_key`, one field per entry. |
| `schema` | string or object | yes | A string is the path of the stream's JSON Schema file, relative to the manifest; an object is the JSON Schema itself, inline, so a whole connector can be one YAML document. Either way it must be an object schema with `properties`. An injected manifest (section 13) takes inline schemas only. |
| `records` | object | no | Section 3.6. Default `{path: "$", mode: array}`: the body is the array of records. |
| `pagination` | object | no | Section 3.7. Default: one request. |
| `incremental` | object | no | Section 3.8. Present: the stream supports `incremental` and `full_refresh`; absent: `full_refresh` only. On a stream with `partition.parent` it is allowed only when the parent is incremental, and then holds `cursor_field` and `cursor_format` only (3.9). |
| `partition` | object | no | Section 3.9. |
| `on_404` | `fail` or `skip` | no | `skip`: a 404 logs WARN and ends this stream (or this partition of it) without records; default `fail`. |
| `errors` | object | no | `errors.when`: the stream's own body rules (3.4), checked before the top-level ones. |

#### 3.6 `records`

| Key | Type | Meaning |
|---|---|---|
| `path` | path | Where the records are in the body. Default `$`. Stripe `$.data`, Jira `$.issues`, Chargebee `$.list[*].customer`. |
| `mode` | `array`, `object`, `entries` | `array` (default): the selected value is an array; each element is a record and must be an object. `object`: the selected value is one record. `entries`: the selected value is an object; each entry becomes a record whose `key_field` holds the key; an object value is the record's fields, a scalar value lands in `value_field`. Entries are emitted in sorted key order. |
| `key_field`, `value_field` | field | `entries` only. |
| `add` | map<field,source> | Fields added to every record **when the record lacks them or has null there**. A source is a response path (`$.base`, read from the response root), a template (`"{{ parent.id }}"`, `"{{ since }}"`), or a constant scalar. Every added field must exist in the schema. |

A records `path` that is missing from the body or null there (also a key before a `[*]` step, or a null body for `$`) is zero records and the last page of that request, whatever `has_more`, `is_last` or `total` say: Pipedrive v1 answers an empty list with `"data": null`. This matches Airbyte's `DpathExtractor`, which reads a missing path as `[]`. A wrong kind on the way (a string where the path needs an object), an element without the rest of a `[*]` path, a non-array under `mode: array`, a null element or a non-object record fails the stream with the request; an empty array is simply zero records. A misspelt `path` therefore reads nothing instead of failing, and the test entry (`declarativetest`) fails a stream whose fixtures give no records. Bodies are decoded with exact numbers (`json.Number`), so `0.912345` and 64-bit ids survive. The transport decompresses gzip transparently. Bodies that are not JSON (CSV, JSONL, XML) are out of the DSL.

#### 3.7 `pagination`

| Key | Applies to | Meaning |
|---|---|---|
| `type` | | `page`, `offset`, `cursor`, `next_url`, `link`. |
| `size`, `size_param` | all | Page size and the parameter that carries it (`limit`, `per_page`, `maxResults`). Sent on every request for `page`, `offset`, `cursor`; on the first request only for `next_url` and `link`. Both optional (`next_url`/`link` often have no size). |
| `page_param`, `first` | `page` | Page-number parameter and the first page number (default `page`, `1`). |
| `offset_param` | `offset` | Default `offset`; the engine sends the count of records received so far. |
| `cursor_from` | `cursor` | `$.<path>`: a body value; `header:<name>`: a response header; `last:<field>`: a field of the last record (Stripe's `starting_after`). |
| `cursor_param` | `cursor` | Query parameter that receives the cursor. |
| `url_from` | `next_url` | Body path of the next page's URL, absolute or relative to `base_url`. The stream's `params` are merged into it where the URL lacks the key (RevenueCat's `next_page` drops `limit`). |
| `has_more` | all | Body path of a boolean; the stream continues only while it is `true`. |
| `is_last` | all | Body path of a boolean; the stream stops when it is `true` (Jira's `isLast`). |
| `total` | `page`, `offset` | Body path of the total count; stop once that many records were received. |
| `short_page` | `page`, `offset` | `true`: stop after a page shorter than `size`. Off by default: an API that caps the size below the requested one would stop after one page. |
| `max_pages` | all | Hard limit, default `100000`; reaching it fails the stream. |

`link` follows the `Link` response header's `rel="next"` (RFC 5988), as GitHub, Freshdesk, Sentry and Shopify send it.

Stop conditions and guards, in this order after every page whose records path is present (a missing or null one stops at once, 3.6):

1. `has_more` false, `is_last` true, or `total` reached: stop.
2. An empty page: stop.
3. `cursor`, `next_url`, `link`: no next value (absent, null or empty): stop. A value equal to one already used: fail ("pagination repeated cursor X").
4. A body byte-identical to the previous page: fail ("page N repeated page N-1").
5. With a `primary_key`: a page whose records all carry keys seen on the previous page: fail ("page N repeats the records of page N-1"). A partial overlap is allowed (writes between pages).
6. `max_pages`: fail.

Overlapping pages under concurrent writes (offset and page paging) are not deduplicated by the engine; the primary key and the destination's dedup mode handle them. A cursor the API has expired answers a 4xx, which fails the stream; resuming from a fresh listing is a Go connector's job.

#### 3.8 `incremental`

| Key | Type | Meaning |
|---|---|---|
| `cursor_field` | field | Record field that carries the time, nested allowed (`attributes.updated`, `fields.updated`). Becomes `default_cursor_field`; `source_defined_cursor` is true. |
| `cursor_format` | `unix_s`, `unix_ms`, `rfc3339`, `date`, or a Go time layout | How record values are parsed for comparison. Jira: `2006-01-02T15:04:05.000-0700`. |
| `start` | template | Lower bound of the first sync, from config (`"{{ config.start_date }}"`), parsed as RFC 3339 or `YYYY-MM-DD`. Required. |
| `since_format` | as `cursor_format` | How `{{ since }}` and `{{ until }}` render; default `cursor_format`. Jira JQL: `2006-01-02 15:04`. All times are UTC. |
| `lookback` | duration | Subtracted from the saved cursor; default `0s`. |
| `window.step` | duration | Optional: split `[since, until)` into consecutive windows at most `step` long; the stream's requests run once per window with `since`/`until` set to its bounds; STATE is emitted after each window. |
| `window.closed_only` | bool | `true`: read only windows whose end is at or before now (per-day endpoints such as `historical/<date>.json`); default `false`. |

Semantics:

- `{{ since }}` is the lower bound: `start` on the first sync, else the saved cursor minus `lookback`, never earlier than `start`. `{{ until }}` is now, or the window's end. Where the API's filter parameter goes is the stream's business: `created[gte]`, `since`, `updated_at[after]`, `filter: greater-than(updated,{{ since }})` or a JQL string.
- A record whose cursor is older than `since` is not emitted; the engine logs the count per stream. The API's filter is sent but not relied on, so a resumed read is correct on an API that ignores or caps the filter, and static fixtures work. A record with no value at `cursor_field` fails the stream. Values are compared as parsed times, never lexically.
- The STATE of a stream without partitions is `{"<last segment of cursor_field>": <greatest cursor value seen, as the record carries it>}`, for example `{"created": 1760000000}` or `{"date": "2026-10-16"}`, emitted after the stream's last record and after every window. With `partition.values` it is `{"partitions": {"<value>": {"<cursor>": <greatest seen in that value>}}}`, emitted after every value and every window with every value read so far; a value added to the config later starts from `start`, a value removed keeps its entry. A child of an incremental parent keeps one value across all parents, like a stream without partitions.
- After a window the cursor is the greater of the greatest record cursor seen and the window's start, so an empty window still advances. A stream without windows that emits no record emits no state.
- A greatest-seen cursor is correct for descending lists (Stripe) because it is emitted only at the end; a window is the way to checkpoint a long first sync.
- With `window.closed_only`, a finished window is never re-read: the next sync starts at the window after the saved cursor's, minus `lookback` aligned down to a window boundary. Without it, the next sync starts at the saved cursor minus `lookback`, so the boundary record is read again and deduplicated downstream.
- In `full_refresh` the stream reads from `start` (all windows) and emits no state.
- Integer cursors (`since_id`) and cursors that are not times are out of the DSL.

#### 3.9 `partition`

One level only; a partitioned stream cannot be another stream's parent.

| Key | Type | Meaning |
|---|---|---|
| `values` | template or list<string> | A config array property (`"{{ config.repositories }}"`) or literal strings; the stream runs once per value, available as `{{ partition.value }}`. |
| `parent` | string | Name of another stream of this manifest; the stream runs once per parent record, available as `{{ parent.<field> }}`. |

Semantics of `values`: the stream runs value by value; an incremental stream keeps a cursor per value (3.8), so a repository added to the config later is backfilled from `start` without a reset. Windows nest inside: for each value, for each window, the pages.

Semantics of `parent`: streams that share a `parent` and the parent itself are read in one pass over the parent, as source-revenuecat does: each parent record is emitted if the parent stream is in the catalog, then each selected child stream reads for it to its last page; per-stream order is parent order, then page order. The parent is read even when it is not in the catalog, and nothing of it is emitted then. `records.add` with `"{{ parent.id }}"` carries the parent key onto child records; `on_404: skip` on the child skips a parent that disappeared between listing and reading. A child of an incremental parent may be incremental itself: its `incremental` names `cursor_field` and `cursor_format` only (a field of its own records, or one copied from the parent with `add`); `start`, `lookback` and `window` are the parent's. The pass runs from the oldest `since` among the parent (when it is selected incrementally) and the selected incremental children; every stream of the group drops its records older than its own `since` and emits its own STATE after the pass and after every window of the parent. `{{ since }}` and `{{ until }}` in a child render the pass's bounds. A child of a full-refresh parent is full refresh only (RevenueCat).

### 4. Templates

A template is literal text with substitutions of the form `{{ root.key.key }}` or `{{ root.key | join(",") }}`. Nothing else: no conditions, arithmetic, inline defaults or function calls (a fallback value is the property's spec `default`, 3.2). Rendered values are URL-escaped by the place they land in (path segment, query value, header); `join` is the only filter and turns a config array into one string.

| Root | Where allowed | Value |
|---|---|---|
| `config.<name>` | everywhere | A spec property. A missing optional property without a `default` renders empty (and an empty query value is omitted). The lint rejects a name not in `spec.properties`. |
| `partition.value` | streams with `partition.values` | The current value. |
| `parent.<field>` | streams with `partition.parent` | A field of the current parent record, nested allowed. |
| `since`, `until` | incremental streams | The current bounds in `since_format`. |

`spec` text, `connector` and `schema` are not templated. `base_url` and `auth` take `config` only.

### 5. Response paths

A path starts at the response root `$` and continues with `.name` (or `['name with spaces']`), `[n]` for one array element and `[*]` for every element. `[*]` maps the rest of the path over the elements and yields an array: `$.list[*].customer` is the list of customer objects. There are no filters, slices or recursive descent. Record fields (`cursor_field`, `primary_key`, `key_field`, `add` keys) are plain dotted names without `$`.

### 6. Decision table: in the DSL or in Go

Legend: **in** (the manifest expresses it), **Go** (write the connector in Go on the SDK). "Unlocks" counts connectors among the built ones, the six declarative candidates and the researched top 60 that need the feature.

#### Pagination

| Case | Real APIs | Decision | How / why |
|---|---|---|---|
| Page number + size | TikTok Marketing, Zoho, Freshdesk, Mailchimp (offset variant) | in | `type: page`; stop on empty page, `total`, `is_last` or `short_page`. |
| Offset + limit | Jira project/user search (`startAt`), Mailchimp (`offset`/`count`/`total_items`), Pipedrive | in | `type: offset`; the engine sends records received so far. |
| Cursor in body, echoed as a query param | Intercom (`pages.next.starting_after`), Zendesk (`meta.after_cursor`), Airtable (`offset`), Slack (`response_metadata.next_cursor`), Jira issue search (`nextPageToken`), Chargebee (`next_offset`), Facebook (`paging.cursors.after`) | in | `type: cursor, cursor_from: $.…, cursor_param: …`. |
| Cursor = last record id + `has_more` | Stripe | in | `cursor_from: "last:id", has_more: $.has_more`. |
| Cursor in a response header | rare (some internal APIs); kept for symmetry at near-zero cost | in | `cursor_from: "header:X-Next"`. |
| RFC 5988 `Link` header | GitHub, GitLab, Freshdesk, Sentry, Shopify, Okta | in | `type: link`. |
| Full next URL in the body | Klaviyo (`links.next`), RevenueCat (`next_page`), Twilio (`next_page_uri`), Salesforce (`nextRecordsUrl`), Facebook (`paging.next`) | in | `type: next_url`; relative URLs resolve against `base_url`; stream params merged where absent. |
| Token in a POST body | Notion, Google Search Console, GA4, Intercom search | Go | Question (f): needs JSON body templating and a body-side token; its users are three connectors already classified Go. Revisit when a third declarative candidate needs it. |
| Time-window slicing | Open Exchange Rates (per day), Amplitude and Mixpanel exports (per day, CSV/JSONL so Go anyway), ad reports (AppLovin, Facebook insights: Go), Stripe/Chargebee long first syncs | in (widening of (e)) | `incremental.window`; doubles as the checkpoint mechanism for descending lists. |
| Stop: empty page, `has_more`, `is_last`, page shorter than size, total count | all of the above | in | section 3.7. |
| Mixed: two pagination styles in one API | Jira (offset lists, token search), Intercom (cursor lists, scroll API) | in | pagination is per stream. |
| Duplicate or overlapping pages | offset/page APIs under writes | in (documented) | not deduplicated; primary key + destination dedup. |
| Cursor expiry, restart from scratch | Zendesk incremental cursors, Slack | Go | needs a restart policy per API. |
| Infinite loops | any | in | repeated cursor/URL fails, identical page fails, `max_pages` fails; all with the request in the message. |

#### Extraction

| Case | Real APIs | Decision | How / why |
|---|---|---|---|
| Records array under a key | Stripe `data`, Jira `issues`, Klaviyo `data`, Intercom `data`, RevenueCat `items`, Zendesk `tickets` | in | `records.path`. |
| Top-level array | GitHub, Jira `users/search` | in | default `path: $`. |
| Object wrapper per element | Chargebee `list[*].customer` | in | `[*]` in the path. |
| Single object as one record | account/settings endpoints | in | `mode: object`. |
| Object keyed map to rows | Open Exchange Rates `rates` and `currencies.json`, fixer/currencylayer-style feeds | in (widening of (e)) | `mode: entries`; about thirty lines of engine code; unlocks all three OXR proof streams. |
| Envelope metadata onto records | OXR `base`/`timestamp`, RevenueCat `currency` | in | `records.add` with a response path. |
| CSV, JSONL, XML bodies | Amplitude, Mixpanel, S3 | Go | different decoders and memory profile. |
| gzip | GitHub, many | in | transparent in `net/http`. |
| Records under keys that vary per record | polymorphic payloads | Go | — |
| Empty / null | all | in | empty array, missing or null records path, null body = no records (Airbyte parity; Pipedrive v1 `"data": null`); null element, non-object record fail. |
| 200 with `errors[]` | GraphQL-style APIs, Facebook batch | Go | no REST candidate does it. |
| Parent → child fan-out | RevenueCat customer children (3 streams), Jira comments and worklogs, Intercom conversation parts, Stripe subscription items / invoice lines, Zendesk ticket comments | in, restricted (widening of (d)) | `partition.parent`, one level, one parent pass per sibling group, incremental via the parent with the child's own cursor; without it the RevenueCat proof covers 7 of 10 streams, and Jira comments, Intercom conversation parts and Stripe nested lists go to Go. |
| Config list → requests | GitHub repositories, multi-account APIs | in ((a)) | `partition.values` with a cursor per value, so a value added later backfills. |

#### Transformation

| Case | Decision | Why |
|---|---|---|
| Add a field from the envelope, the parent, the partition or `since` | in | needed by OXR (`date`, `base`, `timestamp`), RevenueCat (`customer_id`, `currency`), GitHub (`repository`). |
| Rename, drop, flatten one level | Go | no candidate needs it; schemas keep nested objects (Klaviyo `attributes`, Jira `fields`). |
| Type coercion (numeric strings, epoch to date-time, date-only) | Go | records pass through exactly; a coercion table is a second schema to maintain. Epoch cursors are compared by `cursor_format` without changing the record. |
| Default values, computed constants | partly | `add` with a constant fills a missing field; nothing computes. |
| Filtering records | Go | candidates filter server-side (`types[]`, `state=all`, `include_deleted`). |
| Timezone | in by construction | values untouched; `since`/`until` render in UTC; a `*_without_timezone` schema field keeps the API's zone-less string, which the testkit checks. JQL interprets `since` in the account's zone; `lookback` covers the offset. |

#### Auth, requests, errors

| Case | Decision | How |
|---|---|---|
| API key in header or query, bearer, basic, OAuth refresh (non-rotating) | in | section 3.3. |
| Rotating refresh tokens, JWT/service accounts, session tokens, request signing (AppLovin, Solidgate HMAC) | Go | — |
| Static version headers (`Stripe-Version`, `revision`, `Intercom-Version`, `X-GitHub-Api-Version`) | in ((b)) | `headers`. |
| Config in path, query, headers, base URL | in ((a)) | templates. |
| Conditional parameters | in, one rule | empty renders are omitted. |
| POST / JSON bodies (Google Ads GAQL, Solidgate, Notion) | Go | (f). |
| Retry 5xx and network errors with backoff | in | `retry.transient`. |
| 429 with `Retry-After`, reset-epoch headers, body backoff | in | `retry.rate_limit.wait`. |
| 403 as a rate limit when a header says so (GitHub) | in ((g)) | `retry.rate_limit.also_when`. |
| Requests-per-minute pacing, per-domain budgets (RevenueCat) | Go | 429 handling is the DSL's mechanism; pacing is tuning. |
| 401/403 config errors with the API's text | in | `errors.message`. |
| 2xx with an error in the body (Slack `ok: false`) | in | `errors.when`: fail, config error, retry, rate limit or ignore by one body value. |
| Message heuristics (RevenueCat "v1 key"), probing every stream's permission in `check` | Go | `check` is one request. |
| 404 → skip stream or partition | in | `on_404: skip`. |

### 9. Validation

1. `manifest.schema.json`: every object has `additionalProperties: false`; enums for every closed set; one `if`/`then` branch per `auth.type` and `pagination.type`, so a key of another type is reported as unknown at its own path. Loading decodes YAML to a generic document and validates it with `santhosh-tekuri/jsonschema/v6` (already an SDK dependency), so an unknown key or a wrong type is reported with its instance path.
2. Strict typed decode (`encoding/json` with `DisallowUnknownFields`, of the document both a YAML file and `__manifest` decode to) as the second net.
3. Semantic lint: templates parse and use only allowed roots and known config properties; `since`/`until` only in incremental streams; `partition.parent` names an existing, unpartitioned stream; `parent.*` only with a parent; every schema file exists, and every schema, file or inline, is an object schema with `properties`; no `schema` is a file path in an injected manifest; no spec property starts with `__`; a spec `default` satisfies its property; `cursor_field`, `primary_key`, `key_field`, `value_field` and `add` keys exist in the schema; `check.stream` exists; `base_url` ends with `/`; `pagination` keys match the type; `window.step` and `lookback` are positive; stream names unique.
4. `go run ./cmd/manifest-lint <dir>` prints every error with its path and exits 1; `declarativetest.Run` loads the manifest the same way first; `-spec` prints the spec JSON so `spec.json` can be refreshed without Docker.

### 13. Manifest sources: baked file or injected config

The engine supports both sources.

| | Catalog connector | Builder connector |
|---|---|---|
| Where it is made | a directory in synclet-io/connectors ([CONTRIBUTING](../../CONTRIBUTING.md#a-manifest-connector)) | a future Synclet UI builder (T-43, T-44) |
| Image | its own, `FROM source-declarative` plus `/manifest` | the one shared `ghcr.io/synclet-io/connectors/source-declarative:<version>` |
| Manifest | `manifest.yaml` and `schemas/` baked into the image, read from the file system | stored in Synclet's database, sent in the config's reserved `__manifest` key on every run |
| Schemas | files or inline | inline only |
| A change ships by | a release (new image, new version) | saving in Synclet; the next run uses it, no image build |
| Spec | `spec` of the image | computed by Synclet from the stored manifest (13.4) |

Secrets stay where every connector keeps them: in the normal source config, declared `secret: true` in the manifest's `spec` and encrypted by Synclet as today. The manifest holds no secret, only `{{ config.<name> }}` references to them; Synclet stores `__manifest` apart from the secret config and adds it to the config only when it builds a run.

#### 13.1 Resolution

At start, `main.go` looks for `$SYNCLET_MANIFEST_DIR/manifest.yaml` (default `/manifest`):

1. **Baked file present: catalog mode.** The file is the manifest. A config that carries `__manifest` fails with a config error ("this connector's manifest is built into its image; remove `__manifest`"), at `check`, `discover` and `read`. It is rejected, not ignored, so a misconfigured platform never runs a manifest other than the one the user edited, or the reverse.
2. **No baked file: generic mode.** The config must carry `__manifest`; a config without it fails with a config error ("`__manifest` is required on the generic source-declarative image").

The engine image ships no manifest (its example lives in `example/source-acme`), so the released `source-declarative` image is the generic image. A catalog image whose `COPY` misses the manifest fails `docker build`, and its `spec.json` check fails, so a broken catalog image cannot silently become generic.

#### 13.2 `__manifest`

- Value: the manifest as a JSON object, the same document the YAML decodes to, with anchors and merge keys already resolved by Synclet. A string is a config error. Synclet keeps the YAML text the user wrote and sends its parsed form.
- Size: at most 512 KiB as encoded JSON, inline schemas included. Synclet enforces the limit on save; the engine checks it again and fails with a config error naming the size. The bound keeps a run's config, catalog and state inside the 1 MiB of a Kubernetes Secret and the config file small on Docker.
- `__` is reserved: a manifest's `spec.properties` may not declare a name starting with `__`. The engine removes `__manifest` from the config before it validates the rest against the manifest's spec (3.2), so the user's spec never sees it, and the engine never logs it.
- `schema` in an injected manifest must be an inline object. A string is a config error at its path: "`streams[1].schema`: a file path is not allowed in an injected manifest; inline the schema". Nothing in generic mode reads the file system.

#### 13.3 Validation and errors

An injected manifest passes exactly the checks of section 9 (schema, strict decode, semantic lint) when the config arrives, before anything else. Every failure is a config error listing each problem with its path, prefixed `__manifest.` (`__manifest.streams[2].pagination.cursor_from: unknown key`):

- `check`: `connectionStatus` FAILED with that message;
- `discover` and `read`: a TRACE error with `failure_type: config_error` and the same message, and no records.

The same manifest therefore behaves identically baked or injected: one loader (`manifest.LoadDocument`, which `Load` and `LoadFS` call after reading files) and identical records, state and catalog for the same document.

#### 13.4 `spec` of a builder connector

The `spec` command receives no config, so the generic image cannot know a builder connector's form. Its `spec` returns a placeholder: `connectionSpecification` with one required object property `__manifest`; JSON Schema's default lets every other key through (the SDK's spec type has no `additionalProperties` field). Synclet never shows it as a form.

Synclet takes a builder connector's spec from the stored manifest instead: the mapping of 3.2 (`secret` becomes `airbyte_secret`, the fixed `supportsIncremental` and protocol version) is a pure function of the manifest's `spec` block. Synclet computes it when a manifest version is saved, stores it with that version and serves it like any connector's spec. The mapping is `Manifest.ConnectorSpec` and the validation `manifest.LoadDocument`, both in `internal/manifest`; how Synclet reaches them (an exported package, its own copy, or a command of the image) is settled with T-43.

### 14. Builder infer mode

For the builder only: a test read that returns sampled records and a draft JSON Schema per stream, which the user edits and saves inline. Published connectors never run it, and nothing in `check`, `discover` or `read` changes.

```text
source-declarative infer --config <config.json> [--streams a,b] [--limit 100] [--pages 3]
```

| Input | Meaning |
|---|---|
| `--config` | The source config with `__manifest`, as for `read`. The command exists in generic mode only; a catalog image answers it with a config error. |
| `--streams` | Streams to sample, comma-separated; default all. An unknown name is a config error. |
| `--limit` | Records per stream, default 100, 1 to 1000. |
| `--pages` | Pages per stream (and per partition value or parent), default 3, 1 to 10. |

A missing `--config`, an unreadable file or a value out of range is a usage error: a message on stderr and exit status 2, as for the protocol commands.

The manifest is linted in a relaxed mode: `schema` may be absent on any stream. The rules that need a schema (`cursor_field`, `primary_key`, `key_field`, `value_field` and `add` keys in the schema) are checked against the drafted schema afterwards and reported as LOG warnings; a stream that has a `schema` is checked against it as usual, and an error there fails the command. There is no catalog and no state: every stream reads as full refresh from `start`, the first window only; a `partition.values` stream samples its first value, a child its first parents up to `--limit`, whatever their children return.

Output on stdout, in order:

- RECORD messages, at most `--limit` per stream, exactly as `read` would emit them (with `add`, records older than `start` left out);
- a LOG message naming each request (method and path, without the query when `auth.in: query`); secret config values never appear in a LOG or TRACE message, nor do their JSON-escaped, URL query-escaped and path-escaped forms or the base64 `user:pass` of basic auth; a value shorter than 8 bytes is masked only as a whole token, so text that merely contains it stays readable;
- per stream that fails (HTTP error, extraction error, pagination guard): a TRACE error with its `stream_descriptor` and message; the other streams continue, and the failed stream is left out of the CATALOG;
- LOG warnings for the relaxed-lint findings and for a stream with no records;
- last, one CATALOG message whose streams carry the draft `json_schema`, the stream's `source_defined_primary_key` and cursor from the manifest, as `discover` reports them.

Exit status 0 when the manifest and config load, whatever the streams did; 1 on a config error (catalog image, missing or invalid `__manifest`, invalid config, unknown stream), reported as a TRACE error as for `read`.

Draft schema rules: an object schema with `properties` for every field seen in any sample, nested objects recursively; a field's `type` is the union of the JSON types seen, `null` first and the rest by name, plus `null` when it was null or missing in some sample; `integer` when every number seen is integral, else `number`; `format: date` or `date-time` (RFC 3339) on a string field when every sample parses as one; array `items` merged the same way; no `required`; `additionalProperties: true`; properties sorted by name, so the same samples give the same draft. A stream with no records gets `{type: object, properties: {}, additionalProperties: true}` and a warning.
