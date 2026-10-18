# source-notion

[Notion](https://www.notion.so) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads the users, databases, pages and
blocks shared with a Notion internal integration. Databases, pages and blocks can sync incrementally on
`last_edited_time`.

```bash
task test CONNECTOR=source-notion     # no network, no credentials
task build CONNECTOR=source-notion
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-notion:dev check --config /data/config.json
```

## Setup

1. Create an internal integration at [notion.so/profile/integrations](https://www.notion.so/profile/integrations)
   in the workspace to sync.
2. Under **Capabilities**, keep **Read content**. For the `users` stream also choose **Read user
   information including email addresses**; without it the `users` stream fails with
   `the integration lacks a capability`.
3. Copy the **Internal Integration Secret** into `access_token`.
4. Share the pages and databases to sync with the integration: on each top-level page or database,
   **⋯ → Connections → <your integration>**. Sharing a page shares the pages and databases under it.

## Configuration

| Key | Required | Default | Meaning |
|---|---|---|---|
| `access_token` | yes | | Internal integration secret. Sent only as `Authorization: Bearer <token>`, never in a URL |
| `lookback_minutes` | no | `5` | An incremental read starts this many minutes before the saved `last_edited_time`; 0 or more |
| `max_block_depth` | no | `16` | How deep blocks are read under a page, 1 to 100 |
| `max_blocks_per_page` | no | `10000` | How many blocks are read under one page, 1 or more |

```json
{"access_token": "<your integration secret>"}
```

### Authentication

This version authenticates with an internal integration token only. Public integrations (OAuth) are not
supported.

### Check

`check` requests `GET /v1/users/me`, then `POST /v1/search` for one result, and fails with:

| Cause | Error starts with |
|---|---|
| The token is wrong or revoked (401) | `integration token is invalid` |
| No page and no database is shared with the integration | `nothing is shared with the integration: add it to pages or databases under ⋯ → Connections in Notion` |

## Streams

Every request carries `Notion-Version: 2025-09-03`. In this version a database holds one or more data
sources; each data source has its own schema and rows, and a row is a page whose parent is the data source.

| Stream | Primary key | Sync modes | Source |
|---|---|---|---|
| `users` | `id` | full refresh | `GET /v1/users` |
| `databases` | `id` | full refresh, incremental | data sources found by `POST /v1/search` |
| `pages` | `id` | full refresh, incremental | rows of each data source (`POST /v1/data_sources/{id}/query`) and the other pages found by `POST /v1/search` |
| `blocks` | `id` | full refresh, incremental | the blocks under every page of `pages` (`GET /v1/blocks/{id}/children`, recursively) |

The schemas are fixed; a record holds the top-level keys of the Notion object as the API returns them.

- **users**: `id`, `object`, `type` (`person` or `bot`), `name`, `avatar_url`, `person` (`{email}`), `bot`.
- **databases**: one record per data source: `id` (the data source id), `database_id` (the database it
  belongs to), `title`, `description`, `properties` (the data source's property schema), `parent`,
  `database_parent`, `is_inline`, `icon`, `cover`, `url`, `public_url` and the audit columns.
- **pages**: `id`, `parent`, `properties`, `icon`, `cover`, `is_locked`, `url`, `public_url` and the
  audit columns.
- **blocks**: `id`, `page_id` (the page the block is under), `depth` (1 for a page's direct children),
  `parent`, `type`, `content` (the object under the block's type key, such as `paragraph`, as returned),
  `has_children` and the audit columns.
- Audit columns: `created_time`, `last_edited_time`, `created_by`, `last_edited_by`, `archived`, `in_trash`.

### Page properties

`properties` is the Notion property-value object, keyed by property name, exactly as the API returns it:

```json
{"Name": {"id": "title", "type": "title", "title": [{"type": "text", "plain_text": "Write spec", "...": "..."}]},
 "Amount": {"id": "am%3D1", "type": "number", "number": 10}}
```

Every property type is kept, formulas and rollups included, and a renamed or retyped property does not
change the stream's schema. Pages of different data sources share the one `pages` stream; `parent.data_source_id`
says which data source a row belongs to.

A page found by search whose data source is shared is read only through its data source's query, so it is
emitted once. A row of a data source that is not shared, but which is shared itself, comes from search.

### Blocks

Blocks are read depth first under each page. The children of these blocks are not read under the page:

- `child_page` and `child_database`: the page or database is read as a page or database of its own;
- a `synced_block` that copies another (`synced_from` is set): its children belong to the original's page.

A page with a block at `max_block_depth` that has children, or with more than `max_blocks_per_page`
blocks, fails the read with an error naming the page, the block and the setting to raise. Nothing is
skipped or truncated.

## Incremental sync

The cursor is `last_edited_time`, chosen by the source; `users` is full refresh only. An incremental read
starts `lookback_minutes` before the saved cursor, because Notion rounds `last_edited_time` to the minute
and search indexing lags behind edits; the window is read again on every sync, so deduplicate on `id`.

- **databases**: search runs the most recently edited first and stops at the first data source edited
  before the window. State: `{"last_edited_time": "<RFC 3339>"}`, emitted at the end of the stream.
- **pages**: each data source is queried with the filter
  `{"timestamp": "last_edited_time", "last_edited_time": {"on_or_after": "<saved - lookback>"}}` from its
  own saved cursor, or in full when it has none, and the state is emitted after each data source. Search
  then runs the most recently edited first and stops at the first page edited before the window. State:
  `{"last_edited_time": "<greatest of the search pages>", "data_sources": {"<data source id>": "<greatest of its rows>"}}`,
  emitted once more at the end.
- **blocks**: the same page listing and the same state shape, on the pages' `last_edited_time`. Only pages
  edited since are walked, every level of them, and of their blocks only those edited since are emitted.

Caveats:

- A page newly shared with the integration whose `last_edited_time` is older than the saved cursor is not
  found by an incremental search. Run a full refresh periodically, or after sharing more pages.
- Trashed pages and data sources are not returned by search or query, so deletions are seen only by a full
  refresh. Records carry `archived` and `in_trash` as returned.
- A block edit updates its page's `last_edited_time`, which is what brings the page into an incremental
  blocks read. A deleted block is seen only by a full refresh.

## Limits

- Requests are spaced at least 350 ms apart, under Notion's average of 3 requests per second.
- A 429 waits for its `Retry-After` (1 second without one) and is repeated, at most 5 times.
- A 5xx response or a transport error is repeated 3 times with a delay doubling from 1 second.
- Any other error status fails the read with Notion's `code` and `message`; a 403 says the integration lacks
  a capability.
- Listings are read 100 per page, the most Notion allows, following `start_cursor` (in the JSON body for
  `POST` requests, in the query for `GET`).

## Not supported

- OAuth (public integrations).
- Comments: block-level comments need one request per block, which is too slow for large workspaces.
- Typed per-database streams: all rows go to `pages` with raw `properties`.
