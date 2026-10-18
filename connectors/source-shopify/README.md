# source-shopify

[Shopify](https://www.shopify.com/) store source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads products, variants, customers,
orders with their line items, refunds, fulfillments, transactions, inventory, locations, collections and B2B
companies through the Admin GraphQL API, full refresh or incrementally by update time. Large streams run as
bulk operations whose output is verified before a single record is emitted.

```bash
task test CONNECTOR=source-shopify     # no network, no credentials
task build CONNECTOR=source-shopify
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-shopify:dev check --config /data/config.json
```

Every request goes to `https://<shop>.myshopify.com/admin/api/2026-10/graphql.json`; the API version is
pinned in the connector (`apiVersion` in `client.go`) and moves with connector releases.

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `shop` | yes | Store name (`example`) or its myshopify.com domain (`example.myshopify.com`). Custom domains and URLs are rejected |
| `access_token` | one of | Admin API access token of a custom app installed on the store |
| `client_id`, `client_secret` | one of | Client credentials of a Dev Dashboard app installed on the store |
| `start_date` | no | Earliest update time read: RFC 3339 time or `YYYY-MM-DD`, UTC. Empty reads from the shop's creation |
| `window_days` | no | Days of updates one query reads, 1 to 365, default 30. An incremental sync saves its state after each window |

```json
{"shop": "example", "access_token": "<admin api access token>", "start_date": "2026-01-01"}
```

```json
{"shop": "example.myshopify.com", "client_id": "<client id>", "client_secret": "<client secret>"}
```

Set exactly one of `access_token` or `client_id` with `client_secret`.

### Authentication

- **Access token.** Custom apps created in a store's admin give an Admin API access token
  (`shpat_...`). It is sent only in the `X-Shopify-Access-Token` header.
- **Client credentials.** Apps created in the Dev Dashboard and installed on a store get their token from the
  client credentials grant: the connector posts `client_id` and `client_secret` in the form body of
  `POST https://<shop>.myshopify.com/admin/oauth/access_token` and uses the returned token (valid about
  24 hours) until it expires. A token rejected with 401 is fetched again once.

This connector assumes stores can no longer create new admin custom apps since 2026-01-01, so new setups use a
Dev Dashboard app; existing custom app tokens keep working. This was not verified against a live store.
OAuth consent (public apps) is not supported.

### Scopes

| Scope | Streams |
|---|---|
| `read_products` | `products`, `product_variants`, `collections` |
| `read_customers` | `customers`, `companies`, `company_locations` |
| `read_orders` | `orders`, `refunds`, `fulfillments`, `transactions` |
| `read_all_orders` | without it Shopify returns only the orders of the last 60 days |
| `read_inventory` | `inventory_items`, `inventory_levels` |
| `read_locations` | `inventory_levels`, `locations` |

A `write_` scope counts as its `read_` scope. `shop` needs no scope.

### Check

`check` fetches a token when the config has client credentials, then sends one query for the shop's name and
the app's access scopes. It fails with:

| Cause | Error |
|---|---|
| Wrong or revoked access token (401) | `access token is invalid or revoked (HTTP 401): <Shopify's message>` |
| Wrong client credentials, or the app is not installed (400 or 401 from the grant) | `client_id or client_secret is invalid, or the app is not installed on <shop>.myshopify.com` |
| Unknown shop (404) | `shop <shop>.myshopify.com not found; use the shop's myshopify.com domain` |
| A scope missing | `missing access scope read_orders (orders, refunds, fulfillments, transactions)`, one clause per scope, joined by `; ` |
| Frozen shop (402) | `shop <shop>.myshopify.com is frozen (unpaid)` |
| Locked shop (423) | `shop <shop>.myshopify.com is locked` |

A missing `read_all_orders` passes the check with a WARN log.

## Streams

| Stream | Read as | Sync modes | Cursor |
|---|---|---|---|
| `shop` | one query | full refresh | |
| `products` | bulk `products` | full refresh, incremental | `updatedAt` |
| `product_variants` | bulk `productVariants` | full refresh, incremental | `updatedAt` |
| `customers` | bulk `customers` | full refresh, incremental | `updatedAt` |
| `orders` | bulk `orders`, line items embedded as `lineItems` | full refresh, incremental | `updatedAt` |
| `refunds` | bulk `orders { refunds }` | full refresh, incremental | `order_updated_at` |
| `fulfillments` | bulk `orders { fulfillments }` | full refresh, incremental | `order_updated_at` |
| `transactions` | bulk `orders { transactions }` | full refresh, incremental | `order_updated_at` |
| `inventory_items` | bulk `inventoryItems` | full refresh, incremental | `updatedAt` |
| `inventory_levels` | bulk `locations { inventoryLevels }`, with `location_id` | full refresh | |
| `locations` | pages of `locations`, inactive and legacy included | full refresh | |
| `collections` | pages of `collections` | full refresh, incremental | `updatedAt` |
| `companies` | pages of `companies` | full refresh, incremental | `updatedAt` |
| `company_locations` | pages of `companyLocations` | full refresh, incremental | `updatedAt` |

Every stream's primary key is `id`, the object's GID (`gid://shopify/Order/1001`). Records carry the GraphQL
field names as queried. `refunds`, `fulfillments` and `transactions` records carry the order's `order_id` and
`order_updated_at`; Shopify bumps an order's `updatedAt` when its refunds, fulfillments or transactions change.

Decimal and money amounts (`price`, `amount` of `MoneyV2` and `MoneyBag`) arrive as strings and become JSON
numbers with exactly the same digits, never rounded through a float; a value that is not a decimal number
fails the read.

`companies` and `company_locations` exist on stores with B2B (Shopify Plus). `discover` leaves them out when
the companies query fails with a GraphQL error.

### Windows and state

A windowed stream reads `[from, to)` windows of `window_days`, from the saved cursor or the start
(`start_date`, or the shop's creation) up to one minute before now, each with the search filter
`updated_at:>='<from>' AND updated_at:<'<to>'`. An incremental read emits
`{"cursor": "<to>"}` after all records of a window, so a failed sync resumes at the first unfinished
window. A full refresh reads the same windows and saves no state.

Deleted objects are not visible to the GraphQL query API: an incremental sync never sees a delete. Run a full
refresh (overwrite) to drop deleted rows.

### Bulk operations

Each bulk stream runs one `bulkOperationRunQuery` per window, polls it every 5 seconds and downloads its
JSONL output from the signed `url`, without the Shopify token. The output goes to a temporary file and is
verified before any record is emitted. The stream fails, and its state does not advance, when:

- the run returns `userErrors`, such as `OPERATION_IN_PROGRESS` when another bulk query of the app runs on the
  store;
- the operation ends `FAILED`, `CANCELED`, `CANCELING` or `EXPIRED`, or has a `partialDataUrl`; partial data is
  never read;
- the output does not hold exactly `objectCount` lines and `rootObjectCount` roots, a line is not a JSON
  object or lacks its final newline, or a child line's `__parentId` is not the root before it;
- a completed operation reports objects but no `url`.

The error names the stream, the window, the operation id, its status and `errorCode`. A completed operation
with no objects and no `url` is an empty window. When the read is cancelled while an operation runs, the
connector sends `bulkOperationCancel` for it.

### Rate limits

The connector paces by the query cost Shopify reports in `extensions.cost`: before a request it waits until
the bucket, restoring at `restoreRate`, holds the cost of the last query. A `THROTTLED` error waits the same
way and is retried up to 10 times; a 429 waits its `Retry-After`; a 5xx, a transport error or an
`INTERNAL_SERVER_ERROR` is retried 3 times with a doubling delay. `ACCESS_DENIED` fails the stream naming the
scope Shopify requires.

## Tests

The tests run against an `httptest` server serving hand-written synthetic fixtures from `testdata/`
(`example.myshopify.com`, made-up GIDs, `example.com` emails, invented amounts): GraphQL requests are answered
by `testdata/graphql/<operation>[-<key>].json` and bulk outputs by `testdata/bulk/<stream>.jsonl`. They can be
re-recorded from a free Partner development store; scrub every identifier before committing.

## Follow-ups

- Metafields of products, customers and orders.
- Incremental `inventory_levels`.
- Line items of refunds (`refundLineItems`) and fulfillments.
