# source-revenuecat

[RevenueCat](https://www.revenuecat.com) source built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Reads a project's configuration (apps,
products, entitlements, offerings), its customers with their subscriptions, purchases and active
entitlements, and the overview metrics from the [REST API v2](https://www.revenuecat.com/docs/api-v2).
Every stream is full refresh.

```bash
task test CONNECTOR=source-revenuecat     # no network, no credentials
task build CONNECTOR=source-revenuecat
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-revenuecat:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `api_key` | yes | Secret API key of version **V2** (Project settings > API keys > New, version V2). Sent only as `Authorization: Bearer <api_key>`, never in a URL. API v1 keys do not work with REST API v2 |
| `project_id` | no | Project to read. Empty: the key must see exactly one project (`GET /projects`), which is used |
| `environment` | no | `sandbox` or `production`: reads only that store environment's customer subscriptions and purchases. Empty reads both |

```json
{"api_key": "<your v2 secret key>", "project_id": "proj1ab2c3d4", "environment": "production"}
```

### Permissions

A v2 key carries a set of read permissions. Each stream needs:

| Stream | Permission |
|---|---|
| `projects` | `project_configuration:projects:read` |
| `apps` | `project_configuration:apps:read` |
| `products` | `project_configuration:products:read` |
| `entitlements` | `project_configuration:entitlements:read` |
| `offerings` | `project_configuration:offerings:read` |
| `customers`, `customer_active_entitlements` | `customer_information:customers:read` |
| `customer_subscriptions` | `customer_information:subscriptions:read` |
| `customer_purchases` | `customer_information:purchases:read` |
| `metrics_overview` | `charts_metrics:overview:read` |

An empty `project_id` also needs `project_configuration:projects:read`.

### Check

`check` lists the projects, then requests the first record of every stream (`limit=1`; the customer
streams on the first customer, if the project has one). It fails with:

| Cause | Error contains |
|---|---|
| Wrong or revoked key (401) | `invalid API key ...; API v1 keys do not work with REST API v2` |
| A v1 key (401 whose message names v1 or the API version) | `API v1 key used for REST API v2` |
| The key lacks permissions (403) | `key lacks permission for streams: <stream> (<permission>), ...`, every such stream |
| `project_id` is not among the key's projects, or the project returns 404 | `unknown project "<id>"` |
| No `project_id` and the key sees several projects | `project_id is empty and the key can see N projects (<ids>)` |
| No `project_id` and the key cannot list projects | `project_id is empty and the key cannot list projects` |

A read fails with the same messages when it meets a 401, a 403 (naming the stream and its permission) or
a 404 of the project.

## Streams

| Stream | Route under `/v2/projects/{project_id}` | Rate limit domain | Primary key |
|---|---|---|---|
| `projects` | `/v2/projects` | project configuration | `id` |
| `apps` | `/apps` | project configuration | `id` |
| `products` | `/products` | project configuration | `id` |
| `entitlements` | `/entitlements` | project configuration | `id` |
| `offerings` | `/offerings` | project configuration | `id` |
| `customers` | `/customers` | customer information | `id` |
| `customer_subscriptions` | `/customers/{id}/subscriptions` | customer information | `id` |
| `customer_purchases` | `/customers/{id}/purchases` | customer information | `id` |
| `customer_active_entitlements` | `/customers/{id}/active_entitlements` | customer information | `customer_id`, `entitlement_id` |
| `metrics_overview` | `/metrics/overview` | charts and metrics | `id` |

`projects` lists every project the key can see; the other streams read the configured project.

Records are emitted as the API returns them. Schemas declare the documented fields and set
`additionalProperties: true`, so a field RevenueCat adds later is kept. Nested objects (revenue amounts,
entitlement lists, store details) are typed `object`. Timestamps stay integer milliseconds since the
Unix epoch, as the API sends them; each field's description says so.

The customer streams fan out from one pass over the customer list: for each customer the connector
emits the customer (when `customers` is selected), then reads each selected child stream of that
customer to its last page. A child record carries the customer's id in `customer_id` (added to active
entitlements, which have none; a subscription or purchase naming another customer fails the read). A
404 on a child route means the customer was deleted after it was listed: the read logs a warning and
skips that customer's remaining child streams. The `environment` filter is sent on subscriptions and
purchases, the two routes that document it.

`metrics_overview` is one request per read: one record per entry of `metrics`, each with the response's
`currency`.

Lists are paged with `limit=100` and `starting_after`, taken from each page's `next_page` until a page
has none. A `next_page` without `starting_after`, or one that repeats an earlier value, fails the read.

## No incremental sync

The API offers no updated-since filter: no list endpoint can be asked for records changed after a
time, so every stream is full refresh and each sync reads the whole project. `metrics/revenue` takes a
date range, but it returns one revenue total per range that RevenueCat revises as late transactions,
refunds and store reports arrive, and it is limited to 25 requests per minute, so it is not read as an
incremental stream in this version.

## Rate limits

RevenueCat limits each domain per key: project configuration 60, customer information 480, charts and
metrics 25 requests per minute. A read spaces the requests of each domain one interval apart (1s,
125ms and 2.4s), across all streams of the read. After each response the
`RevenueCat-Rate-Limit-Current-Limit` header replaces the domain's limit, and a
`RevenueCat-Rate-Limit-Current-Usage` at or above that limit adds one more interval before the next
request. A 429 waits for its `Retry-After` (else the body's `backoff_ms`, else 60 seconds) and repeats
the request, up to 5 times; then the read fails naming the path. A 5xx response, an error body marked
`retryable` or a network error is retried three times, waiting 1s, 2s and 4s. 401 and 403 are
configuration errors and are not retried.

A large project takes a while: each customer costs one request per selected child stream (more for
long lists), at 480 requests per minute.

## Tests

Tests replay HTTP from hand-written fixtures; they need no network and no credentials (see
[CONTRIBUTING](../../CONTRIBUTING.md#testing-api-sources-with-recorded-http)).

- **Layout:** `GET /v2/<p>` is answered by `testdata/<p>.json`; with `starting_after=X` by
  `testdata/<p>/after-X.json`. Error bodies live in `testdata/errors/`. Every list stream has two
  pages. All values are synthetic (`proj_test1`, `cust_test_001`, `prod_monthly_test`, round amounts).
- **Helper:** `fixtures_test.go` holds `fixtureServer`, which serves `testdata/`, answers 401 to any
  request without `Authorization: Bearer test-secret-key` (with a v1 message for `test-v1-key`), 404 to
  a path of any project but `proj_test1`, fails the test when a request carries a key in its URL, lets a
  test override the status, file and headers of one path (optionally for its first requests only), and
  records the requests. `testSource` points a `Source` at it through its unexported fields (`baseURL`,
  `client`, `now`, `retryDelay`, `wait`) on a fake clock whose `wait` records each duration and moves
  the clock instead of sleeping; the fields are zero in `main`.
- `TestReadAllStreamsPages` covers every stream across pages and the exact requests,
  `TestFanOutPacing`, `TestRateLimitHeaderLowersLimit` and `TestRetryAfter429` the rate limiting,
  `TestCustomerDeleted` the child 404, `TestCheckCauses` each check failure, `TestConformance` the SDK
  conformance suite for full refresh.
