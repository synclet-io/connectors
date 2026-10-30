# Changelog

## [0.1.1](https://github.com/synclet-io/connectors/compare/source-airtable-v0.1.0...source-airtable-v0.1.1) (2026-10-30)


### Bug Fixes

* **source-airtable:** end check and discover after 4 minutes ([e1e1a01](https://github.com/synclet-io/connectors/commit/e1e1a0151fa0ca8aa705e93a7af00a31b5828b03))
* **source-airtable:** fail fast on plan API limit, bound and log rate-limit waits, fetch base tables concurrently ([6ed2eb0](https://github.com/synclet-io/connectors/commit/6ed2eb09c13028633fe074d3477eb78caffb51ae))

## 0.1.0 (2026-10-19)


### Features

* **source-airtable:** guarded since formula, bases 403 names the schema scope, reserved field names renamed ([d9bf971](https://github.com/synclet-io/connectors/commit/d9bf971ee92938aeb713a99172e9258538fb86b4))
* **source-airtable:** one stream per table with field-typed schemas, incremental by last-modified-time field ([fed3fab](https://github.com/synclet-io/connectors/commit/fed3fab52cc21de3fc146e067415959fc5f97d52))


### Bug Fixes

* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## Changelog
