# Changelog

## [0.1.1](https://github.com/synclet-io/connectors/compare/source-google-analytics-data-api-v0.1.0...source-google-analytics-data-api-v0.1.1) (2026-10-30)


### Bug Fixes

* **google:** keep authorized_user keys, drop key members of other flows ([051a1ba](https://github.com/synclet-io/connectors/commit/051a1baaa98434fac425ea0df4e4bf1142be691b))
* **source-google-analytics-data-api:** reject service account keys whose token_uri is not Google's ([dd1e945](https://github.com/synclet-io/connectors/commit/dd1e945219e4a99dccbe65651b0ebc0b8a60e44a))

## 0.1.0 (2026-10-19)


### Features

* **source-google-analytics-data-api:** GA4 reports source with built-in and custom reports ([5f8ec7e](https://github.com/synclet-io/connectors/commit/5f8ec7e6591138a371ce3b3747ef24c4a1d60935))
* **source-google-analytics-data-api:** test data loss, thresholding and row dates, tell scope errors apart, warn when daily tokens run out ([57430cd](https://github.com/synclet-io/connectors/commit/57430cd4f11f4f02bd6d9c1d33e7049c13bffa86))


### Bug Fixes

* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## Changelog
