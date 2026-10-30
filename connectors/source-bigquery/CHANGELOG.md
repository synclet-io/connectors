# Changelog

## [0.2.2](https://github.com/synclet-io/connectors/compare/source-bigquery-v0.2.1...source-bigquery-v0.2.2) (2026-10-30)


### Bug Fixes

* **google:** keep authorized_user keys, drop key members of other flows ([051a1ba](https://github.com/synclet-io/connectors/commit/051a1baaa98434fac425ea0df4e4bf1142be691b))
* **source-bigquery:** reject service account keys whose token_uri is not Google's ([afa397f](https://github.com/synclet-io/connectors/commit/afa397fb1f812cd1dfa5548e97b2441b5be55f9b))

## [0.2.1](https://github.com/synclet-io/connectors/compare/source-bigquery-v0.2.0...source-bigquery-v0.2.1) (2026-10-19)


### Bug Fixes

* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## [0.2.0](https://github.com/synclet-io/connectors/compare/source-bigquery-v0.1.0...source-bigquery-v0.2.0) (2026-10-18)


### Features

* **source-bigquery:** BigQuery source with full refresh and incremental cursor reads ([767759e](https://github.com/synclet-io/connectors/commit/767759e1af2e6fa25d26e7a6c858715c5699a654))
* **source-bigquery:** check probes table data and visible datasets, maps only accessDenied to roles ([9882d86](https://github.com/synclet-io/connectors/commit/9882d86fd705d46b3654bd19e323046d6597c635))
