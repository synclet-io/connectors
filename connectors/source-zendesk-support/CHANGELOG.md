# Changelog

## [0.1.1](https://github.com/synclet-io/connectors/compare/source-zendesk-support-v0.1.0...source-zendesk-support-v0.1.1) (2026-10-19)


### Bug Fixes

* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## 0.1.0 (2026-10-18)


### Features

* **source-zendesk-support:** tickets, users, ticket events and lists source with API token auth ([909bce1](https://github.com/synclet-io/connectors/commit/909bce169a8a371d9ebfb6cd4f09503160c9a0bf))


### Bug Fixes

* **source-zendesk-support:** ticket_events steps past a stalled end_time, empty state starts at start_date, subdomain is lowercased ([060eccc](https://github.com/synclet-io/connectors/commit/060ecccd272ad9397888aae2cf27f83a061e0f39))
* **source-zendesk-support:** tickets and users start from start_date on a state without a cursor ([6280a4c](https://github.com/synclet-io/connectors/commit/6280a4c0eca764b4e1187f2e89c55787c8c0d9a5))
