# Changelog

## 0.2.0 (2026-10-18)

### Changes

* Moved from synclet-io/synclet to synclet-io/connectors; the image is `ghcr.io/synclet-io/connectors/source-mysql`.
* Built on synclet-go-sdk v0.2.0 instead of saturn4er/airbyte-go-sdk v0.1.5: commands take a context cancelled on SIGTERM, a failed check reports its reason in `CONNECTION_STATUS`, single JSON schema types are written as strings and the spec key is `changelogUrl`. Discovered streams declare `"type": "object"`.
* CDC state is a GLOBAL state: the binlog position is the shared state and each stream's snapshot progress its stream state. A LEGACY CDC state saved by 0.1.x is still read, so a running CDC sync continues without a reset.
* The CDC check reads `log_bin`, `binlog_format` and `binlog_row_image` again; `SHOW VARIABLES LIKE ?` failed as a prepared statement, so every CDC check failed.
* The integration tests run on a MySQL testcontainer instead of docker-compose on port 3307.
* Minor version bump: the SDK move is a breaking change of the connector's code and protocol output. Configuration and saved state stay compatible.

## [0.1.2](https://github.com/synclet-io/synclet/compare/source-mysql-v0.1.1...source-mysql-v0.1.2) (2026-04-01)


### Bug Fixes

* trigger build ([d527235](https://github.com/synclet-io/synclet/commit/d52723525a2feb333f26caefa30bb63972eb4ae5))

## [0.1.1](https://github.com/synclet-io/synclet/compare/source-mysql-v0.1.0...source-mysql-v0.1.1) (2026-04-01)


### Bug Fixes

* **connectors:** remove unused dependencies from Dockerfiles ([7cc6ab4](https://github.com/synclet-io/synclet/commit/7cc6ab437b8e4249da91ad86b60560adb7501c11))
