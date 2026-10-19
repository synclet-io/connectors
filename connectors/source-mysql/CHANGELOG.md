# Changelog

## [0.3.0](https://github.com/synclet-io/connectors/compare/source-mysql-v0.2.0...source-mysql-v0.3.0) (2026-10-19)


### ⚠ BREAKING CHANGES

* **source-mysql:** BINARY, VARBINARY and BLOB values are base64-encoded in full refresh and incremental reads instead of raw strings; a STANDARD read whose configured table is missing fails with a config error instead of skipping the stream.

### Features

* **source-mysql:** check names the cause: wrong credentials, unreachable host, missing database, no privilege on the... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** config keys follow Airbyte (ssl_mode, tunnel_method, replication_method); ssl, tunnel and replica... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** optional tunnel_host_key pins the SSH jump server's host keys ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** rebuild on source-postgres' layout and synclet-go-sdk v0.4.0 ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** zero_date_handling offers its two values as fixed choices in the spec ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** zero_date_handling reads zero dates as null (default) or fails the read naming stream, column and... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))


### Bug Fixes

* **deps:** bump the docker group across 5 directories with 2 updates ([#16](https://github.com/synclet-io/connectors/issues/16)) ([fe642ef](https://github.com/synclet-io/connectors/commit/fe642ef1743be0a491b49b1011eb5fbdf00ea444))
* **deps:** bump the gomod group across 2 directories with 6 updates ([#17](https://github.com/synclet-io/connectors/issues/17)) ([4fbf3ed](https://github.com/synclet-io/connectors/commit/4fbf3eda00e0a76bc0d714b1733391a5adebdf56))
* **source-mysql:** a 0.2 binary or BIT cursor that lost bytes to U+FFFD fails the read with a config error asking to reset the stream state ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** a 0.2 state with a binary or BIT cursor resumes from the same bytes and is saved in the new encoding ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** a binary incremental cursor is saved base64-encoded and a BIT cursor as an integer, so resuming compares the exact bytes ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** BLOB and BINARY values are base64 in full refresh, incremental and CDC; bit(1) is a boolean, bit(n... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** CDC binlog values read as SELECT reads them: VARBINARY base64, JSON as documents, ENUM and SET as ... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** DATETIME reads without offset, TIMESTAMP in UTC with Z, in a +00:00 session ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** every query runs on the text protocol: exact DECIMAL with its scale, exact unsigned BIGINT, TIME b... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** full refresh streams the table in one query and saves no state; the partitioned and PK-chunked rea... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** identifiers are backtick-quoted in every query, CDC snapshot included, so names with dots and back... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** in STANDARD reads a stream whose table is missing fails with a config error instead of being skipped; CDC still skips it with a warning ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** incremental reads one streaming query from the saved cursor (&gt;=) with a checkpoint every 10000 row... ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* **source-mysql:** ZEROFILL numbers read without their padding, as valid JSON numbers ([c103242](https://github.com/synclet-io/connectors/commit/c1032423db808a9bcb5cba15670fe852f3e99a24))
* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## 0.2.0 (2026-10-18)

### Changes

* Moved from synclet-io/synclet to synclet-io/connectors; the image is `ghcr.io/synclet-io/connectors/source-mysql`.
* Built on synclet-go-sdk v0.3.0 instead of saturn4er/airbyte-go-sdk v0.1.5: commands take a context cancelled on SIGTERM, a failed check reports its reason in `CONNECTION_STATUS`, single JSON schema types are written as strings and the spec key is `changelogUrl`. Discovered streams declare `"type": "object"`.
* CDC state is a GLOBAL state: the binlog position is the shared state and each stream's snapshot progress its stream state. A LEGACY CDC state saved by 0.1.x is still read, so a running CDC sync continues without a reset.
* The CDC check reads `log_bin`, `binlog_format` and `binlog_row_image` again; `SHOW VARIABLES LIKE ?` failed as a prepared statement, so every CDC check failed.
* Records and stream states carry the database as namespace, as the discovered streams do. A stream state saved by 0.1.x without namespace is still read.
* DECIMAL and NUMERIC values are JSON numbers, as their stream schema declares, instead of strings.
* The integration tests run on a MySQL testcontainer instead of docker-compose on port 3307; they include the synclet-go-sdk conformance suite.
* Cursor and primary key values in saved state are decoded exactly, so an incremental cursor on a DECIMAL or BIGINT column resumes from its exact value.
* CDC snapshot progress is kept only for streams of the configured catalog; entries of other streams in the saved state are dropped.
* Minor version bump: the SDK move is a breaking change of the connector's code and protocol output. Configuration and saved state stay compatible.

## [0.1.2](https://github.com/synclet-io/synclet/compare/source-mysql-v0.1.1...source-mysql-v0.1.2) (2026-04-01)


### Bug Fixes

* trigger build ([d527235](https://github.com/synclet-io/synclet/commit/d52723525a2feb333f26caefa30bb63972eb4ae5))

## [0.1.1](https://github.com/synclet-io/synclet/compare/source-mysql-v0.1.0...source-mysql-v0.1.1) (2026-04-01)


### Bug Fixes

* **connectors:** remove unused dependencies from Dockerfiles ([7cc6ab4](https://github.com/synclet-io/synclet/commit/7cc6ab437b8e4249da91ad86b60560adb7501c11))
