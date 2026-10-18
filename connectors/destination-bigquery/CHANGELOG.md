# Changelog

## 0.2.0 (2026-10-18)

### Changes

* Moved from synclet-io/synclet to synclet-io/connectors; the image is `ghcr.io/synclet-io/connectors/destination-bigquery`.
* Built on synclet-go-sdk v0.3.0 instead of saturn4er/airbyte-go-sdk v0.1.5: commands take a context cancelled on SIGTERM, a failed check reports its reason in `CONNECTION_STATUS`, single JSON schema types are written as strings and the spec key is `changelogUrl`.
* Input is read with the SDK's message reader: lines of any length are accepted, and a line that is not a protocol message fails the write instead of being skipped.
* A queued GLOBAL state is echoed as GLOBAL with its shared and stream states.
* Minor version bump: the SDK move is a breaking change of the connector's code and protocol output. Configuration stay compatible.

## [0.1.1](https://github.com/synclet-io/synclet/compare/destination-bigquery-v0.1.0...destination-bigquery-v0.1.1) (2026-04-01)


### Bug Fixes

* **connectors:** remove unused dependencies from Dockerfiles ([7cc6ab4](https://github.com/synclet-io/synclet/commit/7cc6ab437b8e4249da91ad86b60560adb7501c11))
