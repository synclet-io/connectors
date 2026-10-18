# Changelog

## 0.3.0 (2026-10-18)

### Changes

* Moved from synclet-io/synclet to synclet-io/connectors; the image is `ghcr.io/synclet-io/connectors/source-google-sheets`.
* Built on synclet-go-sdk v0.2.0 instead of saturn4er/airbyte-go-sdk v0.1.5: commands take a context cancelled on SIGTERM, a failed check reports its reason in `CONNECTION_STATUS`, single JSON schema types are written as strings and the spec key is `changelogUrl`. Discovered streams declare `"type": "object"`.
* Minor version bump: the SDK move is a breaking change of the connector's code and protocol output. Configuration and saved state stay compatible.

## [0.2.1](https://github.com/synclet-io/synclet/compare/source-google-sheets-v0.2.0...source-google-sheets-v0.2.1) (2026-04-01)


### Bug Fixes

* connectors name ([a9f05f1](https://github.com/synclet-io/synclet/commit/a9f05f18ff31d2f3a8f411734dec37a69bd31c65))

## [0.2.0](https://github.com/synclet-io/synclet/compare/source-google-sheets-v0.1.2...source-google-sheets-v0.2.0) (2026-04-01)


### Features

* **connectors:** add meta.json metadata and forward in release workflow ([ce73f9f](https://github.com/synclet-io/synclet/commit/ce73f9f144855d60a11dcf26aa945c0753777531))

## [0.1.2](https://github.com/synclet-io/synclet/compare/source-google-sheets-v0.1.1...source-google-sheets-v0.1.2) (2026-04-01)


### Bug Fixes

* trigger build ([d527235](https://github.com/synclet-io/synclet/commit/d52723525a2feb333f26caefa30bb63972eb4ae5))

## [0.1.1](https://github.com/synclet-io/synclet/compare/source-google-sheets-v0.1.0...source-google-sheets-v0.1.1) (2026-04-01)


### Bug Fixes

* **connectors:** remove unused dependencies from Dockerfiles ([7cc6ab4](https://github.com/synclet-io/synclet/commit/7cc6ab437b8e4249da91ad86b60560adb7501c11))
