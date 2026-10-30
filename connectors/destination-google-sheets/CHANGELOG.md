# Changelog

## [0.2.2](https://github.com/synclet-io/connectors/compare/destination-google-sheets-v0.2.1...destination-google-sheets-v0.2.2) (2026-10-30)


### Bug Fixes

* **destination-google-sheets:** reject service account keys whose token_uri is not Google's ([9782339](https://github.com/synclet-io/connectors/commit/9782339f6ec0a1ae6d43bbcb62ca2ae2e0e55efd))
* **google:** keep authorized_user keys, drop key members of other flows ([051a1ba](https://github.com/synclet-io/connectors/commit/051a1baaa98434fac425ea0df4e4bf1142be691b))

## [0.2.1](https://github.com/synclet-io/connectors/compare/destination-google-sheets-v0.2.0...destination-google-sheets-v0.2.1) (2026-10-19)


### Bug Fixes

* **deps:** bump the docker group across 5 directories with 2 updates ([#16](https://github.com/synclet-io/connectors/issues/16)) ([fe642ef](https://github.com/synclet-io/connectors/commit/fe642ef1743be0a491b49b1011eb5fbdf00ea444))
* **deps:** bump the gomod group across 2 directories with 6 updates ([#17](https://github.com/synclet-io/connectors/issues/17)) ([4fbf3ed](https://github.com/synclet-io/connectors/commit/4fbf3eda00e0a76bc0d714b1733391a5adebdf56))

## 0.2.0 (2026-10-18)

### Changes

* Moved from synclet-io/synclet to synclet-io/connectors; the image is `ghcr.io/synclet-io/connectors/destination-google-sheets`.
* Built on synclet-go-sdk v0.3.0 instead of saturn4er/airbyte-go-sdk v0.1.5: commands take a context cancelled on SIGTERM, a failed check reports its reason in `CONNECTION_STATUS`, single JSON schema types are written as strings and the spec key is `changelogUrl`.
* Input is read with the SDK's message reader: lines of any length are accepted, and a line that is not a protocol message fails the write instead of being skipped.
* Integer and decimal values are decoded as exact JSON numbers, so values beyond float64 precision keep every digit.
* Minor version bump: the SDK move is a breaking change of the connector's code and protocol output. Configuration stays compatible.

## [0.1.1](https://github.com/synclet-io/synclet/compare/destination-google-sheets-v0.1.0...destination-google-sheets-v0.1.1) (2026-04-01)


### Bug Fixes

* **connectors:** remove unused dependencies from Dockerfiles ([7cc6ab4](https://github.com/synclet-io/synclet/commit/7cc6ab437b8e4249da91ad86b60560adb7501c11))
