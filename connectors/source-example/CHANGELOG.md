# Changelog

## [0.2.1](https://github.com/synclet-io/connectors/compare/source-example-v0.2.0...source-example-v0.2.1) (2026-10-19)


### Bug Fixes

* **deps:** bump the gomod group across 2 directories with 6 updates ([#17](https://github.com/synclet-io/connectors/issues/17)) ([4fbf3ed](https://github.com/synclet-io/connectors/commit/4fbf3eda00e0a76bc0d714b1733391a5adebdf56))
* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## 0.2.0 (2026-10-18)

### Changes

* Built on synclet-go-sdk v0.3.0: `cli.RunSource` runs the connector, every command takes a context cancelled on SIGTERM and a typed `Config`, and `Read` emits records through an `Emitter` and stops when its context is cancelled.
* The spec declares protocol version 0.5.2 and its changelog key is `changelogUrl`; the discovered stream declares `"type": "object"`.
* Tests run the synclet-go-sdk conformance suite (`testkit.RunSourceSuite`).
* `.gitignore` and `.dockerignore` exclude the built `source-example` binary.
* Minor version bump: the SDK move changes the connector's code and protocol output. The configuration stays compatible.
