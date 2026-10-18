# Changelog

## 0.2.0 (2026-10-18)

### Changes

* Built on synclet-go-sdk v0.3.0: `cli.RunSource` runs the connector, every command takes a context cancelled on SIGTERM and a typed `Config`, and `Read` emits records through an `Emitter` and stops when its context is cancelled.
* The spec declares protocol version 0.5.2 and its changelog key is `changelogUrl`; the discovered stream declares `"type": "object"`.
* Tests run the synclet-go-sdk conformance suite (`testkit.RunSourceSuite`).
* `.gitignore` and `.dockerignore` exclude the built `source-example` binary.
* Minor version bump: the SDK move changes the connector's code and protocol output. The configuration stays compatible.
