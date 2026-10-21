# Changelog

## [0.2.0](https://github.com/synclet-io/connectors/compare/source-declarative-v0.1.0...source-declarative-v0.2.0) (2026-10-21)


### Features

* **source-declarative:** a spec default fills a missing or null config value ([666a08c](https://github.com/synclet-io/connectors/commit/666a08ca65c923a17227e2952987f60f3cea0f99))
* **source-declarative:** errors.when turns a 2xx body into a fail, config error, retry, rate limit or ignore ([98a22a4](https://github.com/synclet-io/connectors/commit/98a22a4690a10e0d919b451f446418380f3f422e))


### Bug Fixes

* **source-declarative:** a missing or null records path reads as no records and ends the request ([35f4583](https://github.com/synclet-io/connectors/commit/35f4583e9d92e9ded914cce07183d351e7bb0f94))

## 0.1.0 (2026-10-20)


### Features

* **source-declarative:** builder infer mode with draft schemas ([e12a8e0](https://github.com/synclet-io/connectors/commit/e12a8e016941e2d52123f0b522aa12ec1a9ae5fd))
* **source-declarative:** conformance entry, proof manifests and contributing guide ([a483739](https://github.com/synclet-io/connectors/commit/a4837392c1c0bbf3c37de626cca20262403f480f))
* **source-declarative:** engine for baked and injected manifests ([5d8f200](https://github.com/synclet-io/connectors/commit/5d8f2007dc84f6131d12314e187d97c4829adba4))
* **source-declarative:** manifest format, loader and lint ([ae13abe](https://github.com/synclet-io/connectors/commit/ae13abe0bbd0fb5005c8bc13319cd215b0ec610b))


### Bug Fixes

* **source-declarative:** config_error survives errors.Join ([eb3999b](https://github.com/synclet-io/connectors/commit/eb3999be73671536c105187d9466304a3c909e0a))
* **source-declarative:** declarativetest Inspect sees every fixture request ([d99f89f](https://github.com/synclet-io/connectors/commit/d99f89f7a47e72e8e6b5fb0cf6ca67424915fb1b))
* **source-declarative:** escape path segments after rendering ([78ec21e](https://github.com/synclet-io/connectors/commit/78ec21e9bd132a91081b4b93584db73d70e43787))
* **source-declarative:** fail a record without a primary key value ([2cc3583](https://github.com/synclet-io/connectors/commit/2cc35839f2dd3f407aa9b4f9d7c242e8b8284b12))
* **source-declarative:** fail pagination that stops before its total ([a7ef1f5](https://github.com/synclet-io/connectors/commit/a7ef1f555648971460234f9caa65bbd7013051a0))
* **source-declarative:** fixture headers may carry absolute base URL links ([e99a6bc](https://github.com/synclet-io/connectors/commit/e99a6bcd2bbcc38ce809765100b6977b035ac3cf))
* **source-declarative:** infer samples a child's first --limit parents ([0263419](https://github.com/synclet-io/connectors/commit/0263419351c08053da7fd74b45f9274f2966dc58))
* **source-declarative:** keep redirects on the base host ([6040877](https://github.com/synclet-io/connectors/commit/6040877905c0cdab23277a365626649a8268ecb4))
* **source-declarative:** keep secret config values out of config errors, 404 warnings and number secrets ([0e77502](https://github.com/synclet-io/connectors/commit/0e7750220caa8341e1344ca7e0950d8f178e156b))
* **source-declarative:** mask encoded secret forms in every log, trace and check message ([492a491](https://github.com/synclet-io/connectors/commit/492a4915c54224d1f51cf5370a61cbfa156a43d3))
* **source-declarative:** mask more encoded secret forms, overlapping secrets and panic messages ([93a87f9](https://github.com/synclet-io/connectors/commit/93a87f918148bccf85008f4e10e044ad7705f308))
* **source-declarative:** mask runtime OAuth access and refresh tokens ([199c740](https://github.com/synclet-io/connectors/commit/199c740e1c55f16e5ae9d45bfa8007d3790233c1))
* **source-declarative:** review findings ([dd72f53](https://github.com/synclet-io/connectors/commit/dd72f53898da7fee3a6815f7ea2d550e32bb48e1))
* **source-stripe:** review findings ([70aef04](https://github.com/synclet-io/connectors/commit/70aef04bf21fcaa89ee7dad9dbeadb809ccf7c84))

## Changelog
