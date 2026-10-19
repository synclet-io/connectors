# Changelog

## 0.1.0 (2026-10-19)


### Features

* **source-mongodb:** decide date-time with the JSON schema format check, redact the decoded URI password ([76cd3d5](https://github.com/synclet-io/connectors/commit/76cd3d54c315a90177eda6f082f02417529cfd39))
* **source-mongodb:** infer date-time only for strict RFC 3339 strings, keep the URI password without a config one ([d5af59e](https://github.com/synclet-io/connectors/commit/d5af59e901e6c51a8169f27ebe8492d2a46abc95))
* **source-mongodb:** MongoDB source with schema sampling, exact Decimal128, cursor incremental, TLS and SSH tunnel ([da8a196](https://github.com/synclet-io/connectors/commit/da8a196a75d59d890a79af1024667c093e5b225d))


### Bug Fixes

* **source-mongodb:** document the cursor value checks and allow_missing_cursor; spec snapshot ([6b1c227](https://github.com/synclet-io/connectors/commit/6b1c227ed9cc3d8891f3562958c8fc354818e513))
* **source-mongodb:** fail incremental reads on cursor values a range query would skip; allow_missing_cursor opt-in ([3a19579](https://github.com/synclet-io/connectors/commit/3a19579bd23b6c68929f6cba6566a4e8a5f27c43))
* **source-mongodb:** gofmt the cursor type test ([82a3cb5](https://github.com/synclet-io/connectors/commit/82a3cb54b39874be237df56838ce77480d1b699a))
* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## Changelog
