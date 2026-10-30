# Changelog

## [0.2.0](https://github.com/synclet-io/connectors/compare/source-solidgate-v0.1.0...source-solidgate-v0.2.0) (2026-10-30)


### Features

* **source-solidgate:** fields of the earlier connector 3.7.0, null for a field Solidgate does not send ([98e176f](https://github.com/synclet-io/connectors/commit/98e176ff8575da7e146da6f61c05c1e57e171c6a))


### Bug Fixes

* **source-solidgate:** state is the last day read per account, so days without records are not read again ([e647430](https://github.com/synclet-io/connectors/commit/e647430c80f781d7df2678f84e61e61c8db41bd1))

## 0.1.0 (2026-10-19)


### Features

* **source-solidgate:** keep the earlier connector's timestamp formats and every subscription invoice ([da8c296](https://github.com/synclet-io/connectors/commit/da8c2967d9fff91a6269cfa088873a3cfe3ebbbc))
* **source-solidgate:** orders, subscriptions, disputes, fraud alerts, products and financial entries source ([4de8b28](https://github.com/synclet-io/connectors/commit/4de8b286167fbd1b8aa887e1102d408c555c7fa4))
* **source-solidgate:** synclet-go-sdk v0.3.1, nested schemas as the earlier connector, products paging guard ([1afc859](https://github.com/synclet-io/connectors/commit/1afc8597f2a67e5f3bbb824ae8eb651b6cbac82b))


### Bug Fixes

* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## Changelog
