# Changelog

## 0.1.0 (2026-10-19)


### Features

* **destination-postgres:** Postgres destination with COPY batches, overwrite swap, dedup merge and state echo after commit ([c14951a](https://github.com/synclet-io/connectors/commit/c14951ada711721a0d4dcc8714baa9ea1fca8f3b))
* **destination-postgres:** reject time zones in timestamp and time values and invalid UTF-8 in strings; test held GLOBAL and LEGACY states ([dc6625a](https://github.com/synclet-io/connectors/commit/dc6625a89edad0c217e06651209f358244513f43))
* **destination-postgres:** scan string escapes for lone surrogates and find the timestamp time part after the date's last digit ([d0f5a26](https://github.com/synclet-io/connectors/commit/d0f5a2651cbf0989012faff8fdd57647b7302139))


### Bug Fixes

* **destination-postgres:** hold states of overwrite streams matched by name, whatever their namespace ([b5ce2e1](https://github.com/synclet-io/connectors/commit/b5ce2e12f0cc9e482911efef3008b552b7e3dff4))

## Changelog
