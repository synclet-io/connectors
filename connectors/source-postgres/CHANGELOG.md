# Changelog

## [0.2.1](https://github.com/synclet-io/connectors/compare/source-postgres-v0.2.0...source-postgres-v0.2.1) (2026-10-19)


### Bug Fixes

* **deps:** bump the docker group across 5 directories with 2 updates ([#16](https://github.com/synclet-io/connectors/issues/16)) ([fe642ef](https://github.com/synclet-io/connectors/commit/fe642ef1743be0a491b49b1011eb5fbdf00ea444))
* **deps:** bump the gomod group across 2 directories with 6 updates ([#17](https://github.com/synclet-io/connectors/issues/17)) ([4fbf3ed](https://github.com/synclet-io/connectors/commit/4fbf3eda00e0a76bc0d714b1733391a5adebdf56))
* sources send sourceStats on every state through synclet-go-sdk v0.4.0 ([4eb0e12](https://github.com/synclet-io/connectors/commit/4eb0e12e66e7b8f3b529996a03f08497fa6500e8))

## 0.2.0 (2026-10-18)

### Changes

* Discover lists tables readable only through column-level `SELECT` grants, with the granted columns; a primary key with a column the user may not read is left out.
* The connection is built from the configuration alone: `PG*` environment variables, `~/.pgpass` and `~/.postgresql` no longer reach the driver.
* New optional `tunnel_host_key` in both SSH tunnel methods pins the jump server's host keys, one per line in authorized_keys or known_hosts format (the output of ssh-keyscan); the client asks for a key of a pinned type, and a server presenting none of the pinned keys is refused.
* Multidimensional arrays of every element type are flattened, so their values match the array schema; only `real`, `double precision` and `numeric` arrays were flattened before.
* Infinite dates and timestamps, and infinite elements of their arrays, read as `null` instead of `"infinity"`, which their schema formats cannot hold.
* A stream without a namespace whose name exists in several configured schemas fails with a config error instead of reading the first schema's table.
* `ssl_mode` `prefer` and `allow` refuse a `ca_certificate`, since both may connect without encryption.
* Minor version bump: the spec gains an optional field; existing configurations stay valid except `prefer` or `allow` with a `ca_certificate`.
