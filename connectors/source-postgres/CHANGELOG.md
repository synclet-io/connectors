# Changelog

## 0.2.0 (2026-10-18)

### Changes

* Discover lists tables readable only through column-level `SELECT` grants, with the granted columns; a primary key with a column the user may not read is left out.
* The connection is built from the configuration alone: `PG*` environment variables, `~/.pgpass` and `~/.postgresql` no longer reach the driver.
* New optional `tunnel_host_key` in both SSH tunnel methods pins the jump server's host key (authorized_keys format or a known_hosts line); a mismatched key refuses the tunnel.
* Multidimensional arrays of every element type are flattened, so their values match the array schema; only `real`, `double precision` and `numeric` arrays were flattened before.
* Infinite dates and timestamps, and infinite elements of their arrays, read as `null` instead of `"infinity"`, which their schema formats cannot hold.
* A stream without a namespace whose name exists in several configured schemas fails with a config error instead of reading the first schema's table.
* `ssl_mode` `prefer` and `allow` refuse a `ca_certificate`, since both may connect without encryption.
* Minor version bump: the spec gains an optional field; existing configurations stay valid except `prefer` or `allow` with a `ca_certificate`.
