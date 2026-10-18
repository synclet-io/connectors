# source-example

Minimal Go source built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk).
Emits `count` rows (default 10) into a single `numbers` stream with `id` and `label` columns.

Copy this directory to start a new connector; see [CONTRIBUTING.md](../../CONTRIBUTING.md).

```bash
task test CONNECTOR=source-example
task build CONNECTOR=source-example
echo '{"count": 3}' > /tmp/config.json
docker run --rm -v /tmp:/data ghcr.io/synclet-io/connectors/source-example:dev check --config /data/config.json
```
