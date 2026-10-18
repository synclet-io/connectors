# Contributing

Every directory under `connectors/` is one connector. Its name is the image name and must start
with `source-` or `destination-`. In short: **copy a connector dir, fill `meta.json`, run `task spec`.**

Requires Go 1.26+, Docker, [Task](https://taskfile.dev), `jq` and `golangci-lint` v2.

```
connectors/source-acme/
├── Dockerfile     # builds an image whose ENTRYPOINT speaks the Airbyte protocol
├── meta.json      # registry metadata, validated against schema/meta.schema.json
├── spec.json      # output of the image's `spec` command, checked in
└── ...            # connector code (any language)
```

## 1. Write the connector

The image must implement the [Airbyte protocol](https://docs.airbyte.com/understanding-airbyte/airbyte-protocol):
`spec`, `check --config`, `discover --config` and `read --config --catalog [--state]` for sources;
`spec`, `check --config` and `write --config --catalog` (records on stdin) for destinations. Messages go to
stdout as JSON lines.

Copy `source-example`, which is built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk)
v0.3.0:

```bash
cp -r connectors/source-example connectors/source-acme
# edit go.mod (module name), main.go, source.go, .gitignore, .dockerignore, meta.json, README.md
```

Connectors in this repository are written in Go on synclet-go-sdk and use ordinary Go libraries for
everything else (HTTP clients, database drivers). For a larger example, read `source-postgres`.

### The SDK API

A connector is a type with methods, and `main` hands it to the SDK:

```go
type Source struct{}

func main() { cli.RunSource(Source{}) }
```

- `cli.RunSource` and `cli.RunDestination` parse the command line, read the config, catalog and state files,
  run the command and exit: 0 on success, 1 on failure.
- A source implements `synclet.Source[C]`: `Spec(ctx)`, `Check(ctx, cfg)`, `Discover(ctx, cfg)` and
  `Read(ctx, cfg, catalog, state, em)`. A destination implements `synclet.Destination[C]`: `Spec`, `Check`
  and `Write(ctx, cfg, catalog, in, em)`, which echoes each STATE through `em` once the records before it are
  committed.
- `C` is your configuration struct. The SDK decodes the configuration JSON into it before every call, so
  methods take a typed `cfg`, never a file path.
- `ctx` is cancelled on SIGTERM. Pass it to every network and database call and check `ctx.Err()` in loops
  that do no I/O, so a cancelled sync stops promptly.
- A non-nil error from `Check` becomes a FAILED `CONNECTION_STATUS` whose message is the error text.
- Messages go out through the `*synclet.Emitter`: `em.Record`, `em.StreamState`, `em.GlobalState`,
  `em.Log`. `synclet.Logger(ctx)` returns a `slog.Logger` whose records become LOG messages.

### Tests

`testkit.RunSource` and `testkit.RunDestination` run one command in-process and return its parsed messages.
Every source runs the conformance suite, `testkit.RunSourceSuite`, with a working config, a config `check`
must reject and a configured catalog. It checks spec, check, discover and read against the protocol: record
schemas, state order, incremental resume and that a read stops once its context is cancelled. See
`source-example/source_test.go` and `TestConformance` in `source-postgres`.

Tests use no build tags. Tests that need Docker (testcontainers) call `t.Skip` under `testing.Short()`, so
`go test -short ./...` runs everything else without Docker; `go test ./...` runs all of them. CI runs
`go vet`, `golangci-lint` and `go test -race ./...` for every changed connector, with Docker available.
Tests must not need real credentials: use testcontainers for databases, MinIO for S3 and recorded HTTP
fixtures or a fake server for API sources.

Each connector's `.gitignore` and `.dockerignore` list its built binary (`/source-acme`), so `go build` output
stays out of git and the image context.

The image must build for `linux/amd64` and `linux/arm64`. Avoid installing anything that needs network
access at runtime, and keep `ENTRYPOINT` pointing at the connector binary.

## 2. Fill in meta.json

```json
{
  "$schema": "../../schema/meta.schema.json",
  "name": "Acme",
  "connector_type": "source",
  "version": "0.1.0",
  "release_stage": "alpha",
  "support_level": "community",
  "source_type": "api",
  "license": "MIT"
}
```

| Field | Required | Notes |
|---|---|---|
| `name` | yes | Display name in Synclet |
| `connector_type` | yes | `source` or `destination`; must match the directory prefix |
| `version` | yes | Semver. The image tag Synclet will run. Bump it to release |
| `release_stage` | yes | `alpha`, `beta`, `generally_available`, `custom` |
| `support_level` | yes | `community`, `certified` |
| `license` | yes | The connector's own license (SPDX id). Independent of this repository's license |
| `source_type` | no | `api`, `database`, `file`; empty for destinations |
| `documentation_url`, `icon_url` | no | Shown in the connector catalog |
| `breaking_changes` | no | `{"2.0.0": {"message": "...", "upgrade_deadline": "2027-01-31"}}` |
| `tombstone` | no | `true` hides the connector in Synclet without deleting its history |

The schema documents the remaining optional fields (`tags`, `allowed_hosts`, `resource_requirements`, ...)
and which registry field each one becomes. Editors that understand `$schema` autocomplete them.

## 3. Generate spec.json

```bash
task spec CONNECTOR=source-acme
```

This builds the image, runs `spec` and writes the result to `spec.json`. Synclet renders the connector's
configuration form from it without pulling the image. CI rebuilds the image and fails if `spec.json` is
stale, so rerun this whenever the spec changes.

## 4. Check locally and open a PR

```bash
task validate
task test CONNECTOR=source-acme
task lint CONNECTOR=source-acme
task spec:check CONNECTOR=source-acme
task entry CONNECTOR=source-acme | jq .     # the registry entry a release will publish
```

On the pull request CI validates all metadata, runs vet, lint and tests and builds the image for the
changed connectors only, compares `spec.json` with the image, and fails if code changed while `version`
still equals an already released version.

## Releasing a new version

Bump `version` in `meta.json` (plus `spec.json` if the spec changed) and merge. Then tag the merge commit
and push the tag:

```bash
git tag source-acme-v0.2.0 && git push origin source-acme-v0.2.0
```

The release workflow checks that the tag matches `meta.json`, builds
`ghcr.io/synclet-io/connectors/source-acme:<version>` (also `latest`) for linux/amd64 and linux/arm64,
runs `spec` on the pushed image for both platforms, checks the image can be pulled anonymously and then
sends the connector's registry entry to
[synclet-connector-registry](https://github.com/synclet-io/synclet-connector-registry). If any step fails,
nothing is sent and the registry keeps the previous version. A failed release can be re-run from
**Actions → Release → Run workflow** with the same tag.

Changes that only touch `meta.json` (other than `version`), `spec.json`, `*.md` or `*_test.go` do not need
a version bump in CI, but reach the registry only with the connector's next release.

## Removing a connector

Set `"tombstone": true`, bump `version` and release it. Synclet skips tombstoned entries, so the connector
disappears from the catalog on the next repository sync while its images stay available.
