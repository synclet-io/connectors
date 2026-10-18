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

Copy `source-example`, which is built on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk):

```bash
cp -r connectors/source-example connectors/source-acme
# edit go.mod (module name), source.go, meta.json, README.md
```

Connectors in this repository are written in Go on synclet-go-sdk and use ordinary Go libraries for
everything else (HTTP clients, database drivers). CI runs `go vet`, `golangci-lint` and `go test` for every
changed connector. Tests must not need real credentials: use testcontainers for databases (Docker is
available in CI), MinIO for S3 and recorded HTTP fixtures for API sources.

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
