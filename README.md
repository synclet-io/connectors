# Synclet connectors

MIT-licensed source and destination connectors for [Synclet](https://github.com/synclet-io/synclet), written in
Go on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Every connector speaks the Airbyte protocol,
ships as a Docker image and is listed in the
[Synclet connector registry](https://github.com/synclet-io/synclet-connector-registry), which Synclet installs read
from:

```
https://synclet-io.github.io/synclet-connector-registry/registry.json
```

```
connectors/<name>/        one connector per directory: Dockerfile, meta.json, spec.json, Go code
schema/meta.schema.json   JSON Schema every meta.json is validated against
scripts/                  Go tool: validate metadata, resolve release tags, build registry entries
.github/workflows/        ci.yml (pull requests, main), release.yml (release tags)
Taskfile.yml              local shortcuts
```

| Connector | Image |
|---|---|
| [source-example](connectors/source-example) | `ghcr.io/synclet-io/connectors/source-example` |

## How it works

```
pull request / main ──► ci.yml
                        ├─ test the scripts, validate every meta.json and spec.json
                        ├─ fail if a connector's code changed but its version is already released
                        ├─ dry-run registry entry generation
                        └─ per changed connector: go vet, golangci-lint, go test (Docker available for
                           testcontainers), docker build, spec.json == `spec` output

tag <dir>-v<version> ──► release.yml
                        ├─ plan: the tag must match connectors/<dir>/meta.json's version
                        ├─ image: spec check, build linux/amd64 + linux/arm64, push
                        │     ghcr.io/synclet-io/connectors/<dir>:<version> and :latest,
                        │     run `spec` on the pushed image for both platforms, check it is public
                        └─ registry: repository_dispatch `connector-released` to
                              synclet-io/synclet-connector-registry with the connector's entry
```

Each connector is versioned and released on its own: bump `version` in its `meta.json`, merge, then push the
tag `<dir>-v<version>`. The `registry` job runs only after the image job succeeded, so an entry for a failed
build is never sent. The registry's `update-registry` workflow replaces the connector's entry in
`registry.json` and GitHub Pages redeploys it.

The entry is built by `go run ./scripts entry` from `meta.json` and `spec.json` in the [Airbyte OSS registry
format](https://connectors.airbyte.com/files/registries/v0/oss_registry.json); breaking changes are written as
`releases.breakingChanges` with camelCase fields. `task entry CONNECTOR=<dir>` prints it locally.

### Registry dispatch

Sending `repository_dispatch` to another repository needs more than the default `GITHUB_TOKEN`. The release
workflow mints a token from a GitHub App and reads two repository (or organization) secrets:

| Secret | Value |
|---|---|
| `REGISTRY_DISPATCH_APP_ID` | App ID of a GitHub App installed on `synclet-io/synclet-connector-registry` with **Contents: Read and write** |
| `REGISTRY_DISPATCH_APP_PRIVATE_KEY` | A private key (PEM) of that App |

The App that `synclet-io/synclet` already uses for its connector releases works. Until both secrets exist the
`registry` job fails with a message naming them; the image is published regardless, and **Actions → Release →
Run workflow** with the same tag sends the entry once they are set.

### Image visibility

The first push of a connector creates a **private** GHCR package. Synclet pulls images anonymously, so the
release fails at "Image is publicly pullable" until the package is public: open **synclet-io → Packages →
connectors/\<name\> → Package settings → Change visibility → Public** once per connector, then re-run the
release for the same tag.

## Local development

Requires Go 1.26+, Docker, [Task](https://taskfile.dev), `jq` and golangci-lint v2.

```bash
task validate                                   # schema-check every meta.json and spec.json
task test CONNECTOR=source-example              # go vet + go test
task lint CONNECTOR=source-example              # golangci-lint
task build CONNECTOR=source-example             # docker build for the local platform
task spec CONNECTOR=source-example              # build, run `spec`, write spec.json
task spec:check CONNECTOR=source-example        # fail if spec.json is stale
task entry CONNECTOR=source-example             # print the registry entry a release sends
task registry                                   # write dist/registry.json with every connector
```

### Building against a local synclet-go-sdk

To work on the SDK and a connector together, point both Go and Docker at a local checkout:

```bash
task sdk:local SDK_DIR=../synclet-go-sdk                              # writes connectors/go.work (git-ignored)
task spec CONNECTOR=source-example SDK_DIR=../synclet-go-sdk          # docker build --build-context sdk=../synclet-go-sdk
```

Each connector's Dockerfile has an empty `sdk` stage; `--build-context sdk=<dir>` replaces it, and the build then
adds `replace github.com/synclet-io/synclet-go-sdk => <dir>` to the image's copy of `go.mod`. Nothing in the
repository changes, so CI always builds against the published module.

See [CONTRIBUTING.md](CONTRIBUTING.md) for adding a connector.

## License

[MIT](LICENSE). Each connector also declares its license in the `license` field of its `meta.json`, which is
published in the registry and shown in Synclet.

Airbyte is a trademark of Airbyte, Inc.; this project is not affiliated with or endorsed by Airbyte, Inc.
