# Synclet connectors

MIT-licensed source and destination connectors for [Synclet](https://github.com/synclet-io/synclet), written in
Go on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Every connector speaks the Airbyte protocol,
ships as a Docker image and is listed in this repository's connector registry, published on GitHub Pages, which
Synclet installs read from:

```
https://synclet-io.github.io/connectors/registry.json
```

```
connectors/<name>/        one connector per directory: Dockerfile, meta.json, spec.json, Go code
schema/meta.schema.json   JSON Schema every meta.json is validated against
scripts/                  Go tool: validate metadata, resolve release tags, build registry.json
.github/workflows/        ci.yml (pull requests, main), release-please.yml (main), release.yml (release tags),
                          registry.yml (after each release)
release-please-config.json, .release-please-manifest.json   one release-please package per connector
Taskfile.yml              local shortcuts
```

| Connector | Image |
|---|---|
| [source-example](connectors/source-example) | `ghcr.io/synclet-io/connectors/source-example` |
| [source-airtable](connectors/source-airtable) | `ghcr.io/synclet-io/connectors/source-airtable` |
| [source-applovin](connectors/source-applovin) | `ghcr.io/synclet-io/connectors/source-applovin` |
| [source-bigquery](connectors/source-bigquery) | `ghcr.io/synclet-io/connectors/source-bigquery` |
| [source-facebook-marketing](connectors/source-facebook-marketing) | `ghcr.io/synclet-io/connectors/source-facebook-marketing` |
| [source-google-ads](connectors/source-google-ads) | `ghcr.io/synclet-io/connectors/source-google-ads` |
| [source-google-sheets](connectors/source-google-sheets) | `ghcr.io/synclet-io/connectors/source-google-sheets` |
| [source-mssql](connectors/source-mssql) | `ghcr.io/synclet-io/connectors/source-mssql` |
| [source-mysql](connectors/source-mysql) | `ghcr.io/synclet-io/connectors/source-mysql` |
| [source-openexchangerates](connectors/source-openexchangerates) | `ghcr.io/synclet-io/connectors/source-openexchangerates` |
| [source-postgres](connectors/source-postgres) | `ghcr.io/synclet-io/connectors/source-postgres` |
| [source-revenuecat](connectors/source-revenuecat) | `ghcr.io/synclet-io/connectors/source-revenuecat` |
| [source-s3](connectors/source-s3) | `ghcr.io/synclet-io/connectors/source-s3` |
| [source-solidgate](connectors/source-solidgate) | `ghcr.io/synclet-io/connectors/source-solidgate` |
| [source-tiktok-marketing](connectors/source-tiktok-marketing) | `ghcr.io/synclet-io/connectors/source-tiktok-marketing` |
| [source-zendesk-support](connectors/source-zendesk-support) | `ghcr.io/synclet-io/connectors/source-zendesk-support` |
| [destination-bigquery](connectors/destination-bigquery) | `ghcr.io/synclet-io/connectors/destination-bigquery` |
| [destination-google-sheets](connectors/destination-google-sheets) | `ghcr.io/synclet-io/connectors/destination-google-sheets` |
| [destination-postgres](connectors/destination-postgres) | `ghcr.io/synclet-io/connectors/destination-postgres` |
| [destination-s3](connectors/destination-s3) | `ghcr.io/synclet-io/connectors/destination-s3` |

## How it works

```
pull request / main ──► ci.yml
                        ├─ test the scripts, validate every meta.json and spec.json
                        ├─ check release-please lists every connector at its meta.json version
                        ├─ dry-run registry entry generation
                        └─ per changed connector: go vet, golangci-lint, go test (Docker available for
                           testcontainers), docker build, spec.json == `spec` output

main ─────────────────► release-please.yml
                        └─ one release pull request per connector with releasable commits; merging it
                           bumps meta.json and CHANGELOG.md and creates the tag <dir>-v<version>

tag <dir>-v<version> ──► release.yml
                        ├─ plan: the tag must match connectors/<dir>/meta.json's version
                        └─ image: spec check, build linux/amd64 + linux/arm64, push
                              ghcr.io/synclet-io/connectors/<dir>:<version> and :latest,
                              run `spec` on the pushed image for both platforms, check it is public

Release succeeded ────► registry.yml (on main)
                        └─ rebuild registry.json from every connector's newest published release and
                           deploy it to GitHub Pages
```

Each connector is versioned and released on its own, by [release-please](https://github.com/googleapis/release-please)
from [Conventional Commits](https://www.conventionalcommits.org) (see [CONTRIBUTING.md](CONTRIBUTING.md#releasing)).

`registry.yml` runs only after a release succeeded, on main, because the `github-pages` environment lets only
main deploy; **Actions → Registry → Run workflow** republishes the file by hand. When several releases finish
together, GitHub cancels the registry runs still waiting behind the running one; the last run covers all of
them. It builds `registry.json` with
`go run ./scripts registry --released`: for every connector it takes the newest `<dir>-v<version>` tag whose
image can be pulled anonymously and reads `meta.json` and `spec.json` as of that tag. A release that is still
running or failed is therefore never listed, and two releases finishing in either order publish the same file.
Entries use the [Airbyte OSS registry format](https://connectors.airbyte.com/files/registries/v0/oss_registry.json);
breaking changes are written as `releases.breakingChanges` with camelCase fields. `task entry CONNECTOR=<dir>`
prints one entry from the working tree.

### Release token

Tags and pull requests created with the default `GITHUB_TOKEN` start no workflows, so release-please runs with a
token minted from a GitHub App. The workflow reads two repository (or organization) secrets:

| Secret | Value |
|---|---|
| `REGISTRY_DISPATCH_APP_ID` | App ID of a GitHub App installed on `synclet-io/connectors` with **Contents**, **Pull requests** and **Issues: Read and write** |
| `REGISTRY_DISPATCH_APP_PRIVATE_KEY` | A private key (PEM) of that App |

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
task entry CONNECTOR=source-example             # print a connector's registry entry from the working tree
task registry                                   # write dist/registry.json with every connector (working tree)
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
