# Synclet connectors

MIT-licensed source and destination connectors for [Synclet](https://github.com/synclet-io/synclet), written in
Go on [synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk). Every connector speaks the Airbyte protocol,
ships as a Docker image and is listed in this repository's connector registry, published on GitHub Pages, which
Synclet installs read from:

```
https://synclet-io.github.io/connectors/registry.json
```

To write a connector, read the guides on the Synclet docs site:
[Writing a connector](https://synclet-io.github.io/synclet-site/docs/connector-development/writing-a-connector/)
walks through source-postgres (interfaces, spec, check, discover, read, state, the conformance suite and
testcontainers), and
[Publishing a connector](https://synclet-io.github.io/synclet-site/docs/connector-development/publishing-a-connector/)
covers the release tag, the GHCR image, the registry entry and running your own connectors repository from
[synclet-connectors-template](https://github.com/synclet-io/synclet-connectors-template).

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
| [destination-mssql](connectors/destination-mssql) | `ghcr.io/synclet-io/connectors/destination-mssql` |
| [source-example](connectors/source-example) | `ghcr.io/synclet-io/connectors/source-example` |
| [source-airtable](connectors/source-airtable) | `ghcr.io/synclet-io/connectors/source-airtable` |
| [source-amplitude](connectors/source-amplitude) | `ghcr.io/synclet-io/connectors/source-amplitude` |
| [source-applovin](connectors/source-applovin) | `ghcr.io/synclet-io/connectors/source-applovin` |
| [source-bigquery](connectors/source-bigquery) | `ghcr.io/synclet-io/connectors/source-bigquery` |
| [source-declarative](connectors/source-declarative) | `ghcr.io/synclet-io/connectors/source-declarative` (engine of the manifest connectors; hidden from the catalog) |
| [source-facebook-marketing](connectors/source-facebook-marketing) | `ghcr.io/synclet-io/connectors/source-facebook-marketing` |
| [source-freshdesk](connectors/source-freshdesk) | `ghcr.io/synclet-io/connectors/source-freshdesk` |
| [source-gcs](connectors/source-gcs) | `ghcr.io/synclet-io/connectors/source-gcs` |
| [source-gitlab](connectors/source-gitlab) | `ghcr.io/synclet-io/connectors/source-gitlab` |
| [source-google-ads](connectors/source-google-ads) | `ghcr.io/synclet-io/connectors/source-google-ads` |
| [source-google-analytics-data-api](connectors/source-google-analytics-data-api) | `ghcr.io/synclet-io/connectors/source-google-analytics-data-api` |
| [source-google-search-console](connectors/source-google-search-console) | `ghcr.io/synclet-io/connectors/source-google-search-console` |
| [source-google-sheets](connectors/source-google-sheets) | `ghcr.io/synclet-io/connectors/source-google-sheets` |
| [source-hubspot](connectors/source-hubspot) | `ghcr.io/synclet-io/connectors/source-hubspot` |
| [source-mailchimp](connectors/source-mailchimp) | `ghcr.io/synclet-io/connectors/source-mailchimp` |
| [source-mixpanel](connectors/source-mixpanel) | `ghcr.io/synclet-io/connectors/source-mixpanel` |
| [source-mongodb](connectors/source-mongodb) | `ghcr.io/synclet-io/connectors/source-mongodb` |
| [source-mssql](connectors/source-mssql) | `ghcr.io/synclet-io/connectors/source-mssql` |
| [source-mysql](connectors/source-mysql) | `ghcr.io/synclet-io/connectors/source-mysql` |
| [source-notion](connectors/source-notion) | `ghcr.io/synclet-io/connectors/source-notion` |
| [source-openexchangerates](connectors/source-openexchangerates) | `ghcr.io/synclet-io/connectors/source-openexchangerates` |
| [source-pipedrive](connectors/source-pipedrive) | `ghcr.io/synclet-io/connectors/source-pipedrive` |
| [source-postgres](connectors/source-postgres) | `ghcr.io/synclet-io/connectors/source-postgres` |
| [source-revenuecat](connectors/source-revenuecat) | `ghcr.io/synclet-io/connectors/source-revenuecat` |
| [source-salesforce](connectors/source-salesforce) | `ghcr.io/synclet-io/connectors/source-salesforce` |
| [source-s3](connectors/source-s3) | `ghcr.io/synclet-io/connectors/source-s3` |
| [source-sentry](connectors/source-sentry) | `ghcr.io/synclet-io/connectors/source-sentry` |
| [source-shopify](connectors/source-shopify) | `ghcr.io/synclet-io/connectors/source-shopify` |
| [source-slack](connectors/source-slack) | `ghcr.io/synclet-io/connectors/source-slack` |
| [source-solidgate](connectors/source-solidgate) | `ghcr.io/synclet-io/connectors/source-solidgate` |
| [source-tiktok-marketing](connectors/source-tiktok-marketing) | `ghcr.io/synclet-io/connectors/source-tiktok-marketing` |
| [source-twilio](connectors/source-twilio) | `ghcr.io/synclet-io/connectors/source-twilio` |
| [source-typeform](connectors/source-typeform) | `ghcr.io/synclet-io/connectors/source-typeform` |
| [source-zendesk-support](connectors/source-zendesk-support) | `ghcr.io/synclet-io/connectors/source-zendesk-support` |
| [destination-bigquery](connectors/destination-bigquery) | `ghcr.io/synclet-io/connectors/destination-bigquery` |
| [destination-clickhouse](connectors/destination-clickhouse) | `ghcr.io/synclet-io/connectors/destination-clickhouse` |
| [destination-dev-null](connectors/destination-dev-null) | `ghcr.io/synclet-io/connectors/destination-dev-null` |
| [destination-gcs](connectors/destination-gcs) | `ghcr.io/synclet-io/connectors/destination-gcs` |
| [destination-google-sheets](connectors/destination-google-sheets) | `ghcr.io/synclet-io/connectors/destination-google-sheets` |
| [destination-kafka](connectors/destination-kafka) | `ghcr.io/synclet-io/connectors/destination-kafka` |
| [destination-mysql](connectors/destination-mysql) | `ghcr.io/synclet-io/connectors/destination-mysql` |
| [destination-postgres](connectors/destination-postgres) | `ghcr.io/synclet-io/connectors/destination-postgres` |
| [destination-s3](connectors/destination-s3) | `ghcr.io/synclet-io/connectors/destination-s3` |
| [destination-s3-data-lake](connectors/destination-s3-data-lake) | `ghcr.io/synclet-io/connectors/destination-s3-data-lake` |

## How it works

```
pull request / main ──► ci.yml
                        ├─ test the scripts, validate every meta.json and spec.json
                        ├─ check release-please lists every connector at its meta.json version
                        ├─ dry-run registry entry generation
                        └─ per changed connector: go vet, golangci-lint, go test (Docker available for
                           testcontainers), docker build, spec.json == `spec` output

main ─────────────────► release-please.yml
                        └─ one release pull request for every connector with releasable commits; merging
                           it bumps their meta.json and CHANGELOG.md and creates a tag <dir>-v<version>
                           per connector

tag <dir>-v<version> ──► release.yml
                        ├─ plan: the tag must match connectors/<dir>/meta.json's version
                        └─ image: spec check, build linux/amd64 + linux/arm64, push
                              ghcr.io/synclet-io/connectors/<dir>:<version> and :latest,
                              run `spec` on the pushed image for both platforms, check it is public

Release succeeded ────► registry.yml (on main)
                        └─ rebuild registry.json from every connector's newest published release and
                           deploy it to GitHub Pages

Mondays 06:00 UTC ────► api-watch.yml
                        ├─ check every pinned upstream API version, one issue per finding
                        └─ with ANTHROPIC_API_KEY: Claude drafts a bump pull request per new version
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
| `REGISTRY_DISPATCH_APP_ID` | App ID of a GitHub App installed on `synclet-io/connectors` with **Contents**, **Pull requests** and **Issues: Read and write** (the API watch uses it too) |
| `REGISTRY_DISPATCH_APP_PRIVATE_KEY` | A private key (PEM) of that App |

### Image visibility

The first push of a connector creates a **private** GHCR package. Synclet pulls images anonymously, so the
release fails at "Image is publicly pullable" until the package is public: open **synclet-io → Packages →
connectors/\<name\> → Package settings → Change visibility → Public** once per connector, then re-run the
release for the same tag.

## Upstream API versions

Connectors that pin an upstream API version declare it in `api_versions` in `meta.json` (see
[CONTRIBUTING.md](CONTRIBUTING.md#upstream-api-versions)). Every Monday `api-watch.yml` runs
`go run ./scripts apiwatch`, which asks each provider for its newest version and the sunset date of the pinned one
(a JSON endpoint or probe where there is one, the provider's versioning page otherwise) and writes the report as a
table into the run summary. `go run ./scripts apiwatch-issues` then keeps one issue labelled `api-watch` per
connector API that has a newer version or sunsets within 90 days (`sunset-90`, `sunset-30`), updates it as the date
nears and closes it once the pin is current. Checks that fail (a provider page changed shape) are collected in one
issue. The run fails while a pin is within 30 days of its sunset, or past it, without an open bump pull request.

With the `ANTHROPIC_API_KEY` repository secret set, a second job hands each new version to Claude
([claude-code-action](https://github.com/anthropics/claude-code-action), `claude-opus-5-5`, at most 60 turns), which
reads the provider's changelog, bumps the pin and adapts the connector. Claude may only read and edit files, run
`go build`, `go vet`, `go test` and `go run ./scripts validate`, and fetch pages of the provider's hosts; it gets no
token that can push. The workflow then commits as the release App on `api-bump/<connector>/<version>`, opens a pull
request labelled `api-bump` and `needs-review` (a draft when the connector's tests fail) and links it from the
issue. Nothing is merged automatically. A branch or pull request of that name, open or closed, stops the version
from being drafted again: to retry a rejected bump, delete the branch and run the workflow with `connector` set.

**Actions → API watch → Run workflow** takes `dry_run` (summary only, no issue or pull request changes),
`connector` and `bump`. Locally, `go run ./scripts apiwatch --report /tmp/report.json` checks the live providers and
`--fixtures scripts/apiwatch/testdata` replays the recorded responses the tests use;
`go run ./scripts apiwatch-issues --report /tmp/report.json --repo synclet-io/connectors --dry-run` prints the
issue changes without making them.

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
