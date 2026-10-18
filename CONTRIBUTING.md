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

Start from the connector closest to yours. `source-postgres` is the reference connector, built on
[synclet-go-sdk](https://github.com/synclet-io/synclet-go-sdk):

```bash
cp -r connectors/source-postgres connectors/source-acme
# edit go.mod (module name), the Go code, .gitignore, .dockerignore, meta.json, README.md; empty CHANGELOG.md
```

Connectors in this repository are written in Go on synclet-go-sdk and use ordinary Go libraries for
everything else (HTTP clients, database drivers).

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
`TestConformance` in `source-postgres/integration_test.go`.

Tests use no build tags. Tests that need Docker (testcontainers) call `t.Skip` under `testing.Short()`, so
`go test -short ./...` runs everything else without Docker; `go test ./...` runs all of them. CI runs
`go vet`, `golangci-lint` and `go test -race ./...` for every changed connector, with Docker available.
Tests must not need real credentials: use testcontainers for databases, MinIO for S3 and recorded HTTP
fixtures for API sources (see
[Testing API sources with recorded HTTP](#testing-api-sources-with-recorded-http)).

Each connector's `.gitignore` and `.dockerignore` list its built binary (`/source-acme`), so `go build` output
stays out of git and the image context.

The image must build for `linux/amd64` and `linux/arm64`. Avoid installing anything that needs network
access at runtime, and keep `ENTRYPOINT` pointing at the connector binary.

### Testing API sources with recorded HTTP

API sources test against an `httptest` server that serves hand-written JSON from `testdata/`.
`source-openexchangerates` is the reference; copy its `fixtures_test.go`.

Why not go-vcr cassettes: recording needs a real account (often on a paid plan), every cassette needs
scrubbing hooks for tokens and account data, cassettes go stale and have to be re-recorded when a
request changes, they add a dependency, and the error responses worth testing (401, 403, a plan that
lacks a route) cannot be recorded on purpose. The price of hand-written fixtures is that they can
drift from the real API, so start them from the response examples in the API's documentation.

- **Layout.** `testdata/` mirrors the API paths: `GET /api/latest.json` is answered by
  `testdata/latest.json`, `GET /api/historical/2026-10-16.json` by `testdata/historical/2026-10-16.json`.
  Error bodies go in `testdata/errors/`, named after the API's error code (`invalid_app_id.json`).
- **Helper per connector.** Copy `fixtures_test.go` and adapt it to the API (path prefix, auth header,
  pagination); there is no shared test package across connectors. It provides `fixtureServer(t,
  overrides)`, which serves `testdata/`, answers a request with the wrong credentials with the API's
  auth error, lets a test replace the status and file of one path, and records the paths requested,
  so a test can assert exactly which requests a read made.
- **Injection.** The connector's `Source` struct has unexported fields for the base URL, the clock,
  the `*http.Client` and the retry delay. They are zero in `main`, where the connector uses the real
  API, `time.Now`, its own client and real delays; tests set them to the fixture server, a fixed
  time and a millisecond delay. Nothing is configurable from the connector's config for tests.
- **Synthetic data.** Fixtures hold made-up values only: the documentation's examples, fake ids,
  `example.com` emails, round or invented amounts. Tests use an obviously fake credential
  (`test-app-id`) that the fixture server checks.
- **Scrubbing.** When a fixture starts from a real response, keep only the body; drop headers and
  replace tokens, account and user ids, names, emails and amounts. Before committing, grep
  `testdata/` for the real key and the account's identifiers.
- **No secret in a URL.** Send credentials in a header. The fixture server fails the test when a
  request carries the credential as a query parameter, so it cannot leak into a logged URL or an
  error message.
- **Retries.** Retry and backoff live in the connector. Test them with an inline `httptest` handler
  that fails first and then succeeds, and test that errors which must not be retried (auth, plan,
  quota) are requested once.

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
| `version` | yes | Semver. The image tag Synclet will run. Set by release-please; start a new connector at `0.1.0` |
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
changed connectors only, and compares `spec.json` with the image. Do not change `version` or `CHANGELOG.md`
by hand.

## Releasing

Releases are cut by [release-please](https://github.com/googleapis/release-please) from
[Conventional Commits](https://www.conventionalcommits.org). Use the connector directory as the scope:

```
fix(source-acme): retry on HTTP 429
feat(source-acme)!: rename the `items` stream to `orders`
```

release-please assigns a commit to every connector whose directory it touches, whatever the scope says; the
scope is for people reading the history. `fix` releases a patch, `feat` a minor version and `!` or a
`BREAKING CHANGE:` footer a major version; `chore`, `docs`, `test`, `ci` and
`refactor` release nothing on their own. Record a breaking change in `breaking_changes` in `meta.json` too, so
Synclet can warn before the upgrade.

After a merge to main, release-please opens or updates one pull request per connector with releasable
changes, titled `chore: release <dir> <version>`. It bumps `version` in `meta.json`, the connector's entry in
`.release-please-manifest.json` and its `CHANGELOG.md`. Merging that pull request creates the GitHub release
and the tag `<dir>-v<version>`, which runs the release workflow: it checks that the tag matches
`meta.json`, builds `ghcr.io/synclet-io/connectors/<dir>:<version>` (also `latest`) for linux/amd64 and
linux/arm64, runs `spec` on the pushed image for both platforms, checks the image can be pulled anonymously
and, once it succeeds, the Registry workflow republishes `registry.json` on GitHub Pages. If any step fails, the registry keeps the previous
version. A failed release can be re-run from **Actions → Release → Run workflow** with the same tag.

A new connector needs a package in `release-please-config.json` (copy another connector's) and starts at
`"version": "0.1.0"` in `meta.json`, the config's `initial-version`. Leave it out of
`.release-please-manifest.json`: a connector missing there is not released yet, and release-please opens its
first release pull request at `0.1.0` whatever its commits are. Merging that pull request adds it to the
manifest. `task validate` fails until the package is there and `meta.json` declares `0.1.0`. After the first
release, make its GHCR package public (see the README).

## Removing a connector

Set `"tombstone": true` in a `feat(<dir>)!:` or `fix(<dir>):` commit and release it. Synclet skips tombstoned entries, so the connector
disappears from the catalog on the next repository sync while its images stay available.
