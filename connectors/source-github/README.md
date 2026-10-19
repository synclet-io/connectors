# source-github

[GitHub](https://github.com) source for Synclet, a manifest connector run by the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON schema per
stream, no Go. Reads repository metadata, issues, pull requests, commits, releases, issue comments and
stargazers of a list of repositories through the REST API.

```bash
task test CONNECTOR=source-github                     # no network, no credentials
go run ./cmd/manifest-lint ../source-github           # from connectors/source-declarative
docker build --build-arg BASE=ghcr.io/synclet-io/connectors/source-declarative:dev -t ghcr.io/synclet-io/connectors/source-github:dev connectors/source-github
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `access_token` | yes | Personal access token (fine-grained or classic) with read access to every listed repository. Sent only as `Authorization: Bearer <token>`. |
| `repositories` | yes | Repositories to read, as `owner/name`, such as `["octo-example/api", "octo-example/web"]`. |
| `start_date` | yes | RFC 3339 time; issues, issue comments and commits updated before it are not read. |

```json
{"access_token": "<token>", "repositories": ["octo-example/api"], "start_date": "2026-01-01T00:00:00Z"}
```

A fine-grained token needs read access to *Metadata*, *Issues*, *Pull requests* and *Contents* of the
repositories. `check` requests the first repository (`GET /repos/<owner>/<name>`).

## Streams

Every stream runs once per configured repository, and every record carries `repository` (`owner/name`).

| Stream | Route | Sync modes | Primary key | Cursor |
|---|---|---|---|---|
| `repositories` | `repos/<repo>` | full refresh | `id` | |
| `issues` | `repos/<repo>/issues?state=all&sort=updated&direction=asc&since=` | full refresh, incremental | `id` | `updated_at` |
| `pull_requests` | `repos/<repo>/pulls?state=all&sort=updated&direction=asc` | full refresh | `id` | |
| `commits` | `repos/<repo>/commits?since=` | full refresh, incremental | `repository`, `sha` | `commit.committer.date` |
| `releases` | `repos/<repo>/releases` | full refresh | `id` | |
| `issue_comments` | `repos/<repo>/issues/comments?sort=updated&direction=asc&since=` | full refresh, incremental | `id` | `updated_at` |
| `stargazers` | `repos/<repo>/stargazers` (`Accept: application/vnd.github.star+json`) | full refresh | `repository`, `user.id` | |

Records are passed through as GitHub sends them. `issues` includes pull requests, as the GitHub API does
(they carry a `pull_request` object). `commits` lists the default branch. Lists are paged by the `Link`
header with `per_page=100`.

Not read in this version: GitHub App authentication, GraphQL, GitHub Enterprise Server, and streams
the format cannot express, such as an incremental `pull_requests` (the endpoint has no `since`) and
per-pull-request children (reviews, review comments, commits of a pull request), which need a parent
stream that is itself partitioned by repository.

## Incremental sync

`issues`, `issue_comments` and `commits` keep a cursor per repository:

```json
{"partitions": {"octo-example/api": {"updated_at": "2026-08-12T10:00:00Z"}, "octo-example/web": {"updated_at": "2026-09-12T10:00:00Z"}}}
```

The next read sends `since=<saved cursor minus 1h>` and drops records older than that bound; the boundary
records are read again and deduplicated by the primary key. A repository added to the list later is read
from `start_date`. A full refresh reads from `start_date` and saves no state.

The `commits` cursor is the committer date, and GitHub's `since` filters on it. A commit whose committer
date is older than the saved cursor when it reaches the default branch, such as one merged by a fast-forward
or a merge commit that keeps the original commits' dates, is not read by an incremental sync. Run a full
refresh of `commits` periodically (for example weekly) to pick such commits up.

## Errors and rate limits

- 401 (bad credentials) and 403 (missing permission) fail `check` and every read as configuration errors
  with GitHub's `message`.
- 429, and 403 with `x-ratelimit-remaining: 0` (the primary rate limit), wait for `Retry-After` or until
  `x-ratelimit-reset`, then repeat the request, up to five times.
- A 403 of the secondary rate limit, which carries `Retry-After` while `x-ratelimit-remaining` is above 0,
  is not waited for: it fails the read as a configuration error with GitHub's `message`. Rerun the sync
  later, or read fewer repositories per connection.
- 5xx responses and network errors are retried three times with a doubling delay from 1s.
- A repository that does not exist or that the token cannot see answers 404 and fails the read.

## Tests

`manifest_test.go` runs the engine against hand-written synthetic fixtures in `testdata/` (two
repositories, two `Link`-paged pages per list stream, one of them reached through an absolute
`https://api.github.com/repositories/<id>/...` link as GitHub sends it) without network or credentials: the
SDK conformance suite for full refresh and for each incremental stream, `check` with 401 and 403, a one-time
429, both repositories per stream, the `since` sent on a first and on a resumed read, and the `star+json`
`Accept` header of `stargazers`.
