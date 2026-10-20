# source-gitlab

[GitLab](https://gitlab.com) source for Synclet, a manifest connector run by the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON schema per
stream, no Go. Reads projects, issues, merge requests, commits, pipelines, jobs, releases, tags, branches,
users, members, milestones and labels of a list of projects, and metadata, members, milestones and labels
of a list of groups, through the [REST API v4](https://docs.gitlab.com/api/rest/), on gitlab.com or a
self-managed instance.

```bash
task test CONNECTOR=source-gitlab                     # no network, no credentials
go run ./cmd/manifest-lint ../source-gitlab           # from connectors/source-declarative
task build CONNECTOR=source-gitlab
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `access_token` | yes | Personal, project or group access token with the `read_api` scope, or an OAuth access token; sent only as `Authorization: Bearer <token>`. An OAuth token expires after two hours and is not refreshed by the connector. |
| `api_url` | yes | Host of the instance, `gitlab.com` by default, or the host (and port) of a self-managed instance. The API is read at `https://<api_url>/api/v4/`. |
| `projects` | yes | Numeric project IDs, such as `["101", "202"]`. |
| `groups` | no | Numeric group IDs, for `groups`, `group_members`, `group_milestones` and `group_labels`. |
| `start_date` | yes | RFC 3339 time; issues, merge requests and pipelines updated, and commits committed, before it are not read. |

```json
{"access_token": "glpat-<token>", "api_url": "gitlab.com", "projects": ["101", "202"], "groups": ["11"], "start_date": "2026-01-01T00:00:00Z"}
```

`check` requests the first project (`GET /projects/<id>`).

## Streams

Project streams run once per configured project, group streams once per configured group.

| Stream | Route | Sync modes | Primary key | Cursor |
|---|---|---|---|---|
| `projects` | `projects/<id>` | full refresh | `id` | |
| `groups` | `groups/<id>?with_projects=false` | full refresh | `id` | |
| `issues` | `projects/<id>/issues?scope=all&order_by=updated_at&sort=asc&updated_after=` | full refresh, incremental | `id` | `updated_at` |
| `merge_requests` | `projects/<id>/merge_requests?scope=all&order_by=updated_at&sort=asc&updated_after=` | full refresh, incremental | `id` | `updated_at` |
| `pipelines` | `projects/<id>/pipelines?order_by=updated_at&sort=asc&updated_after=` | full refresh, incremental | `id` | `updated_at` |
| `commits` | `projects/<id>/repository/commits?since=` | full refresh, incremental | `project_id`, `id` | `committed_date` |
| `jobs` | `projects/<id>/jobs` | full refresh | `id` | |
| `releases` | `projects/<id>/releases` | full refresh | `project_id`, `tag_name` | |
| `tags` | `projects/<id>/repository/tags` | full refresh | `project_id`, `name` | |
| `branches` | `projects/<id>/repository/branches` | full refresh | `project_id`, `name` | |
| `users` | `projects/<id>/users` | full refresh | `id` | |
| `project_members` | `projects/<id>/members` | full refresh | `project_id`, `id` | |
| `project_milestones` | `projects/<id>/milestones` | full refresh | `id` | |
| `project_labels` | `projects/<id>/labels` | full refresh | `project_id`, `id` | |
| `group_members` | `groups/<id>/members` | full refresh | `group_id`, `id` | |
| `group_milestones` | `groups/<id>/milestones` | full refresh | `id` | |
| `group_labels` | `groups/<id>/labels` | full refresh | `group_id`, `id` | |

Lists are paged by the `Link` header with `per_page=100`. Records are passed through as GitLab sends them.
Where GitLab's record has no project or group id (commits, jobs, releases, tags, branches, members, labels),
the connector adds `project_id` or `group_id` as the configured ID, a string; issues, merge requests,
pipelines and milestones keep GitLab's own integer `project_id`. `users` lists every project's users and
is deduplicated by `id`. `commits` lists the default branch.

## Incremental sync

`issues`, `merge_requests`, `pipelines` and `commits` keep a cursor per project:

```json
{"partitions": {"101": {"updated_at": "2026-09-01T10:00:00.000Z"}, "202": {"updated_at": "2026-07-01T10:00:00.000Z"}}}
```

The next read sends `updated_after` (`since` for commits) with the saved cursor minus 1h and drops older
records; the boundary records are read again and deduplicated by the primary key. A project added to the
list later is read from `start_date`. A full refresh reads from `start_date` and saves no state.

The `commits` cursor is the committed date. A commit that reaches the default branch with an older committed
date than the saved cursor (a fast-forward of an old branch, a merge that keeps the original commits) is not
read by an incremental sync; run a full refresh of `commits` periodically to pick such commits up.

## Errors and rate limits

- 401 (bad or expired token) and 403 (missing scope or access) fail `check` and every read as configuration
  errors with GitLab's `message`.
- 429 waits for `Retry-After`, or until `RateLimit-Reset`, then repeats the request, up to five times.
- 5xx responses and network errors are retried three times with a doubling delay from 1s.
- A project or group that does not exist or that the token cannot see answers 404 and fails the read.

Not read in this version:

- the projects of a group: list each project ID in `projects`. The format runs a stream per configured value
  or per record of an unpartitioned parent, not per project of a group;
- projects and groups by path (`group/project`): a `/` inside one path segment cannot be expressed;
- an instance under a path (`https://host/gitlab/`) or plain `http://`;
- children of issues, merge requests or pipelines (notes, approvals, commits of a merge request, jobs of a
  pipeline, which `jobs` covers per project), which need a parent stream that is itself partitioned by
  project; epics; incremental `jobs` (the endpoint has no time filter).

## Tests

`manifest_test.go` runs the engine against hand-written synthetic fixtures in `testdata/` (two projects and a
group, two `Link`-paged pages per list of the first project and the group) without network or credentials:
the SDK conformance suite for full refresh and for each incremental stream, `check` with 401 and 403, a
one-time 429, records per project in every project stream, and the filters and order sent on a first and on
a resumed read.
