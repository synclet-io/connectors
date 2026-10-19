# source-jira

[Jira Cloud](https://developer.atlassian.com/cloud/jira/platform/rest/v3/) source, a manifest connector run by
the [source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml), one schema per stream
in [`schemas/`](schemas) and recorded fixtures in [`testdata/`](testdata). Reads projects, users, issue fields
and boards in full, and issues incrementally by their `updated` time.

```bash
task test CONNECTOR=source-jira     # no network, no credentials
task build CONNECTOR=source-jira
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-jira:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `domain` | yes | Host name of the site, such as `your-site.atlassian.net`, without `https://` |
| `email` | yes | Email of the Atlassian account the API token belongs to |
| `api_token` | yes | [API token](https://id.atlassian.com/manage-profile/security/api-tokens) of that account; sent with the email as HTTP basic auth |
| `start_date` | yes | RFC 3339 time; issues last updated before it are not read |

```json
{"domain": "your-site.atlassian.net", "email": "you@example.com", "api_token": "<token>", "start_date": "2026-01-01T00:00:00Z"}
```

The connector sees what the account sees: projects, boards and issues it cannot browse are not read. `check`
requests one project; a wrong email or token fails it with Jira's message (HTTP 401), as does an account
without access (HTTP 403).

## Streams

| Stream | Route | Sync modes | Primary key | Pagination |
|---|---|---|---|---|
| `projects` | `GET /rest/api/3/project/search` (expanded with description, lead, issue types, URL, keys, insight) | full refresh | `id` | `startAt` until `isLast` |
| `users` | `GET /rest/api/3/users/search` | full refresh | `accountId` | `startAt` until an empty page |
| `issue_fields` | `GET /rest/api/3/field` | full refresh | `id` | none, one response |
| `boards` | `GET /rest/agile/1.0/board` | full refresh | `id` | `startAt` until `isLast` |
| `issues` | `GET /rest/api/3/search/jql` with `fields=*all` | full refresh, incremental | `id` | `nextPageToken` until `isLast` |

`issue_fields` holds the system and custom fields of company-managed and team-managed projects alike;
team-managed fields carry a `scope` naming their project. Fields used only in projects the account cannot
browse are left out.

`boards` needs Jira Software: on a site without it Jira answers `/rest/agile/1.0/board` with HTTP 404 or
403 and the sync fails, so deselect `boards` there.

Records are passed through as Jira sends them; an issue keeps its fields, custom fields included, under
`fields`.

Not read in this version:

- `sprints` (`/rest/agile/1.0/board/{id}/sprint`): Jira answers it with HTTP 400 for every board that is
  not a scrum board, and a manifest can neither skip a 400 nor read only some boards' children;
- issue comments and worklogs (a request per issue), and OAuth 2.0 (3LO) sign-in.

## Incremental sync

`issues` uses the source-defined cursor `fields.updated` and asks Jira for

```
updated >= <since> ORDER BY updated ASC
```

where `since` is `start_date` on the first read, else the saved cursor minus one day, as milliseconds since
the Unix epoch. JQL reads an unquoted number as an instant, so the query does not depend on the account's
time zone. The one-day lookback covers issues that `/search/jql` indexes late (its results are eventually
consistent); the issues it reads again are deduplicated by `id` downstream. Issues updated before
`since` are dropped even if Jira returns them. After the last issue the connector saves the greatest
`updated` it read, as Jira sent it:

```json
{"updated": "2026-03-02T10:15:30.000+0200"}
```

A full refresh reads every issue updated since `start_date` and saves no state.

## Fixtures

Every test runs against `testdata/`, served to the engine by `declarativetest`: synthetic projects, users,
fields, boards and issues on `example.atlassian.net`, two pages per stream. They can be re-recorded from a
free Jira Cloud site; replace every name, email, account id and URL before committing.
