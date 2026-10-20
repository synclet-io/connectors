# source-mailchimp

[Mailchimp](https://mailchimp.com) source, a manifest connector on the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON
schema per stream, no Go. Reads lists, list members, segments, tags, interest categories, campaigns, email
activity, reports and automations from the [Mailchimp Marketing API](https://mailchimp.com/developer/marketing/api/)
(`3.0`), campaigns, reports and automations incrementally.

```bash
task test CONNECTOR=source-mailchimp     # fixtures, no network, no credentials
task build CONNECTOR=source-mailchimp
docker run --rm -v "$PWD:/data" ghcr.io/synclet-io/connectors/source-mailchimp:dev check --config /data/config.json
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `api_key` | yes | Mailchimp API key, sent only as the basic auth password |
| `data_center` | yes | The account's data center, the suffix of the API key after the dash (`us21` for `…-us21`); requests go to `https://<data_center>.api.mailchimp.com/3.0/` |
| `start_date` | yes | RFC 3339 time; campaigns and automations created and reports of campaigns sent before it are not read |

## Streams

| Stream | Endpoint | Sync modes | Cursor |
|---|---|---|---|
| `lists` | `GET /lists` | full refresh | |
| `list_members` | `GET /lists/{list_id}/members`, for every list | full refresh | |
| `segments` | `GET /lists/{list_id}/segments`, for every list | full refresh | |
| `tags` | `GET /lists/{list_id}/tag-search`, for every list; each record carries `list_id` | full refresh | |
| `interest_categories` | `GET /lists/{list_id}/interest-categories`, for every list | full refresh | |
| `campaigns` | `GET /campaigns?since_create_time=<since>` | full refresh, incremental | `create_time` |
| `email_activity` | `GET /reports/{campaign_id}/email-activity`, for every campaign | full refresh | |
| `reports` | `GET /reports?since_send_time=<since>` | full refresh, incremental | `send_time` |
| `automations` | `GET /automations?since_create_time=<since>` | full refresh, incremental | `create_time` |

Records are Mailchimp's resources as sent, `_links` included. Primary keys: `id`; `list_members` and
`tags` `list_id` and `id` (a member's `id` is the hash of its email, the same in every list);
`email_activity` `campaign_id` and `email_id`. Pages are `offset`/`count=1000` and stop at `total_items`;
`tag-search` answers in one page.

The list children read every list on each sync. `email_activity` reads the campaigns that `campaigns`
reads, those created since `start_date`, and skips a campaign without a report (404, a campaign that was
never sent). An incremental read sends `start_date` on the first sync and the saved cursor afterwards;
records older than it are dropped by the engine too. A campaign is matched by when it was created, so a
later change to a campaign already read is picked up only by a full refresh.

`check` reads the first page of `lists`. A rejected key (401) and a key of another data center (403) fail
it with Mailchimp's `detail`.

## Differences from Airbyte's Mailchimp source

- The data center is a configuration key: a manifest template cannot cut the suffix off the API key.
- API key only. An OAuth access token would need a second auth method chosen by the configuration, which
  the manifest format does not have.
- `list_members`, `segments` and `email_activity` are full refresh. Their parents (lists, campaigns) have
  no update time, and the format keeps a child's cursor across all parents, not one per parent, so a
  per-list or per-campaign cursor (`since_last_changed`, `since`) is not expressible.
- `email_activity` records are one per email with its `activity` array, as the API sends them; Airbyte
  emits one record per action, a transformation a manifest does not do.
- `interests` is not read: it needs the list and the interest category, a second level of fan-out.
  `interest_categories` is read instead.
- `unsubscribes`, `segment_members` and the e-commerce streams are not read.

## Schemas

Each schema declares the fields Mailchimp documents for the resource, every field nullable except the
primary key, with `additionalProperties: true` on every object so a field Mailchimp adds passes through.
Only the creation and change times carry `format: date-time`; the other times can be the empty string.

## Tests

`manifest_test.go` runs `declarativetest.Run` (manifest lint, SDK conformance for full refresh and each
incremental stream, the 401 check, at least two pages per stream, one 429) over the synthetic fixtures in
`testdata/`, plus the 403 check, the filter value of the first and the resumed read, the records of every
list child per list, the skipped unsent campaign, open schemas and a kept unknown field. A request is
answered by `testdata/<path>.json`, a later page by `testdata/<path>/offset=<n>.json`. Everything in them
is synthetic.
