# source-slack

[Slack](https://slack.com) source for Synclet, a manifest connector run by the
[source-declarative](../source-declarative) engine: [`manifest.yaml`](manifest.yaml) plus one JSON schema per
stream, no Go. Reads the channels a bot is a member of, their messages (incrementally) and the workspace's
users through the [Slack Web API](https://api.slack.com/web).

```bash
task test CONNECTOR=source-slack                      # no network, no credentials
go run ./cmd/manifest-lint ../source-slack            # from connectors/source-declarative
task build CONNECTOR=source-slack
```

## Configuration

| Key | Required | Meaning |
|---|---|---|
| `bot_token` | yes | Bot user OAuth token (`xoxb-...`), sent only as `Authorization: Bearer <token>`. Scopes: `channels:read`, `channels:history`, `users:read`; `groups:read` and `groups:history` for private channels, `im:*` and `mpim:*` for direct messages. |
| `start_date` | yes | RFC 3339 time; messages posted before it are not read. |
| `channel_types` | no | Comma-separated conversation types, of `public_channel`, `private_channel`, `mpim` and `im`. Empty: Slack's default, `public_channel`. |

```json
{"bot_token": "xoxb-<token>", "start_date": "2026-01-01T00:00:00Z", "channel_types": "public_channel,private_channel"}
```

`check` requests the first page of `users.list`.

## Streams

| Stream | Method | Sync modes | Primary key | Cursor |
|---|---|---|---|---|
| `channels` | `users.conversations?types=&limit=200` | full refresh, incremental | `id` | `_read_at` |
| `channel_messages` | `conversations.history?channel=&oldest=&inclusive=true&limit=200` | full refresh, incremental | `channel_id`, `ts` | `ts` |
| `users` | `users.list?limit=200` | full refresh | `id` | |

Every list is paged by `response_metadata.next_cursor`. `channel_messages` runs once per channel listed by
`channels` and every message carries `channel_id`. Threads appear as their parent message (with
`thread_ts`, `reply_count`, `latest_reply`); replies are not read.

`channels` lists the conversations the bot is a member of (`users.conversations`), not every conversation of
the workspace (`conversations.list`): Slack refuses `conversations.history` of a channel the bot has not
joined, and the connector cannot join channels. Invite the bot to every channel to read.

Records are passed through as Slack sends them, with one addition: every `channels` record carries
`_read_at`, the Unix time (seconds, as a string) of the read that listed it. It is the cursor of `channels`
and the reason `channel_messages` can be incremental: the engine runs a child stream incrementally only
below an incremental parent, and no field of a channel moves when a message is posted. Every incremental
read therefore lists every channel and re-emits it (deduplicated by `id`).

## Incremental sync

`channel_messages` keeps one cursor across all channels, the newest `ts` it has read:

```json
{"ts": "1792000000.000300"}
```

The next read sends `oldest=<cursor minus 7 days>` to every channel and drops older messages. The 7-day
lookback re-reads a week of every channel, so edits, reactions and new replies on recent messages reach the
destination (deduplicated by `channel_id`, `ts`); older messages keep the state they had when last read. The
lookback is fixed by the manifest. The pass over the channels starts at the older of the `channels` and the
`channel_messages` cursors, so select `channels` incrementally too (or leave it out of the catalog); with
`channels` in full refresh every read requests messages from `start_date` and filters them locally.

## Errors and rate limits

- Slack answers most errors, `invalid_auth` and `missing_scope` among them, with HTTP 200 and
  `{"ok": false, "error": "..."}`. `check` succeeds on the status alone, so a revoked token passes `check`;
  the read then fails with `records path $.members is not in the response` (or `$.channels`, `$.messages`)
  and the request, without Slack's `error` text. The rare 401 and 403 responses fail as configuration errors
  with `error`.
- 429 waits for `Retry-After` (30s when absent), up to ten times. Slack's per-method rate tiers are met by
  waiting, not by pacing. Use a bot of an app installed in your own workspace: Slack limits
  `conversations.history` far more tightly (one request a minute, 15 messages per page) for apps distributed
  outside the Slack Marketplace, which would make large channels very slow to read.
- 5xx responses and network errors are retried three times with a doubling delay from 1s.

Not read in this version: thread replies (`conversations.replies` needs a parent message, a second level of
fan-out), channel members (`conversations.members` returns an array of user ids, not of objects), files,
and a channel filter by name.

## Tests

`manifest_test.go` runs the engine against hand-written synthetic fixtures in `testdata/` (three channels on
two pages, one channel's messages on two pages, a channel without messages, users on two pages) without
network or credentials: the SDK conformance suite for full refresh and for each incremental stream,
`check` with 401 and 403, a read failing on a 200 `ok: false` body, a one-time 429, messages per channel,
the `types` parameter, and the `oldest` sent on a first and on a resumed read.
