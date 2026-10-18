package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// boolean is the JSON schema type the SDK has no constant for.
const boolean synclet.PropType = "boolean"

// streamKind is how a stream is requested.
type streamKind int

const (
	// cursorExport is a cursor-based incremental export: start_time, then
	// cursor=after_cursor until end_of_stream.
	cursorExport streamKind = iota
	// timeExport is a time-based incremental export: start_time, then the
	// response's end_time as the next start_time.
	timeExport
	// paged is a list with cursor pagination: page[size], then
	// page[after]=meta.after_cursor while meta.has_more.
	paged
)

// streamDef is one stream: the API path, the key of its records in the
// response body and its schema. cursor is empty for full refresh only
// streams.
type streamDef struct {
	name   string
	path   string
	key    string
	kind   streamKind
	cursor string
	props  map[synclet.PropertyName]synclet.PropertySpec
}

func nullable(t synclet.PropType) synclet.PropertySpec {
	return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}}
}

func props(types map[synclet.PropType][]synclet.PropertyName, timestamps ...synclet.PropertyName) map[synclet.PropertyName]synclet.PropertySpec {
	out := map[synclet.PropertyName]synclet.PropertySpec{"id": {PropertyType: typed(synclet.Integer)}}
	for t, names := range types {
		for _, n := range names {
			out[n] = nullable(t)
		}
	}
	for _, n := range timestamps {
		p := nullable(synclet.String)
		p.Format = synclet.DateTime
		out[n] = p
	}
	return out
}

var streams = []streamDef{
	{
		name: "tickets", path: "incremental/tickets/cursor.json", key: "tickets", kind: cursorExport, cursor: "updated_at",
		props: props(map[synclet.PropType][]synclet.PropertyName{
			synclet.String: {"url", "external_id", "type", "subject", "raw_subject", "description", "priority", "status", "recipient"},
			synclet.Integer: {
				"requester_id", "submitter_id", "assignee_id", "organization_id", "group_id", "forum_topic_id",
				"problem_id", "ticket_form_id", "brand_id", "custom_status_id", "generated_timestamp",
			},
			boolean: {"has_incidents", "is_public", "allow_channelback", "allow_attachments", "from_messaging_channel"},
			synclet.Array: {
				"collaborator_ids", "follower_ids", "email_cc_ids", "followup_ids", "tags", "custom_fields", "fields",
				"sharing_agreement_ids",
			},
			synclet.Object: {"via", "satisfaction_rating"},
		}, "created_at", "updated_at", "due_at"),
	},
	{
		name: "users", path: "incremental/users/cursor.json", key: "users", kind: cursorExport, cursor: "updated_at",
		props: props(map[synclet.PropType][]synclet.PropertyName{
			synclet.String: {
				"url", "name", "email", "time_zone", "iana_time_zone", "phone", "locale", "role", "external_id", "alias",
				"signature", "details", "notes", "ticket_restriction",
			},
			synclet.Integer: {"locale_id", "organization_id", "role_type", "custom_role_id", "default_group_id"},
			boolean: {
				"shared_phone_number", "verified", "active", "shared", "shared_agent", "two_factor_auth_enabled",
				"moderator", "only_private_comments", "restricted_agent", "suspended", "report_csv",
			},
			synclet.Array:  {"tags"},
			synclet.Object: {"photo", "user_fields"},
		}, "created_at", "updated_at", "last_login_at"),
	},
	{
		name: "ticket_events", path: "incremental/ticket_events.json", key: "ticket_events", kind: timeExport, cursor: "created_at",
		props: props(map[synclet.PropType][]synclet.PropertyName{
			synclet.String:  {"via", "event_type"},
			synclet.Integer: {"ticket_id", "updater_id", "timestamp"},
			synclet.Array:   {"child_events", "merged_ticket_ids"},
			synclet.Object:  {"system"},
		}, "created_at"),
	},
	{
		name: "organizations", path: "organizations.json", key: "organizations", kind: paged,
		props: props(map[synclet.PropType][]synclet.PropertyName{
			synclet.String:  {"url", "name", "external_id", "details", "notes"},
			synclet.Integer: {"group_id"},
			boolean:         {"shared_tickets", "shared_comments"},
			synclet.Array:   {"domain_names", "tags"},
			synclet.Object:  {"organization_fields"},
		}, "created_at", "updated_at"),
	},
	{
		name: "groups", path: "groups.json", key: "groups", kind: paged,
		props: props(map[synclet.PropType][]synclet.PropertyName{
			synclet.String: {"url", "name", "description"},
			boolean:        {"default", "deleted", "is_public"},
		}, "created_at", "updated_at"),
	},
	{
		name: "ticket_fields", path: "ticket_fields.json", key: "ticket_fields", kind: paged,
		props: props(map[synclet.PropType][]synclet.PropertyName{
			synclet.String: {
				"url", "type", "title", "raw_title", "description", "raw_description", "regexp_for_validation",
				"title_in_portal", "raw_title_in_portal", "tag", "key", "agent_description", "relationship_target_type",
			},
			synclet.Integer: {"position", "sub_type_id"},
			boolean: {
				"active", "required", "collapsed_for_agents", "visible_in_portal", "editable_in_portal",
				"required_in_portal", "removable",
			},
			synclet.Array:  {"system_field_options", "custom_field_options", "custom_statuses"},
			synclet.Object: {"relationship_filter"},
		}, "created_at", "updated_at"),
	},
	{
		name: "satisfaction_ratings", path: "satisfaction_ratings.json", key: "satisfaction_ratings", kind: paged,
		props: props(map[synclet.PropType][]synclet.PropertyName{
			synclet.String:  {"url", "score", "comment", "reason"},
			synclet.Integer: {"assignee_id", "group_id", "requester_id", "ticket_id", "reason_code", "reason_id"},
		}, "created_at", "updated_at"),
	},
}

func streamByName(name string) (streamDef, bool) {
	for _, d := range streams {
		if d.name == name {
			return d, true
		}
	}
	return streamDef{}, false
}

// Discover returns the seven streams; every account has the same routes, so
// nothing is requested.
func (Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	catalog := &synclet.Catalog{}
	for _, d := range streams {
		s := synclet.Stream{
			Name:                    d.name,
			JSONSchema:              synclet.ObjectSchema(d.props),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
			SourceDefinedPrimaryKey: [][]string{{"id"}},
		}
		if d.cursor != "" {
			s.SupportedSyncModes = append(s.SupportedSyncModes, synclet.SyncModeIncremental)
			s.SourceDefinedCursor = true
			s.DefaultCursorField = []string{d.cursor}
		}
		catalog.Streams = append(catalog.Streams, s)
	}
	return catalog, nil
}
