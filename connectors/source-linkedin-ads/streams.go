package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// namespace is the namespace of every stream.
const namespace = "linkedin_ads"

const (
	streamAdAccounts        = "ad_accounts"
	streamCampaignGroups    = "campaign_groups"
	streamCampaigns         = "campaigns"
	streamCreatives         = "creatives"
	streamAnalyticsCampaign = "ad_analytics_by_campaign"
	streamAnalyticsCreative = "ad_analytics_by_creative"
)

// cursorField is the cursor of every analytics stream.
const cursorField = "date"

// boolean is the JSON schema type the SDK has no constant for.
const boolean synclet.PropType = "boolean"

type streamKind int

const (
	kindAccounts streamKind = iota
	kindEntity
	kindAnalytics
)

// field is one top-level field of a stream's schema.
type field struct {
	name   string
	typ    synclet.PropType
	format synclet.FormatType
}

// streamDef describes one stream and how it is read.
type streamDef struct {
	name   string
	kind   streamKind
	fields []field
	pk     []string
	// resource, finder and search are the request of a kindEntity stream:
	// adAccounts/<id>/<resource>?q=<finder>&search=<search>.
	resource string
	finder   string
	search   string
	// pivot and pivotField are the analytics pivot of a kindAnalytics stream
	// and the record field that holds the id of its pivot URN.
	pivot      string
	pivotField string
}

func (d streamDef) incremental() bool { return d.kind == kindAnalytics }

func (d streamDef) stream() synclet.Stream {
	props := map[synclet.PropertyName]synclet.PropertySpec{}
	for _, f := range d.fields {
		types := []synclet.PropType{synclet.Null, f.typ}
		for _, k := range d.pk {
			if k == f.name {
				types = types[1:]
			}
		}
		props[synclet.PropertyName(f.name)] = synclet.PropertySpec{PropertyType: typed(types...), Format: f.format}
	}
	s := synclet.Stream{
		Name:                    d.name,
		Namespace:               namespace,
		JSONSchema:              synclet.ObjectSchema(props),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{},
	}
	for _, k := range d.pk {
		s.SourceDefinedPrimaryKey = append(s.SourceDefinedPrimaryKey, []string{k})
	}
	if d.incremental() {
		s.SupportedSyncModes = append(s.SupportedSyncModes, synclet.SyncModeIncremental)
		s.SourceDefinedCursor = true
		s.DefaultCursorField = []string{cursorField}
	}
	return s
}

func fields(typ synclet.PropType, names ...string) []field {
	out := make([]field, len(names))
	for i, n := range names {
		out[i] = field{name: n, typ: typ}
	}
	return out
}

func concat(parts ...[]field) []field {
	var out []field
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// statusSearch is a search finder's criteria that match every status in
// statuses; without it LinkedIn leaves some statuses out.
func statusSearch(statuses ...string) string {
	return "(status:(values:" + restliList(statuses...) + "))"
}

// analyticsMetrics are the metrics every analytics stream asks for. With
// dateRange and pivotValues they stay within LinkedIn's limit of 20 fields.
var analyticsMetrics = []string{
	"impressions", "clicks", "costInLocalCurrency", "costInUsd", "landingPageClicks",
	"externalWebsiteConversions", "externalWebsitePostClickConversions", "externalWebsitePostViewConversions",
	"conversionValueInLocalCurrency", "oneClickLeads", "totalEngagements", "likes", "comments", "shares",
	"follows", "videoViews", "videoCompletions",
}

func analytics(name, pivot, pivotField string) streamDef {
	fs := concat(
		fields(synclet.String, "account_id", pivotField, "pivot_value"),
		[]field{{name: cursorField, typ: synclet.String, format: synclet.Date}},
		fields(synclet.Number, analyticsMetrics...),
	)
	return streamDef{
		name: name, kind: kindAnalytics, fields: fs,
		pk: []string{pivotField, cursorField}, pivot: pivot, pivotField: pivotField,
	}
}

func entity(name, resource, finder, search string, fs ...[]field) streamDef {
	return streamDef{name: name, kind: kindEntity, resource: resource, finder: finder, search: search, fields: concat(fs...), pk: []string{"id"}}
}

// streamDefs lists every stream in discover order.
var streamDefs = []streamDef{
	{
		name: streamAdAccounts,
		kind: kindAccounts,
		fields: concat(
			fields(synclet.Integer, "id"),
			fields(synclet.String, "name", "currency", "status", "type", "reference"),
			fields(boolean, "test", "notifiedOnCampaignOptimization", "notifiedOnCreativeApproval",
				"notifiedOnCreativeRejection", "notifiedOnEndOfCampaign", "notifiedOnNewFeaturesEnabled"),
			fields(synclet.Array, "servingStatuses"),
			fields(synclet.Object, "version", "changeAuditStamps"),
		),
		pk: []string{"id"},
	},
	entity(streamCampaignGroups, "adCampaignGroups", "search",
		statusSearch("ACTIVE", "PAUSED", "ARCHIVED", "CANCELED", "DRAFT", "PENDING_DELETION", "REMOVED"),
		fields(synclet.Integer, "id"),
		fields(synclet.String, "account", "name", "status", "objectiveType"),
		fields(boolean, "test", "backfilled"),
		fields(synclet.Array, "servingStatuses"),
		fields(synclet.Object, "runSchedule", "totalBudget", "dailyBudget", "changeAuditStamps")),
	entity(streamCampaigns, "adCampaigns", "search",
		statusSearch("ACTIVE", "PAUSED", "ARCHIVED", "COMPLETED", "CANCELED", "DRAFT", "PENDING_DELETION", "REMOVED"),
		fields(synclet.Integer, "id"),
		fields(synclet.String, "account", "campaignGroup", "name", "status", "type", "costType", "objectiveType",
			"format", "creativeSelection", "optimizationTargetType", "pacingStrategy", "associatedEntity"),
		fields(boolean, "test", "offsiteDeliveryEnabled", "audienceExpansionEnabled", "storyDeliveryEnabled"),
		fields(synclet.Array, "servingStatuses"),
		fields(synclet.Object, "locale", "dailyBudget", "totalBudget", "unitCost", "runSchedule", "targetingCriteria",
			"offsitePreferences", "version", "changeAuditStamps")),
	entity(streamCreatives, "creatives", "criteria", "",
		fields(synclet.String, "id", "account", "campaign", "intendedStatus", "createdBy", "lastModifiedBy", "name"),
		fields(synclet.Integer, "createdAt", "lastModifiedAt"),
		fields(boolean, "isServing", "isTest"),
		fields(synclet.Array, "servingHoldReasons"),
		fields(synclet.Object, "content", "review", "leadgenCallToAction", "inlineContent")),
	analytics(streamAnalyticsCampaign, "CAMPAIGN", "campaign_id"),
	analytics(streamAnalyticsCreative, "CREATIVE", "creative_id"),
}

func streamByName(name string) (streamDef, bool) {
	for _, d := range streamDefs {
		if d.name == name {
			return d, true
		}
	}
	return streamDef{}, false
}

// Discover returns every stream; the API has the same streams for every
// account, so nothing is requested.
func (Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	cat := &synclet.Catalog{}
	for _, d := range streamDefs {
		cat.Streams = append(cat.Streams, d.stream())
	}
	return cat, nil
}
