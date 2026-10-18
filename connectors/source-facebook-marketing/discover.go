package main

import (
	"context"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// namespace is the namespace of every stream.
const namespace = "facebook_marketing"

// boolean is the JSON schema boolean type, which the SDK has no constant for.
const boolean synclet.PropType = "boolean"

const (
	streamAdInsights          = "ad_insights"
	streamAdInsightsAge       = "ad_insights_age"
	streamAdInsightsAgeGender = "ad_insights_age_gender"
	streamAdInsightsCountry   = "ad_insights_country"
	streamAdInsightsPlatform  = "ad_insights_platform"
	streamAdInsightsHourly    = "ad_insights_hourly"
	streamAccountSpendDaily   = "account_spend_daily"
	streamBusinessCredits     = "business_extended_credits"
	streamAdAccounts          = "ad_accounts"
	streamCampaigns           = "campaigns"
	streamAdSets              = "adsets"
	streamAds                 = "ads"
	streamAdCreatives         = "ad_creatives"
)

type streamKind int

const (
	kindInsights streamKind = iota
	kindAdAccounts
	kindEdge
	kindBusinessCredits
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
	// level and breakdowns are the insights request of a kindInsights stream.
	level      string
	breakdowns []string
	// edge is the ad account edge a kindEdge stream lists.
	edge string
}

func (d streamDef) incremental() bool { return d.kind == kindInsights }

// requestFields returns the fields a request asks the API for: every schema
// field but the breakdowns and date_start, which the API adds on its own.
func (d streamDef) requestFields() []string {
	var names []string
	for _, f := range d.fields {
		if f.name == "date_start" || slices.Contains(d.breakdowns, f.name) {
			continue
		}
		names = append(names, f.name)
	}
	return names
}

func (d streamDef) stream() synclet.Stream {
	props := map[synclet.PropertyName]synclet.PropertySpec{}
	for _, f := range d.fields {
		props[synclet.PropertyName(f.name)] = synclet.PropertySpec{PropertyType: typed(f.typ), Format: f.format}
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
		s.DefaultCursorField = []string{"date_start"}
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

func timeFields(names ...string) []field {
	out := make([]field, len(names))
	for i, n := range names {
		out[i] = field{name: n, typ: synclet.String, format: synclet.DateTime}
	}
	return out
}

func concat(parts ...[]field) []field { return slices.Concat(parts...) }

// adInsightsFields are the fields of ad_insights and its breakdown streams.
var adInsightsFields = concat(
	fields(synclet.String, "account_id", "campaign_id", "campaign_name", "adset_id", "adset_name", "ad_id", "ad_name"),
	fields(synclet.String, "date_start"),
	fields(synclet.Number, "spend"),
	fields(synclet.Integer, "clicks", "unique_clicks", "impressions", "unique_inline_link_clicks", "inline_link_clicks"),
	fields(synclet.Array, "cost_per_thruplay", "actions", "unique_outbound_clicks", "video_play_actions",
		"video_avg_time_watched_actions", "video_continuous_2_sec_watched_actions",
		"video_p25_watched_actions", "video_p50_watched_actions", "video_p75_watched_actions", "video_p100_watched_actions"),
)

func adInsights(name string, breakdowns ...string) streamDef {
	return streamDef{
		name:       name,
		kind:       kindInsights,
		fields:     concat(adInsightsFields, fields(synclet.String, breakdowns...)),
		pk:         append([]string{"account_id", "ad_id", "date_start"}, breakdowns...),
		level:      "ad",
		breakdowns: breakdowns,
	}
}

// creditAmountPrefixes are the CurrencyAmount fields of an extended credit;
// each becomes four flat fields.
var creditAmountPrefixes = []string{"balance", "credit_available", "online_max_balance", "max_balance", "allocated_amount"}

func creditFields() []field {
	var fs []field
	for _, p := range creditAmountPrefixes {
		fs = append(fs, field{name: p + "_amount", typ: synclet.Number})
		fs = append(fs, fields(synclet.String, p+"_currency", p+"_amount_in_hundredths", p+"_offsetted_amount")...)
	}
	fs = append(fs, fields(synclet.String, "credit_type", "legal_entity_name", "liable_biz_name", "owner_business_id",
		"owner_business_name", "send_bill_to_biz_name", "id", "date_created")...)
	return append(fs, fields(boolean, "is_automated_experience", "is_access_revoked")...)
}

func edge(name, edge string, fs ...[]field) streamDef {
	return streamDef{name: name, kind: kindEdge, edge: edge, fields: concat(fs...), pk: []string{"id"}}
}

// streamDefs lists every stream in discover order.
var streamDefs = []streamDef{
	adInsights(streamAdInsights),
	adInsights(streamAdInsightsAge, "age"),
	adInsights(streamAdInsightsAgeGender, "age", "gender"),
	adInsights(streamAdInsightsCountry, "country"),
	adInsights(streamAdInsightsPlatform, "publisher_platform", "platform_position"),
	adInsights(streamAdInsightsHourly, "hourly_stats_aggregated_by_advertiser_time_zone"),
	{
		name: streamAccountSpendDaily,
		kind: kindInsights,
		fields: concat(
			fields(synclet.String, "account_id", "account_currency"),
			fields(synclet.String, "date_start"),
			fields(synclet.Number, "spend"),
		),
		pk:    []string{"account_id", "date_start"},
		level: "account",
	},
	{name: streamBusinessCredits, kind: kindBusinessCredits, fields: creditFields(), pk: []string{"id"}},
	{
		name: streamAdAccounts,
		kind: kindAdAccounts,
		fields: concat(
			fields(synclet.String, "id", "account_id", "name", "currency", "timezone_name"),
			fields(synclet.Integer, "account_status"),
			timeFields("created_time"),
		),
		pk: []string{"id"},
	},
	edge(streamCampaigns, "campaigns",
		fields(synclet.String, "id", "account_id", "name", "status", "effective_status", "objective"),
		timeFields("created_time", "updated_time")),
	edge(streamAdSets, "adsets",
		fields(synclet.String, "id", "account_id", "campaign_id", "name", "status", "effective_status"),
		timeFields("created_time", "updated_time")),
	edge(streamAds, "ads",
		fields(synclet.String, "id", "account_id", "campaign_id", "adset_id", "name", "status", "effective_status"),
		timeFields("created_time", "updated_time")),
	edge(streamAdCreatives, "adcreatives",
		fields(synclet.String, "id", "account_id", "name", "title", "body", "object_type", "thumbnail_url")),
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
