package main

import (
	"context"
	"slices"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// namespace is the namespace of every stream.
const namespace = "tiktok_marketing"

const (
	streamAdvertisers            = "advertisers"
	streamCampaigns              = "campaigns"
	streamAdGroups               = "ad_groups"
	streamAds                    = "ads"
	streamAdsReportDaily         = "ads_report_daily"
	streamAdGroupsReportDaily    = "ad_groups_report_daily"
	streamCampaignsReportDaily   = "campaigns_report_daily"
	streamAdvertisersReportDaily = "advertisers_report_daily"
)

// cursorField is the cursor of every report stream.
const cursorField = "stat_time_day"

type streamKind int

const (
	kindAdvertisers streamKind = iota
	kindEntity
	kindReport
)

// field is one top-level field of a stream's schema.
type field struct {
	name   string
	typ    []synclet.PropType
	format synclet.FormatType
}

// streamDef describes one stream and how it is read.
type streamDef struct {
	name   string
	kind   streamKind
	fields []field
	pk     []string
	// path is the get endpoint of a kindEntity stream.
	path string
	// dataLevel and dimension are the report request of a kindReport stream:
	// its data_level and the id dimension next to stat_time_day.
	dataLevel string
	dimension string
}

func (d streamDef) incremental() bool { return d.kind == kindReport }

// requestFields returns the schema field names a get request asks for.
func (d streamDef) requestFields() []string {
	names := make([]string, len(d.fields))
	for i, f := range d.fields {
		names[i] = f.name
	}
	return names
}

func (d streamDef) stream() synclet.Stream {
	props := map[synclet.PropertyName]synclet.PropertySpec{}
	for _, f := range d.fields {
		props[synclet.PropertyName(f.name)] = synclet.PropertySpec{PropertyType: typed(f.typ...), Format: f.format}
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
		out[i] = field{name: n, typ: []synclet.PropType{typ}}
	}
	return out
}

func concat(parts ...[]field) []field { return slices.Concat(parts...) }

// reportMetrics are the metrics every report stream asks for.
var reportMetrics = []string{
	"spend", "impressions", "clicks", "ctr", "cpc", "cpm", "reach", "frequency",
	"conversion", "cost_per_conversion", "conversion_rate",
}

func report(name, dataLevel, dimension string) streamDef {
	fs := fields(synclet.String, "advertiser_id")
	if dimension != "advertiser_id" {
		fs = append(fs, fields(synclet.String, dimension)...)
	}
	fs = append(fs, field{name: cursorField, typ: []synclet.PropType{synclet.String}, format: synclet.Date})
	for _, m := range reportMetrics {
		fs = append(fs, field{name: m, typ: []synclet.PropType{synclet.Null, synclet.Number}})
	}
	return streamDef{
		name: name, kind: kindReport, fields: fs,
		pk:        []string{dimension, cursorField},
		dataLevel: dataLevel, dimension: dimension,
	}
}

func entity(name, path, pk string, fs ...[]field) streamDef {
	return streamDef{name: name, kind: kindEntity, path: path, fields: concat(fs...), pk: []string{pk}}
}

// streamDefs lists every stream in discover order.
var streamDefs = []streamDef{
	{
		name: streamAdvertisers,
		kind: kindAdvertisers,
		fields: concat(
			fields(synclet.String, "advertiser_id", "name", "company", "status", "role", "currency", "timezone",
				"display_timezone", "country", "industry", "description"),
			fields(synclet.Number, "balance"),
			fields(synclet.Integer, "create_time"),
		),
		pk: []string{"advertiser_id"},
	},
	entity(streamCampaigns, "campaign/get/", "campaign_id",
		fields(synclet.String, "campaign_id", "advertiser_id", "campaign_name", "campaign_type", "objective_type",
			"budget_mode", "operation_status", "secondary_status", "create_time", "modify_time"),
		fields(synclet.Number, "budget")),
	entity(streamAdGroups, "adgroup/get/", "adgroup_id",
		fields(synclet.String, "adgroup_id", "advertiser_id", "campaign_id", "adgroup_name", "placement_type",
			"budget_mode", "bid_type", "optimization_goal", "billing_event", "schedule_type", "schedule_start_time",
			"schedule_end_time", "operation_status", "secondary_status", "create_time", "modify_time"),
		fields(synclet.Number, "budget", "bid_price")),
	entity(streamAds, "ad/get/", "ad_id",
		fields(synclet.String, "ad_id", "advertiser_id", "campaign_id", "adgroup_id", "ad_name", "ad_format",
			"ad_text", "call_to_action", "landing_page_url", "operation_status", "secondary_status",
			"create_time", "modify_time")),
	report(streamAdsReportDaily, "AUCTION_AD", "ad_id"),
	report(streamAdGroupsReportDaily, "AUCTION_ADGROUP", "adgroup_id"),
	report(streamCampaignsReportDaily, "AUCTION_CAMPAIGN", "campaign_id"),
	report(streamAdvertisersReportDaily, "AUCTION_ADVERTISER", "advertiser_id"),
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
// advertiser, so nothing is requested.
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
