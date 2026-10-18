package main

import (
	"fmt"
	"regexp"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// namespace is the namespace of every stream.
const namespace = "google_ads"

const (
	streamCustomer                = "customer"
	streamCustomerClient          = "customer_client"
	streamCampaign                = "campaign"
	streamAdGroup                 = "ad_group"
	streamAdGroupAd               = "ad_group_ad"
	streamCampaignPerformance     = "campaign_performance_daily"
	streamAdGroupPerformance      = "ad_group_performance_daily"
	streamAdPerformance           = "ad_performance_daily"
	streamGeographicViewDaily     = "geographic_view_daily"
	cursorKey                     = "segments_date"
	metricsFields                 = "metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions, metrics.conversions_value, metrics.all_conversions, metrics.ctr, metrics.average_cpc"
	customQueryNamePatternLiteral = `^[a-z][a-z0-9_]*$`
)

var customQueryNamePattern = regexp.MustCompile(customQueryNamePatternLiteral)

// streamDef is one stream: a GAQL query, its record keys of the primary key,
// and whether it comes from custom_queries.
type streamDef struct {
	name   string
	query  gaqlQuery
	pk     []string
	custom bool
}

// incremental reports whether the stream selects segments.date, which makes
// it a daily stream read by date range with segments_date as its cursor.
func (d streamDef) incremental() bool { return d.query.selects(dateField) }

// fields returns the stream's fields with their types from types.
func (d streamDef) fields(types map[string]fieldType) []field {
	fs := make([]field, len(d.query.fields))
	for i, p := range d.query.fields {
		fs[i] = field{path: p, key: recordKey(p), typ: types[p]}
	}
	return fs
}

func (d streamDef) stream(types map[string]fieldType) synclet.Stream {
	props := map[synclet.PropertyName]synclet.PropertySpec{}
	for _, f := range d.fields(types) {
		p := f.typ.property()
		p.Description = f.path
		props[synclet.PropertyName(f.key)] = p
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
		s.DefaultCursorField = []string{cursorKey}
	}
	return s
}

func builtin(name, query string, pk ...string) streamDef {
	q, err := parseQuery(query)
	if err != nil {
		panic(fmt.Sprintf("built-in stream %s: %v", name, err))
	}
	return streamDef{name: name, query: q, pk: pk}
}

// builtinDefs lists the built-in streams in discover order. Every query
// selects customer.id, so records of different accounts stay apart.
var builtinDefs = []streamDef{
	builtin(streamCustomer,
		"SELECT customer.id, customer.descriptive_name, customer.currency_code, customer.time_zone, customer.manager, customer.test_account, customer.status FROM customer",
		"customer_id"),
	builtin(streamCustomerClient,
		"SELECT customer.id, customer_client.id, customer_client.client_customer, customer_client.descriptive_name, customer_client.currency_code, customer_client.time_zone, customer_client.manager, customer_client.test_account, customer_client.status, customer_client.level FROM customer_client",
		"customer_id", "customer_client_id"),
	builtin(streamCampaign,
		"SELECT customer.id, campaign.id, campaign.name, campaign.status, campaign.advertising_channel_type, campaign.bidding_strategy_type, campaign.campaign_budget FROM campaign",
		"campaign_id"),
	builtin(streamAdGroup,
		"SELECT customer.id, campaign.id, ad_group.id, ad_group.name, ad_group.status, ad_group.type, ad_group.cpc_bid_micros FROM ad_group",
		"ad_group_id"),
	builtin(streamAdGroupAd,
		"SELECT customer.id, campaign.id, ad_group.id, ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type, ad_group_ad.ad.final_urls, ad_group_ad.status FROM ad_group_ad",
		"ad_group_id", "ad_group_ad_ad_id"),
	builtin(streamCampaignPerformance,
		"SELECT customer.id, campaign.id, campaign.name, segments.date, "+metricsFields+" FROM campaign",
		"campaign_id", cursorKey),
	builtin(streamAdGroupPerformance,
		"SELECT customer.id, campaign.id, ad_group.id, ad_group.name, segments.date, "+metricsFields+" FROM ad_group",
		"ad_group_id", cursorKey),
	builtin(streamAdPerformance,
		"SELECT customer.id, campaign.id, ad_group.id, ad_group_ad.ad.id, segments.date, "+metricsFields+" FROM ad_group_ad",
		"ad_group_id", "ad_group_ad_ad_id", cursorKey),
	builtin(streamGeographicViewDaily,
		"SELECT customer.id, campaign.id, ad_group.id, geographic_view.country_criterion_id, geographic_view.location_type, segments.date, "+metricsFields+" FROM geographic_view",
		"ad_group_id", "geographic_view_country_criterion_id", "geographic_view_location_type", cursorKey),
}

func builtinByName(name string) (streamDef, bool) {
	for _, d := range builtinDefs {
		if d.name == name {
			return d, true
		}
	}
	return streamDef{}, false
}

// customDefs returns a stream for each custom query. A name must be a
// lowercase identifier, unique, and not the name of a built-in stream.
func customDefs(queries []CustomQuery) ([]streamDef, error) {
	var defs []streamDef
	seen := map[string]bool{}
	for _, cq := range queries {
		name := strings.TrimSpace(cq.Name)
		switch {
		case !customQueryNamePattern.MatchString(name):
			return nil, fmt.Errorf("custom query name %q must match %s", cq.Name, customQueryNamePatternLiteral)
		case seen[name]:
			return nil, fmt.Errorf("custom query name %q is used twice", name)
		}
		if _, ok := builtinByName(name); ok {
			return nil, fmt.Errorf("custom query name %q is the name of a built-in stream", name)
		}
		seen[name] = true
		q, err := parseQuery(cq.Query)
		if err != nil {
			return nil, fmt.Errorf("custom query %s: %w", name, err)
		}
		defs = append(defs, streamDef{name: name, query: q, custom: true})
	}
	return defs, nil
}
