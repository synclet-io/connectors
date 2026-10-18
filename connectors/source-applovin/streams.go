package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	namespace              = "applovin"
	streamAdvertiserReport = "advertiser_report"
	streamMaxRevenueReport = "max_revenue_report"
	cursorField            = "day"
	hourColumn             = "hour"
	typeString             = "string"
	typeNumber             = "number"
)

// column is one column of a report: its API name, which is also its field
// name, its JSON schema type and its description.
type column struct {
	name, typ, desc string
}

// streamDef is one report stream: the API path and report_type it is read
// from, its columns and its primary key.
type streamDef struct {
	name string
	// path is the API path below the base URL.
	path string
	// reportType is the report_type query parameter; empty sends none.
	reportType string
	// typeName names the report type in errors and logs.
	typeName string
	columns  []column
	pk       []string
}

var streamDefs = []streamDef{
	{
		name: streamAdvertiserReport, path: "report", reportType: "advertiser", typeName: "advertiser",
		columns: advertiserColumns(),
		pk:      []string{"day", "campaign_id_external", "country", "platform", "device_type"},
	},
	{
		name: streamMaxRevenueReport, path: "maxReport", typeName: "MAX revenue",
		columns: maxRevenueColumns,
		pk:      []string{"day", "package_name", "platform", "country", "device_type", "ad_format", "max_ad_unit_id", "network"},
	},
}

func streamDefByName(name string) (streamDef, bool) {
	i := slices.IndexFunc(streamDefs, func(d streamDef) bool { return d.name == name })
	if i < 0 {
		return streamDef{}, false
	}
	return streamDefs[i], true
}

// Cohort spans of the advertiser report: each cohort metric exists once per
// span of its list.
var (
	allSpans        = []string{"0d", "1d", "2d", "3d", "7d", "14d", "28d", "30d", "90d", "1y"}
	spansWithout28d = []string{"0d", "1d", "2d", "3d", "7d", "14d", "30d", "90d", "1y"}
	retentionSpans  = []string{"1d", "3d", "7d", "14d", "28d"}
)

var advertiserBaseColumns = []column{
	{"day", typeString, "Day the row covers, YYYY-MM-DD in UTC."},
	{"hour", typeString, "Hour of the day the row covers; AppLovin returns hours for the last 30 days only."},
	{"campaign", typeString, "Name of the campaign."},
	{"campaign_id_external", typeString, "Campaign identifier that the {CAMPAIGN_ID} tracking macro expands to."},
	{"campaign_package_name", typeString, "Bundle identifier of the app the campaign promotes."},
	{"campaign_store_id", typeString, "Store ID of the promoted app: the App Store number on iOS, the package name elsewhere."},
	{"campaign_type", typeString, "What the campaign optimizes for, such as CPP, CPE or one of the ROAS goals."},
	{"campaign_ad_type", typeString, "ua when the campaign acquires new users, rt when it retargets existing ones."},
	{"campaign_bid_goal", typeNumber, "Bid goal in US dollars of a CPP or CPE campaign."},
	{"campaign_roas_goal", typeNumber, "Return-on-ad-spend goal, in percent, of a ROAS campaign."},
	{"platform", typeString, "Operating system of the device: android, fireos, ios or tvos."},
	{"device_type", typeString, "Device class: phone, tablet or other."},
	{"country", typeString, "Country of the user as an ISO 3166-1 alpha-2 code."},
	{"ad", typeString, "Name of the ad."},
	{"ad_creative_type", typeString, "Kind of creative shown, such as VIDEO or PLAYABLE."},
	{"ad_type", typeString, "Format of the ad, such as GRAPHIC, VIDEO or REWARD."},
	{"creative_set", typeString, "Name of the creative set."},
	{"creative_set_id", typeString, "Identifier of the creative set."},
	{"custom_page_id", typeString, "Custom product page (iOS) or custom store listing (Android) the creative set links to."},
	{"external_placement_id", typeString, "Encrypted identifier of the publisher app the ad ran in."},
	{"bidding_and_billing_method", typeString, "How the campaign bids and is billed."},
	{"optimization_day_target", typeString, "Day after install that the campaign optimizes for."},
	{"placement_type", typeString, "Placement the ad ran in, such as BANNER, INTER or REWARDED_INTER."},
	{"size", typeString, "Size of the ad unit, such as BANNER, MREC or INTER."},
	{"target_event", typeString, "In-app event a CPE campaign optimizes for."},
	{"traffic_source", typeString, "Where the impression was bought: AppLovin or the name of an exchange."},
	{"impressions", typeNumber, "Times the ads were shown."},
	{"clicks", typeNumber, "Clicks on the ads."},
	{"ctr", typeNumber, "Clicks divided by impressions."},
	{"conversions", typeNumber, "Installs attributed to the ads."},
	{"conversion_rate", typeNumber, "Installs divided by impressions."},
	{"cost", typeNumber, "Spend in US dollars."},
	{"average_cpa", typeNumber, "Spend divided by installs."},
	{"average_cpc", typeNumber, "Spend divided by clicks."},
	{"sales", typeNumber, "Purchase events attributed to the ads."},
	{"first_purchase", typeNumber, "Users whose first purchase is attributed to the ads."},
	{"target_event_count", typeNumber, "Distinct users who completed the target event."},
}

// cohortMetric is a metric the advertiser report has once per span, as
// <prefix>_<span>.
type cohortMetric struct {
	prefix, desc string
	spans        []string
}

var cohortMetrics = []cohortMetric{
	{"ad_rev", "Ad revenue of the installed users", allSpans},
	{"ad_roas", "Ad revenue of the installed users divided by spend", allSpans},
	{"iap_rev", "In-app purchase revenue of the installed users", allSpans},
	{"iap_roas", "In-app purchase revenue of the installed users divided by spend", allSpans},
	{"roas", "Ad and in-app purchase revenue of the installed users divided by spend", allSpans},
	{"total_rev", "Ad and in-app purchase revenue of the installed users", allSpans},
	{"cost_per_target_event", "Spend per target event of the installed users", spansWithout28d},
	{"cpp", "Spend per purchase of the installed users", spansWithout28d},
	{"sales", "Purchase events of the installed users", spansWithout28d},
	{"target_event_count", "Distinct installed users who completed the target event", spansWithout28d},
	{"unique_purchasers", "Distinct installed users who made a purchase", spansWithout28d},
	{"ret", "Share of installed users who opened the app again", retentionSpans},
}

// spanText says which days after install a span covers.
func spanText(prefix, span string) string {
	if prefix == "ret" {
		return "on day " + span[:len(span)-1] + " after install"
	}
	switch span {
	case "0d":
		return "on the install day"
	case "1d":
		return "within one day of install"
	case "1y":
		return "within one year of install"
	}
	return "within " + span[:len(span)-1] + " days of install"
}

// cohortColumns returns the cohort columns generated from cohortMetrics, in
// metric order and span order.
func cohortColumns() []column {
	var cols []column
	for _, m := range cohortMetrics {
		for _, span := range m.spans {
			cols = append(cols, column{
				name: m.prefix + "_" + span,
				typ:  typeNumber,
				desc: fmt.Sprintf("%s, %s.", m.desc, spanText(m.prefix, span)),
			})
		}
	}
	return cols
}

func advertiserColumns() []column {
	return append(slices.Clone(advertiserBaseColumns), cohortColumns()...)
}

var maxRevenueColumns = []column{
	{"day", typeString, "Day the row covers, YYYY-MM-DD in UTC."},
	{"application", typeString, "Name of the app."},
	{"package_name", typeString, "Package name (Android) or bundle ID (iOS) of the app."},
	{"store_id", typeString, "Store ID of the app: the App Store number on iOS, the package name elsewhere."},
	{"platform", typeString, "Operating system of the device, such as android or ios."},
	{"country", typeString, "Country of the user as an ISO 3166-1 alpha-2 code."},
	{"device_type", typeString, "Device class: phone, tablet or other."},
	{"ad_format", typeString, "Ad format of the ad unit, such as BANNER, INTER or REWARD."},
	{"max_ad_unit_id", typeString, "Identifier of the MAX ad unit."},
	{"max_ad_unit", typeString, "Name of the MAX ad unit."},
	{"max_ad_unit_test", typeString, "Name of the ad unit test group the row belongs to, if any."},
	{"network", typeString, "Ad network that served the impressions."},
	{"custom_network_name", typeString, "Name of the custom network, for rows of a custom network."},
	{"impressions", typeNumber, "Impressions the network served."},
	{"estimated_revenue", typeNumber, "Estimated revenue in US dollars."},
	{"ecpm", typeNumber, "Estimated revenue per thousand impressions, in US dollars."},
	{"attempts", typeNumber, "Ad requests MAX sent to the network."},
	{"responses", typeNumber, "Ad requests the network filled."},
	{"fill_rate", typeNumber, "Responses divided by attempts."},
}

// stream returns the catalog entry of d: the schema lists every column with a
// single type and no root type, and day is the source-defined cursor.
func (d streamDef) stream() synclet.Stream {
	props := make(map[string]any, len(d.columns))
	for _, c := range d.columns {
		props[c.name] = map[string]any{"type": []string{c.typ}, "description": c.desc}
	}
	raw, err := json.Marshal(map[string]any{"properties": props})
	if err != nil {
		panic(fmt.Sprintf("stream %s: schema: %v", d.name, err))
	}
	pk := make([][]string, len(d.pk))
	for i, f := range d.pk {
		pk[i] = []string{f}
	}
	return synclet.Stream{
		Name:                    d.name,
		Namespace:               namespace,
		JSONSchema:              raw,
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
		SourceDefinedCursor:     true,
		DefaultCursorField:      []string{cursorField},
		SourceDefinedPrimaryKey: pk,
	}
}

// requestColumns returns the columns to request for a configured stream
// whose schema lists selected, in the order of d.columns: the selected
// columns plus day and the primary key. An empty selection requests every
// column.
func (d streamDef) requestColumns(selected map[string]bool) []string {
	var cols []string
	for _, c := range d.columns {
		if len(selected) == 0 || selected[c.name] || c.name == cursorField || slices.Contains(d.pk, c.name) {
			cols = append(cols, c.name)
		}
	}
	return cols
}

// record converts one API row into a record. AppLovin sends numbers as
// quoted strings: a number column becomes a JSON number, a null or empty
// value leaves the field out, and a value that is not a number fails with the
// column named. Fields the stream does not declare are left out.
func (d streamDef) record(row map[string]any) (map[string]any, error) {
	rec := make(map[string]any, len(row))
	for _, c := range d.columns {
		v, ok := row[c.name]
		if !ok || v == nil {
			continue
		}
		switch c.typ {
		case typeNumber:
			n, ok, err := toNumber(v)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", c.name, err)
			}
			if ok {
				rec[c.name] = n
			}
		default:
			switch s := v.(type) {
			case string:
				rec[c.name] = s
			case json.Number:
				rec[c.name] = s.String()
			default:
				return nil, fmt.Errorf("column %s: %v is not a string", c.name, v)
			}
		}
	}
	return rec, nil
}

// toNumber returns v as a JSON number; ok is false for an empty string.
func toNumber(v any) (json.Number, bool, error) {
	switch n := v.(type) {
	case json.Number:
		return n, true, nil
	case string:
		if n == "" {
			return "", false, nil
		}
		if _, err := strconv.ParseFloat(n, 64); err != nil || !json.Valid([]byte(n)) {
			return "", false, fmt.Errorf("%q is not a number", n)
		}
		return json.Number(n), true, nil
	}
	return "", false, fmt.Errorf("%v is not a number", v)
}
