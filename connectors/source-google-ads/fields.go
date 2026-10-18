package main

import (
	"encoding/json"
	"strconv"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// boolean is the JSON schema boolean type, which the SDK has no constant for.
const boolean synclet.PropType = "boolean"

// fieldType is the GoogleAdsField data type of a field and whether it is
// repeated.
type fieldType struct {
	dataType string
	repeated bool
}

// field is one selected field of a stream: its GAQL path, its record key and
// its type.
type field struct {
	path string
	key  string
	typ  fieldType
}

func recordKey(path string) string { return strings.ReplaceAll(path, ".", "_") }

var (
	int64Type    = fieldType{dataType: "INT64"}
	doubleType   = fieldType{dataType: "DOUBLE"}
	stringType   = fieldType{dataType: "STRING"}
	enumType     = fieldType{dataType: "ENUM"}
	boolType     = fieldType{dataType: "BOOLEAN"}
	dateType     = fieldType{dataType: "DATE"}
	resourceType = fieldType{dataType: "RESOURCE_NAME"}
)

// knownFields are the types of every field the built-in streams select, so
// that they need no GoogleAdsFields request.
var knownFields = map[string]fieldType{
	"customer.id":                          int64Type,
	"customer.descriptive_name":            stringType,
	"customer.currency_code":               stringType,
	"customer.time_zone":                   stringType,
	"customer.manager":                     boolType,
	"customer.test_account":                boolType,
	"customer.status":                      enumType,
	"customer_client.id":                   int64Type,
	"customer_client.client_customer":      resourceType,
	"customer_client.descriptive_name":     stringType,
	"customer_client.currency_code":        stringType,
	"customer_client.time_zone":            stringType,
	"customer_client.manager":              boolType,
	"customer_client.test_account":         boolType,
	"customer_client.status":               enumType,
	"customer_client.level":                int64Type,
	"campaign.id":                          int64Type,
	"campaign.name":                        stringType,
	"campaign.status":                      enumType,
	"campaign.advertising_channel_type":    enumType,
	"campaign.bidding_strategy_type":       enumType,
	"campaign.campaign_budget":             resourceType,
	"ad_group.id":                          int64Type,
	"ad_group.name":                        stringType,
	"ad_group.status":                      enumType,
	"ad_group.type":                        enumType,
	"ad_group.cpc_bid_micros":              int64Type,
	"ad_group_ad.ad.id":                    int64Type,
	"ad_group_ad.ad.name":                  stringType,
	"ad_group_ad.ad.type":                  enumType,
	"ad_group_ad.ad.final_urls":            {dataType: "STRING", repeated: true},
	"ad_group_ad.status":                   enumType,
	"geographic_view.country_criterion_id": int64Type,
	"geographic_view.location_type":        enumType,
	"segments.date":                        dateType,
	"metrics.impressions":                  int64Type,
	"metrics.clicks":                       int64Type,
	"metrics.cost_micros":                  int64Type,
	"metrics.conversions":                  doubleType,
	"metrics.conversions_value":            doubleType,
	"metrics.all_conversions":              doubleType,
	"metrics.ctr":                          doubleType,
	"metrics.average_cpc":                  doubleType,
}

// property returns the JSON schema of a field type: INT64, UINT64 and
// INT32 are integers, DOUBLE and FLOAT numbers, MESSAGE objects, DATE dated
// strings and everything else strings; a repeated field is an array of them.
func (t fieldType) property() synclet.PropertySpec {
	var p synclet.PropertySpec
	switch t.dataType {
	case "INT64", "UINT64", "INT32":
		p.Type = synclet.PropTypes{synclet.Integer}
	case "DOUBLE", "FLOAT":
		p.Type = synclet.PropTypes{synclet.Number}
	case "BOOLEAN":
		p.Type = synclet.PropTypes{boolean}
	case "MESSAGE":
		p.Type = synclet.PropTypes{synclet.Object}
	default:
		p.Type = synclet.PropTypes{synclet.String}
		if t.dataType == "DATE" {
			p.Format = synclet.Date
		}
	}
	if !t.repeated {
		return p
	}
	items := map[string]any{"type": string(p.Type[0])}
	if p.Format != "" {
		items["format"] = string(p.Format)
	}
	return synclet.PropertySpec{PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Array}}, Items: items}
}

// camel returns the REST JSON name of a GAQL path segment: ad_group_ad
// becomes adGroupAd.
func camel(segment string) string {
	parts := strings.Split(segment, "_")
	for i := 1; i < len(parts); i++ {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	return strings.Join(parts, "")
}

// lookup returns the value at a GAQL path in a REST result row.
func lookup(row map[string]any, path string) (any, bool) {
	var cur any = row
	for seg := range strings.SplitSeq(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = m[camel(seg)]; !ok {
			return nil, false
		}
	}
	return cur, cur != nil
}

// record returns the selected fields of a REST result row under their record
// keys, converted to their schema types. The API writes 64-bit integers as
// strings; they become JSON numbers. A field that is missing or not
// convertible is left out.
func record(fs []field, row map[string]any) map[string]any {
	rec := make(map[string]any, len(fs))
	for _, f := range fs {
		v, ok := lookup(row, f.path)
		if !ok {
			continue
		}
		if v, ok = convert(f.typ, v); ok {
			rec[f.key] = v
		}
	}
	return rec
}

func convert(t fieldType, v any) (any, bool) {
	if t.repeated {
		items, ok := v.([]any)
		if !ok {
			return nil, false
		}
		out := make([]any, 0, len(items))
		one := fieldType{dataType: t.dataType}
		for _, it := range items {
			if c, ok := convert(one, it); ok {
				out = append(out, c)
			}
		}
		return out, true
	}
	switch t.dataType {
	case "INT64", "UINT64", "INT32":
		s, ok := numberText(v)
		if !ok {
			return nil, false
		}
		if _, err := strconv.ParseInt(s, 10, 64); err != nil {
			if _, err := strconv.ParseUint(s, 10, 64); err != nil {
				return nil, false
			}
		}
		return json.Number(s), true
	case "DOUBLE", "FLOAT":
		s, ok := numberText(v)
		if !ok {
			return nil, false
		}
		if _, err := strconv.ParseFloat(s, 64); err != nil {
			return nil, false
		}
		return json.Number(s), true
	case "BOOLEAN":
		b, ok := v.(bool)
		return b, ok
	case "MESSAGE":
		m, ok := v.(map[string]any)
		return m, ok
	}
	switch s := v.(type) {
	case string:
		return s, true
	case json.Number:
		return s.String(), true
	case bool:
		return strconv.FormatBool(s), true
	}
	return nil, false
}

func numberText(v any) (string, bool) {
	switch t := v.(type) {
	case json.Number:
		return t.String(), true
	case string:
		return strings.TrimSpace(t), true
	}
	return "", false
}
