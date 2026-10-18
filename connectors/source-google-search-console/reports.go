package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// propBoolean is the JSON schema boolean type, which the SDK does not name.
const propBoolean synclet.PropType = "boolean"

// namespace is the namespace of every stream.
const namespace = "google_search_console"

const (
	streamSites    = "sites"
	streamSitemaps = "sitemaps"

	dimDate             = "date"
	dimQuery            = "query"
	dimPage             = "page"
	dimCountry          = "country"
	dimDevice           = "device"
	dimSearchAppearance = "searchAppearance"

	keySiteURL     = "site_url"
	keySearchType  = "search_type"
	keyClicks      = "clicks"
	keyImpressions = "impressions"
	keyCTR         = "ctr"
	keyPosition    = "position"

	reportNamePatternLiteral = `^[a-z][a-z0-9_]*$`
)

var reportNamePattern = regexp.MustCompile(reportNamePatternLiteral)

// dimensions are the dimensions a report may group by, in the order the
// spec lists them.
var dimensions = []string{dimDate, dimQuery, dimPage, dimCountry, dimDevice, dimSearchAppearance}

// recordKey is the record key of a dimension.
func recordKey(dim string) string {
	if dim == dimSearchAppearance {
		return "search_appearance"
	}
	return dim
}

var (
	// builtinSearchTypes are the search types every built-in report reads.
	// discover and googleNews reject the query dimension, so only custom
	// reports read them.
	builtinSearchTypes = []string{"web", "image", "video", "news"}
	// searchTypes are the search types a custom report may read.
	searchTypes = []string{"web", "image", "video", "news", "discover", "googleNews"}
	// noQueryTypes are the search types that reject the query dimension.
	noQueryTypes = []string{"discover", "googleNews"}
)

// reportDef is one search analytics stream: a searchAnalytics.query per
// site, day and search type, with date the first dimension.
type reportDef struct {
	name        string
	dimensions  []string
	searchTypes []string
}

func builtin(name string, dims ...string) reportDef {
	return reportDef{name: name, dimensions: dims, searchTypes: builtinSearchTypes}
}

var builtinDefs = []reportDef{
	builtin("search_analytics_by_date", dimDate),
	builtin("search_analytics_by_query", dimDate, dimQuery),
	builtin("search_analytics_by_page", dimDate, dimPage),
	builtin("search_analytics_by_country", dimDate, dimCountry),
	builtin("search_analytics_by_device", dimDate, dimDevice),
	builtin("search_analytics_by_search_appearance", dimDate, dimSearchAppearance),
	builtin("search_analytics_by_query_page", dimDate, dimQuery, dimPage),
}

// reportDefs returns the built-in reports and one per custom report, with
// date added as the first dimension of a custom report that lacks it.
func reportDefs(cfg Config) ([]reportDef, error) {
	defs := slices.Clone(builtinDefs)
	var errs []error
	names := map[string]bool{streamSites: true, streamSitemaps: true}
	for _, d := range builtinDefs {
		names[d.name] = true
	}
	for i, r := range cfg.CustomReports {
		d, err := customDef(r)
		if err != nil {
			errs = append(errs, fmt.Errorf("custom_reports[%d]: %w", i, err))
			continue
		}
		if names[d.name] {
			errs = append(errs, fmt.Errorf("custom_reports[%d]: name %s is taken by another stream", i, d.name))
			continue
		}
		names[d.name] = true
		defs = append(defs, d)
	}
	return defs, errors.Join(errs...)
}

func customDef(r CustomReport) (reportDef, error) {
	if !reportNamePattern.MatchString(r.Name) {
		return reportDef{}, fmt.Errorf("name %q does not match %s", r.Name, reportNamePatternLiteral)
	}
	st := r.SearchType
	if st == "" {
		st = "web"
	}
	d := reportDef{name: r.Name, dimensions: []string{dimDate}, searchTypes: []string{st}}
	var errs []error
	if !slices.Contains(searchTypes, st) {
		errs = append(errs, fmt.Errorf("search_type %q is not one of %s", st, strings.Join(searchTypes, ", ")))
	}
	seen := map[string]bool{dimDate: true}
	for _, n := range r.Dimensions {
		switch {
		case !slices.Contains(dimensions, n):
			errs = append(errs, fmt.Errorf("dimension %q is not one of %s", n, strings.Join(dimensions, ", ")))
		case n == dimDate:
		case seen[n]:
			errs = append(errs, fmt.Errorf("dimension %s is listed twice", n))
		default:
			d.dimensions = append(d.dimensions, n)
		}
		seen[n] = true
	}
	if slices.Contains(noQueryTypes, st) && slices.Contains(d.dimensions, dimQuery) {
		errs = append(errs, fmt.Errorf("search type %s does not support the query dimension", st))
	}
	if err := errors.Join(errs...); err != nil {
		return reportDef{}, fmt.Errorf("%s: %w", r.Name, err)
	}
	return d, nil
}

func typed(t ...synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes(t)}
}

// stream returns the stream of d.
func (d reportDef) stream() synclet.Stream {
	props := map[synclet.PropertyName]synclet.PropertySpec{
		keySiteURL:     {Description: "Search Console property the row belongs to", PropertyType: typed(synclet.String)},
		keySearchType:  {Description: "search type of the row: web, image, video, news, discover or googleNews", PropertyType: typed(synclet.String)},
		keyClicks:      {Description: "clicks from search results", PropertyType: typed(synclet.Integer)},
		keyImpressions: {Description: "impressions in search results", PropertyType: typed(synclet.Integer)},
		keyCTR:         {Description: "click-through rate, clicks divided by impressions", PropertyType: typed(synclet.Number)},
		keyPosition:    {Description: "average position in search results", PropertyType: typed(synclet.Number)},
	}
	pk := [][]string{{keySiteURL}, {keySearchType}}
	for _, n := range d.dimensions {
		p := synclet.PropertySpec{Description: "dimension " + n, PropertyType: typed(synclet.String)}
		if n == dimDate {
			p.Format = synclet.Date
		}
		props[synclet.PropertyName(recordKey(n))] = p
		pk = append(pk, []string{recordKey(n)})
	}
	return synclet.Stream{
		Name:                    d.name,
		Namespace:               namespace,
		JSONSchema:              synclet.ObjectSchema(props),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
		SourceDefinedCursor:     true,
		DefaultCursorField:      []string{dimDate},
		SourceDefinedPrimaryKey: pk,
	}
}

func sitesStream() synclet.Stream {
	return synclet.Stream{
		Name:      streamSites,
		Namespace: namespace,
		JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
			keySiteURL:         {Description: "Search Console property", PropertyType: typed(synclet.String)},
			"permission_level": {Description: "permission of the credentials on the property, such as siteOwner or siteFullUser", PropertyType: typed(synclet.String)},
		}),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{keySiteURL}},
	}
}

func timestamp(desc string) synclet.PropertySpec {
	return synclet.PropertySpec{
		Description:  desc,
		PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWithTZ},
		Format:       synclet.DateTime,
	}
}

func sitemapsStream() synclet.Stream {
	return synclet.Stream{
		Name:      streamSitemaps,
		Namespace: namespace,
		JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
			keySiteURL:          {Description: "Search Console property the sitemap is submitted to", PropertyType: typed(synclet.String)},
			"path":              {Description: "URL of the sitemap", PropertyType: typed(synclet.String)},
			"last_submitted":    timestamp("when the sitemap was last submitted"),
			"last_downloaded":   timestamp("when Google last downloaded the sitemap"),
			"is_pending":        {Description: "the sitemap is waiting to be processed", PropertyType: typed(propBoolean, synclet.Null)},
			"is_sitemaps_index": {Description: "the sitemap is a sitemap index", PropertyType: typed(propBoolean, synclet.Null)},
			"type":              {Description: "type of the sitemap, such as sitemap or rssFeed", PropertyType: typed(synclet.String, synclet.Null)},
			"warnings":          {Description: "warnings Google found in the sitemap", PropertyType: typed(synclet.Integer, synclet.Null)},
			"errors":            {Description: "errors Google found in the sitemap", PropertyType: typed(synclet.Integer, synclet.Null)},
			"contents": {
				Description:  "URLs submitted per content type",
				PropertyType: typed(synclet.Array, synclet.Null),
				Items: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"type":      map[string]any{"type": []string{"string", "null"}},
						"submitted": map[string]any{"type": []string{"integer", "null"}},
						"indexed":   map[string]any{"type": []string{"integer", "null"}},
					},
				},
			},
		}),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{keySiteURL}, {"path"}},
	}
}

// integral returns n as an int64 when it is a whole number, such as 3 or
// 3.0, and fails otherwise.
func integral(field string, n json.Number) (int64, error) {
	if i, err := n.Int64(); err == nil {
		return i, nil
	}
	r, ok := new(big.Rat).SetString(n.String())
	if !ok || !r.IsInt() || !r.Num().IsInt64() {
		return 0, fmt.Errorf("%s %q is not an integer", field, n)
	}
	return r.Num().Int64(), nil
}

// record converts one row of a day. Its keys must be the requested
// dimensions, with date the requested day; clicks and impressions must be
// whole numbers; ctr and position pass through as returned.
func (d reportDef) record(site, searchType, day string, row analyticsRow) (map[string]any, error) {
	if len(row.Keys) != len(d.dimensions) {
		return nil, fmt.Errorf("row has %d keys, requested %d dimensions %v", len(row.Keys), len(d.dimensions), d.dimensions)
	}
	rec := map[string]any{keySiteURL: site, keySearchType: searchType}
	for i, n := range d.dimensions {
		if n == dimDate && row.Keys[i] != day {
			return nil, fmt.Errorf("row of date %s in the report of %s", row.Keys[i], day)
		}
		rec[recordKey(n)] = row.Keys[i]
	}
	for _, m := range []struct {
		key string
		v   json.Number
	}{{keyClicks, row.Clicks}, {keyImpressions, row.Impressions}} {
		if m.v == "" {
			return nil, fmt.Errorf("row has no %s", m.key)
		}
		x, err := integral(m.key, m.v)
		if err != nil {
			return nil, err
		}
		rec[m.key] = x
	}
	for _, m := range []struct {
		key string
		v   json.Number
	}{{keyCTR, row.CTR}, {keyPosition, row.Position}} {
		if m.v == "" {
			return nil, fmt.Errorf("row has no %s", m.key)
		}
		rec[m.key] = m.v
	}
	return rec, nil
}
