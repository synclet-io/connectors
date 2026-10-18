package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// propBoolean is the JSON schema boolean type, which the SDK does not name.
const propBoolean synclet.PropType = "boolean"

// namespace is the namespace of every stream.
const namespace = "google_analytics"

const (
	dateDimension            = "date"
	keyPropertyID            = "property_id"
	keySampled               = "sampled"
	keySamplingRatio         = "sampling_ratio"
	keyThresholded           = "subject_to_thresholding"
	keyDataLoss              = "data_loss_from_other_row"
	maxDimensions            = 9
	maxMetrics               = 10
	reportNamePatternLiteral = `^[a-z][a-z0-9_]*$`
	// ratioDigits is how many decimal places a sampling ratio that has no
	// finite decimal expansion is rounded to.
	ratioDigits = 18
)

var (
	reportNamePattern = regexp.MustCompile(reportNamePatternLiteral)
	unsafeKeyChar     = regexp.MustCompile(`[^A-Za-z0-9_]`)
	numberLiteral     = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)
)

// reportDef is one stream: a runReport over dimensions and metrics, with
// date the first dimension.
type reportDef struct {
	name       string
	dimensions []string
	metrics    []string
	custom     bool
}

var (
	overviewMetrics = []string{
		"totalUsers", "newUsers", "sessions", "sessionsPerUser", "averageSessionDuration",
		"screenPageViews", "screenPageViewsPerSession", "bounceRate",
	}
	builtinDefs = []reportDef{
		{name: "website_overview", dimensions: []string{dateDimension}, metrics: overviewMetrics},
		{name: "daily_active_users", dimensions: []string{dateDimension}, metrics: []string{"active1DayUsers"}},
		{name: "traffic_sources", dimensions: []string{dateDimension, "sessionSource", "sessionMedium"}, metrics: overviewMetrics},
		{name: "pages", dimensions: []string{dateDimension, "hostName", "pagePathPlusQueryString"}, metrics: []string{"screenPageViews", "bounceRate"}},
		{name: "events", dimensions: []string{dateDimension, "eventName"}, metrics: []string{"eventCount", "totalUsers", "eventCountPerUser"}},
		{name: "devices", dimensions: []string{dateDimension, "deviceCategory", "operatingSystem", "browser"}, metrics: overviewMetrics},
		{name: "locations", dimensions: []string{dateDimension, "region", "country", "city"}, metrics: overviewMetrics},
		{name: "key_events", dimensions: []string{dateDimension, "eventName"}, metrics: []string{"keyEvents"}},
	}
)

func (d reportDef) kind() string {
	if d.custom {
		return "custom report"
	}
	return "built-in report"
}

// recordKey is the record key of an API name: every character outside
// [A-Za-z0-9_] becomes _.
func recordKey(apiName string) string { return unsafeKeyChar.ReplaceAllString(apiName, "_") }

// reportDefs returns the built-in reports and one per custom report, with
// date added as the first dimension of a custom report that lacks it.
func reportDefs(cfg Config) ([]reportDef, error) {
	defs := slices.Clone(builtinDefs)
	var errs []error
	names := map[string]bool{}
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
			errs = append(errs, fmt.Errorf("custom_reports[%d]: name %s is taken by another report", i, d.name))
			continue
		}
		names[d.name] = true
		defs = append(defs, d)
	}
	for _, d := range defs {
		if err := d.checkKeys(cfg.dataQuality()); err != nil {
			errs = append(errs, err)
		}
	}
	return defs, errors.Join(errs...)
}

func customDef(r CustomReport) (reportDef, error) {
	if !reportNamePattern.MatchString(r.Name) {
		return reportDef{}, fmt.Errorf("name %q does not match %s", r.Name, reportNamePatternLiteral)
	}
	d := reportDef{name: r.Name, custom: true, dimensions: []string{dateDimension}}
	var errs []error
	seen := map[string]bool{dateDimension: true}
	for _, n := range r.Dimensions {
		switch {
		case strings.TrimSpace(n) == "":
			errs = append(errs, errors.New("a dimension name is empty"))
		case n == dateDimension:
		case seen[n]:
			errs = append(errs, fmt.Errorf("dimension %s is listed twice", n))
		default:
			d.dimensions = append(d.dimensions, n)
		}
		seen[n] = true
	}
	if len(d.dimensions) > maxDimensions {
		errs = append(errs, fmt.Errorf("%d dimensions with date, at most %d", len(d.dimensions), maxDimensions))
	}
	if len(r.Metrics) == 0 || len(r.Metrics) > maxMetrics {
		errs = append(errs, fmt.Errorf("%d metrics, needs 1 to %d", len(r.Metrics), maxMetrics))
	}
	seen = map[string]bool{}
	for _, n := range r.Metrics {
		switch {
		case strings.TrimSpace(n) == "":
			errs = append(errs, errors.New("a metric name is empty"))
		case seen[n]:
			errs = append(errs, fmt.Errorf("metric %s is listed twice", n))
		default:
			d.metrics = append(d.metrics, n)
		}
		seen[n] = true
	}
	if err := errors.Join(errs...); err != nil {
		return reportDef{}, fmt.Errorf("%s: %w", r.Name, err)
	}
	return d, nil
}

// checkKeys fails when two fields of the report, or a field and a key the
// connector adds, share a record key.
func (d reportDef) checkKeys(dataQuality bool) error {
	owner := map[string]string{keyPropertyID: "the property id"}
	if dataQuality {
		for _, k := range []string{keySampled, keySamplingRatio, keyThresholded, keyDataLoss} {
			owner[k] = "the data quality field " + k
		}
	}
	for _, n := range slices.Concat(d.dimensions, d.metrics) {
		k := recordKey(n)
		if o, ok := owner[k]; ok {
			return fmt.Errorf("%s %s: %s and %s both become record key %s", d.kind(), d.name, o, n, k)
		}
		owner[k] = n
	}
	return nil
}

// metricKind is the JSON schema type of a metric: integer for
// TYPE_INTEGER, number for every other metric type.
func metricKind(apiType string) synclet.PropType {
	if apiType == "TYPE_INTEGER" {
		return synclet.Integer
	}
	return synclet.Number
}

func typed(t ...synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes(t)}
}

// stream returns the stream of d, with metric types from kinds.
func (d reportDef) stream(kinds map[string]synclet.PropType, dataQuality bool) synclet.Stream {
	props := map[synclet.PropertyName]synclet.PropertySpec{
		keyPropertyID: {Description: "Google Analytics 4 property id", PropertyType: typed(synclet.String)},
	}
	pk := [][]string{{keyPropertyID}}
	for _, n := range d.dimensions {
		p := synclet.PropertySpec{Description: "dimension " + n, PropertyType: typed(synclet.String)}
		if n == dateDimension {
			p.Format = synclet.Date
		}
		props[synclet.PropertyName(recordKey(n))] = p
		pk = append(pk, []string{recordKey(n)})
	}
	for _, n := range d.metrics {
		props[synclet.PropertyName(recordKey(n))] = synclet.PropertySpec{Description: "metric " + n, PropertyType: typed(kinds[n])}
	}
	if dataQuality {
		props[keySampled] = synclet.PropertySpec{Description: "the report was computed from a sample of the data", PropertyType: typed(propBoolean)}
		props[keySamplingRatio] = synclet.PropertySpec{Description: "samples read divided by the sampling space; null when not sampled", PropertyType: typed(synclet.Number, synclet.Null)}
		props[keyThresholded] = synclet.PropertySpec{Description: "rows may be withheld by thresholding", PropertyType: typed(propBoolean)}
		props[keyDataLoss] = synclet.PropertySpec{Description: "some rows were bucketed into (other)", PropertyType: typed(propBoolean)}
	}
	return synclet.Stream{
		Name:                    d.name,
		Namespace:               namespace,
		JSONSchema:              synclet.ObjectSchema(props),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
		SourceDefinedCursor:     true,
		DefaultCursorField:      []string{dateDimension},
		SourceDefinedPrimaryKey: pk,
	}
}

// checkHeaders fails when a response's headers are not the requested
// dimensions and metrics in order, or a metric's type disagrees with the
// schema.
func (d reportDef) checkHeaders(kinds map[string]synclet.PropType, r reportResponse) error {
	var dims []string
	for _, h := range r.DimensionHeaders {
		dims = append(dims, h.Name)
	}
	if !slices.Equal(dims, d.dimensions) {
		return fmt.Errorf("response dimensions %v, requested %v", dims, d.dimensions)
	}
	if len(r.MetricHeaders) != len(d.metrics) {
		return fmt.Errorf("response has %d metrics, requested %d", len(r.MetricHeaders), len(d.metrics))
	}
	for i, h := range r.MetricHeaders {
		if h.Name != d.metrics[i] {
			return fmt.Errorf("response metric %d is %s, requested %s", i, h.Name, d.metrics[i])
		}
		if got := metricKind(h.Type); got != kinds[h.Name] {
			return fmt.Errorf("metric %s is %s in the response, %s in the schema", h.Name, h.Type, kinds[h.Name])
		}
	}
	return nil
}

// quality is the data quality fields of a response.
type quality struct {
	sampled     bool
	ratio       any
	thresholded bool
	dataLoss    bool
}

func (q quality) any() bool { return q.sampled || q.thresholded || q.dataLoss }

func responseQuality(r reportResponse) (quality, error) {
	q := quality{
		thresholded: r.Metadata.SubjectToThresholding,
		dataLoss:    r.Metadata.DataLossFromOtherRow,
	}
	if len(r.Metadata.SamplingMetadatas) == 0 {
		return q, nil
	}
	q.sampled = true
	read, space := new(big.Int), new(big.Int)
	for _, m := range r.Metadata.SamplingMetadatas {
		a, ok1 := new(big.Int).SetString(m.SamplesReadCount.String(), 10)
		b, ok2 := new(big.Int).SetString(m.SamplingSpaceSize.String(), 10)
		if !ok1 || !ok2 {
			return q, fmt.Errorf("sampling metadata %q of %q is not a pair of integers", m.SamplesReadCount, m.SamplingSpaceSize)
		}
		read.Add(read, a)
		space.Add(space, b)
	}
	if space.Sign() <= 0 {
		return q, fmt.Errorf("sampling space size %s is not positive", space)
	}
	q.ratio = json.Number(decimal(new(big.Rat).SetFrac(read, space)))
	return q, nil
}

// decimal writes r in decimal: exactly when its expansion is finite, else
// rounded to ratioDigits places.
func decimal(r *big.Rat) string {
	den := new(big.Int).Set(r.Denom())
	digits := 0
	for _, p := range []int64{2, 5} {
		n := 0
		bp := big.NewInt(p)
		for new(big.Int).Mod(den, bp).Sign() == 0 {
			den.Div(den, bp)
			n++
		}
		digits = max(digits, n)
	}
	if den.Cmp(big.NewInt(1)) != 0 {
		digits = ratioDigits
	}
	s := r.FloatString(digits)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

// record converts one row. date becomes YYYY-MM-DD and must be day; an
// integer metric becomes an int64 and any other metric its decimal literal
// unchanged.
func (d reportDef) record(kinds map[string]synclet.PropType, prop, day string, dims, mets []value, q *quality) (map[string]any, error) {
	if len(dims) != len(d.dimensions) || len(mets) != len(d.metrics) {
		return nil, fmt.Errorf("row has %d dimensions and %d metrics, headers %d and %d", len(dims), len(mets), len(d.dimensions), len(d.metrics))
	}
	rec := map[string]any{keyPropertyID: prop}
	for i, n := range d.dimensions {
		v := dims[i].Value
		if n == dateDimension {
			t, err := time.Parse("20060102", v)
			if err != nil {
				return nil, fmt.Errorf("date %q is not YYYYMMDD", v)
			}
			if v = t.Format(dateLayout); v != day {
				return nil, fmt.Errorf("row of date %s in the report of %s", v, day)
			}
		}
		rec[recordKey(n)] = v
	}
	for i, n := range d.metrics {
		v := mets[i].Value
		if kinds[n] == synclet.Integer {
			x, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("metric %s: %q is not an integer", n, v)
			}
			rec[recordKey(n)] = x
			continue
		}
		if !numberLiteral.MatchString(v) {
			return nil, fmt.Errorf("metric %s: %q is not a number", n, v)
		}
		rec[recordKey(n)] = json.Number(v)
	}
	if q != nil {
		rec[keySampled] = q.sampled
		rec[keySamplingRatio] = q.ratio
		rec[keyThresholded] = q.thresholded
		rec[keyDataLoss] = q.dataLoss
	}
	return rec, nil
}
