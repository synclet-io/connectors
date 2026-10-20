package main

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"
	"sort"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// boolean is the JSON schema type the SDK has no constant for.
const boolean synclet.PropType = "boolean"

const (
	streamExport        = "export"
	streamEngage        = "engage"
	streamCohorts       = "cohorts"
	streamCohortMembers = "cohort_members"
	streamAnnotations   = "annotations"
	streamFunnels       = "funnels"
	streamRevenue       = "revenue"

	// topPropertiesLimit is how many event property names discover asks
	// events/properties/top for.
	topPropertiesLimit = "1000"
)

func nullable(t synclet.PropType, description string) synclet.PropertySpec {
	return synclet.PropertySpec{Description: description, PropertyType: synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}}
}

func required(t synclet.PropType, description string) synclet.PropertySpec {
	return synclet.PropertySpec{Description: description, PropertyType: typed(t)}
}

func localTimestamp(description string) synclet.PropertySpec {
	return synclet.PropertySpec{
		Description:  description,
		PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWOTZ},
		Format:       synclet.DateTime,
	}
}

// openSchema is an object schema that also accepts fields it does not list.
func openSchema(props map[synclet.PropertyName]synclet.PropertySpec) json.RawMessage {
	b, err := json.Marshal(struct {
		Type                 string                                        `json:"type"`
		AdditionalProperties bool                                          `json:"additionalProperties"`
		Properties           map[synclet.PropertyName]synclet.PropertySpec `json:"properties"`
	}{Type: "object", AdditionalProperties: true, Properties: props})
	if err != nil {
		panic(fmt.Sprintf("schema: %v", err))
	}
	return b
}

func incremental(name string, cursor string, pk [][]string, props map[synclet.PropertyName]synclet.PropertySpec) synclet.Stream {
	return synclet.Stream{
		Name:                    name,
		JSONSchema:              openSchema(props),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
		SourceDefinedCursor:     true,
		DefaultCursorField:      []string{cursor},
		SourceDefinedPrimaryKey: pk,
	}
}

func exportProps() map[synclet.PropertyName]synclet.PropertySpec {
	return map[synclet.PropertyName]synclet.PropertySpec{
		"event": required(synclet.String, "Name of the event"),
		"time": {
			Description:  "When the event happened, from the time property",
			PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String}, AirbyteType: synclet.TimestampWithTZ},
			Format:       synclet.DateTime,
		},
		"distinct_id": nullable(synclet.String, "Distinct ID of the user who sent the event"),
		"insert_id":   nullable(synclet.String, "$insert_id, the deduplication key of the event"),
	}
}

func profileProps() map[synclet.PropertyName]synclet.PropertySpec {
	return map[synclet.PropertyName]synclet.PropertySpec{
		"distinct_id": required(synclet.String, "Distinct ID of the user"),
		"last_seen":   localTimestamp("$last_seen, when the user last sent an event, in the project time zone"),
	}
}

func cohortsStream() synclet.Stream {
	return incremental(streamCohorts, "created", [][]string{{"id"}}, map[synclet.PropertyName]synclet.PropertySpec{
		"id":          required(synclet.Integer, "ID of the cohort"),
		"name":        nullable(synclet.String, "Name of the cohort"),
		"description": nullable(synclet.String, "Description of the cohort"),
		"created":     nullable(synclet.String, "When the cohort was created, YYYY-MM-DD HH:MM:SS in the project time zone"),
		"count":       nullable(synclet.Integer, "Users in the cohort"),
		"is_visible":  nullable(synclet.Integer, "1 when the cohort is visible in the Mixpanel UI"),
		"project_id":  nullable(synclet.Integer, "ID of the project"),
	})
}

func annotationsStream() synclet.Stream {
	return synclet.Stream{
		Name: streamAnnotations,
		JSONSchema: openSchema(map[synclet.PropertyName]synclet.PropertySpec{
			"id":          required(synclet.Integer, "ID of the annotation"),
			"project_id":  nullable(synclet.Integer, "ID of the project"),
			"date":        nullable(synclet.String, "Day the annotation marks, YYYY-MM-DD HH:MM:SS"),
			"description": nullable(synclet.String, "Text of the annotation"),
			"user":        nullable(synclet.Object, "User who created the annotation"),
		}),
		SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh},
		SourceDefinedPrimaryKey: [][]string{{"id"}},
	}
}

func funnelsStream() synclet.Stream {
	return incremental(streamFunnels, "date", [][]string{{"funnel_id"}, {"date"}}, map[synclet.PropertyName]synclet.PropertySpec{
		"funnel_id": required(synclet.Integer, "ID of the saved funnel"),
		"name":      nullable(synclet.String, "Name of the saved funnel"),
		"date": {
			Description: "Day of the funnel counts, in the project time zone", PropertyType: typed(synclet.String), Format: synclet.Date,
		},
		"steps":    nullable(synclet.Array, "Each step with its event, count, conversion ratios and average time"),
		"analysis": nullable(synclet.Object, "Totals of the day: completion, starting_amount, steps and worst"),
	})
}

func revenueStream() synclet.Stream {
	return incremental(streamRevenue, "date", [][]string{{"date"}}, map[synclet.PropertyName]synclet.PropertySpec{
		"date": {
			Description: "Day of the revenue, in the project time zone", PropertyType: typed(synclet.String), Format: synclet.Date,
		},
		"amount":     nullable(synclet.Number, "Sum of the $amount of the day's transactions"),
		"count":      nullable(synclet.Integer, "Users with a transaction that day"),
		"paid_count": nullable(synclet.Integer, "Users with a positive transaction that day"),
	})
}

// renamer maps Mixpanel property names to record field names. A lifted
// property becomes its fixed field. Otherwise a leading $ is dropped,
// unless the project also has the name without it, which keeps it as
// _name; a name equal to a fixed field gets the prefix property_.
type renamer struct {
	lifted map[string]string
	fixed  map[string]bool
}

func newRenamer(lifted map[string]string, fixed map[synclet.PropertyName]synclet.PropertySpec) renamer {
	r := renamer{lifted: lifted, fixed: map[string]bool{}}
	for k := range fixed {
		r.fixed[string(k)] = true
	}
	return r
}

// field returns the field of property name; has reports whether the
// project has another property.
func (r renamer) field(name string, has func(string) bool) string {
	if f, ok := r.lifted[name]; ok {
		return f
	}
	f := name
	if stripped, ok := strings.CutPrefix(name, "$"); ok && stripped != "" {
		f = stripped
		if has(stripped) {
			f = "_" + stripped
		}
	}
	if r.fixed[f] {
		f = "property_" + f
	}
	return f
}

// schemaHints returns the property names a schema made by discover shows
// the project has: x, when the schema holds both x and _x, the field of $x.
func schemaHints(schema json.RawMessage) map[string]bool {
	var s struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	hints := map[string]bool{}
	if json.Unmarshal(schema, &s) != nil {
		return hints
	}
	for f := range s.Properties {
		if x, ok := strings.CutPrefix(f, "_"); ok {
			if _, both := s.Properties[x]; both {
				hints[x] = true
			}
		}
	}
	return hints
}

func setOf(names []string) func(string) bool {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return func(n string) bool { return set[n] }
}

// eventPropertyNames returns the property names of the project's events.
func eventPropertyNames(ctx context.Context, c *apiClient) ([]string, error) {
	var top map[string]json.RawMessage
	if err := c.getJSON(ctx, get(queryAPI, "events/properties/top", url.Values{"limit": {topPropertiesLimit}}), &top); err != nil {
		return nil, fmt.Errorf("event properties: %w", err)
	}
	names := make([]string, 0, len(top))
	for n := range top {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}

// exportSchema adds the event properties to the fixed fields of export.
// Mixpanel properties have no fixed type, so each is a nullable string.
// time, distinct_id and $insert_id are fixed fields already.
func exportSchema(names []string) map[synclet.PropertyName]synclet.PropertySpec {
	props := exportProps()
	r, has := exportRenamer(), setOf(names)
	for _, n := range names {
		if _, ok := r.lifted[n]; ok {
			continue
		}
		props[synclet.PropertyName(r.field(n, has))] = nullable(synclet.String, "Event property "+n)
	}
	return props
}

// exportRenamer lifts time, distinct_id and $insert_id to the fixed fields
// of export.
func exportRenamer() renamer {
	return newRenamer(map[string]string{"time": "time", "distinct_id": "distinct_id", "$insert_id": "insert_id"}, exportProps())
}

// profileProperty is one entry of engage/properties.
type profileProperty struct {
	Type string `json:"type"`
}

// profileProperties returns the profile properties of the project with
// their Mixpanel types.
func profileProperties(ctx context.Context, c *apiClient) (map[string]string, error) {
	var resp struct {
		Results map[string]profileProperty `json:"results"`
	}
	if err := c.getJSON(ctx, get(queryAPI, "engage/properties", nil), &resp); err != nil {
		return nil, fmt.Errorf("profile properties: %w", err)
	}
	types := make(map[string]string, len(resp.Results))
	for n, p := range resp.Results {
		types[n] = p.Type
	}
	return types, nil
}

// profileSchema adds the profile properties to fixed, typed by their
// Mixpanel type; a string, an unknown type or a property without a type is
// a nullable string.
func profileSchema(fixed map[synclet.PropertyName]synclet.PropertySpec, types map[string]string) map[synclet.PropertyName]synclet.PropertySpec {
	r := profileRenamer(fixed)
	has := func(n string) bool { _, ok := types[n]; return ok }
	props := maps.Clone(fixed)
	for n, t := range types {
		if _, ok := r.lifted[n]; ok {
			continue
		}
		props[synclet.PropertyName(r.field(n, has))] = profileSpec(n, t)
	}
	return props
}

// profileRenamer lifts $last_seen to the last_seen field.
func profileRenamer(fixed map[synclet.PropertyName]synclet.PropertySpec) renamer {
	return newRenamer(map[string]string{"$last_seen": "last_seen"}, fixed)
}

func profileSpec(name, mixpanelType string) synclet.PropertySpec {
	desc := "Profile property " + name
	switch mixpanelType {
	case "number":
		return nullable(synclet.Number, desc)
	case "boolean":
		return nullable(boolean, desc)
	case "datetime":
		return localTimestamp(desc + ", in the project time zone")
	case "list":
		return nullable(synclet.Array, desc)
	case "object":
		return nullable(synclet.Object, desc)
	}
	return nullable(synclet.String, desc)
}

func engageStream(types map[string]string) synclet.Stream {
	return incremental(streamEngage, "last_seen", [][]string{{"distinct_id"}}, profileSchema(profileProps(), types))
}

func cohortMembersFixed() map[synclet.PropertyName]synclet.PropertySpec {
	props := profileProps()
	props["cohort_id"] = required(synclet.Integer, "ID of the cohort the user is a member of")
	return props
}

func cohortMembersStream(types map[string]string) synclet.Stream {
	return incremental(streamCohortMembers, "last_seen", [][]string{{"cohort_id"}, {"distinct_id"}}, profileSchema(cohortMembersFixed(), types))
}

// Discover asks the Query API for the event and profile property names,
// which become the fields of export, engage and cohort_members.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	c := s.apiClient(cfg)
	events, err := eventPropertyNames(ctx, c)
	if err != nil {
		return nil, err
	}
	profiles, err := profileProperties(ctx, c)
	if err != nil {
		return nil, err
	}
	export := incremental(streamExport, "time", nil, exportSchema(events))
	return &synclet.Catalog{Streams: []synclet.Stream{
		export,
		engageStream(profiles),
		cohortsStream(),
		cohortMembersStream(profiles),
		annotationsStream(),
		funnelsStream(),
		revenueStream(),
	}}, nil
}
