package main

import (
	"context"
	"encoding/json"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const (
	streamEvents               = "events"
	streamActiveUsers          = "active_users"
	streamAverageSessionLength = "average_session_length"
	streamCohorts              = "cohorts"
	streamAnnotations          = "annotations"
	streamEventsList           = "events_list"
)

// streamNames lists every stream in catalog order.
var streamNames = []string{
	streamEvents, streamActiveUsers, streamAverageSessionLength,
	streamCohorts, streamAnnotations, streamEventsList,
}

func prop(t synclet.PropType, description string) synclet.PropertySpec {
	return synclet.PropertySpec{Description: description, PropertyType: synclet.PropertyType{Type: synclet.PropTypes{t, synclet.Null}}}
}

func timestampProp(description string) synclet.PropertySpec {
	return synclet.PropertySpec{
		Description:  description,
		PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.String, synclet.Null}, AirbyteType: synclet.TimestampWithTZ},
		Format:       synclet.DateTime,
	}
}

func dateProp(description string) synclet.PropertySpec {
	return synclet.PropertySpec{Description: description, PropertyType: typed(synclet.String), Format: synclet.Date}
}

// openObjectProp is an object property whose keys are not known up front.
func openObjectProp(description string) synclet.PropertySpec {
	return prop(synclet.Object, description)
}

func anyProp(description string) synclet.PropertySpec {
	return synclet.PropertySpec{Description: description}
}

// eventTimestampFields are the event fields Amplitude sends as
// "YYYY-MM-DD HH:MM:SS.ffffff" in UTC; records carry them in RFC 3339.
var eventTimestampFields = []string{
	"client_event_time", "client_upload_time", "event_time", "processed_time",
	"server_received_time", "server_upload_time", "user_creation_time",
}

var eventProps = map[synclet.PropertyName]synclet.PropertySpec{
	"uuid":                      {Description: "Unique identifier of the event row in Amplitude.", PropertyType: typed(synclet.String)},
	"$insert_id":                prop(synclet.String, "Insert ID the sender attached to deduplicate the event."),
	"adid":                      prop(synclet.String, "Google Play Services advertising ID of the device."),
	"amplitude_attribution_ids": anyProp("Attribution identifiers Amplitude assigned."),
	"amplitude_event_type":      prop(synclet.String, "Type of an event Amplitude created itself, such as a merge or identify event."),
	"amplitude_id":              prop(synclet.Integer, "Amplitude's identifier of the user."),
	"app":                       prop(synclet.Integer, "Project ID the event belongs to."),
	"city":                      prop(synclet.String, "City of the user."),
	"client_event_time":         timestampProp("Time the event happened on the device, as reported by the device."),
	"client_upload_time":        timestampProp("Time the device uploaded the event."),
	"country":                   prop(synclet.String, "Country of the user."),
	"data":                      openObjectProp("Extra data Amplitude attached to the event."),
	"data_type":                 prop(synclet.String, "Kind of row, such as event or identify."),
	"device_brand":              prop(synclet.String, "Brand of the device."),
	"device_carrier":            prop(synclet.String, "Carrier of the device."),
	"device_family":             prop(synclet.String, "Family of the device, such as iPhone or Samsung Galaxy."),
	"device_id":                 prop(synclet.String, "Identifier of the device."),
	"device_manufacturer":       prop(synclet.String, "Manufacturer of the device."),
	"device_model":              prop(synclet.String, "Model of the device."),
	"device_type":               prop(synclet.String, "Type of the device, such as Apple iPhone 15."),
	"dma":                       prop(synclet.String, "Designated market area of the user."),
	"event_id":                  prop(synclet.Integer, "Counter of the event within the user's events."),
	"event_properties":          openObjectProp("Properties sent with the event."),
	"event_time":                timestampProp("Time of the event: the client event time corrected for the device clock."),
	"event_type":                prop(synclet.String, "Name of the event."),
	"global_user_properties":    openObjectProp("Global user properties of the user."),
	"group_properties":          openObjectProp("Properties of the groups the event belongs to."),
	"groups":                    openObjectProp("Groups the event belongs to."),
	"idfa":                      prop(synclet.String, "Identifier for advertisers of an iOS device."),
	"ip_address":                prop(synclet.String, "IP address of the device."),
	"is_attribution_event":      anyProp("Whether the event came from an attribution partner."),
	"language":                  prop(synclet.String, "Language of the device."),
	"library":                   prop(synclet.String, "SDK or API that sent the event."),
	"location_lat":              prop(synclet.Number, "Latitude of the device."),
	"location_lng":              prop(synclet.Number, "Longitude of the device."),
	"os_name":                   prop(synclet.String, "Operating system of the device."),
	"os_version":                prop(synclet.String, "Version of the operating system."),
	"partner_id":                prop(synclet.String, "Identifier of the partner that sent the event."),
	"paying":                    anyProp("Whether the user has paid."),
	"plan":                      openObjectProp("Tracking plan branch, source and version of the event."),
	"platform":                  prop(synclet.String, "Platform of the device, such as iOS, Android or Web."),
	"processed_time":            timestampProp("Time Amplitude processed the event."),
	"region":                    prop(synclet.String, "Region of the user."),
	"sample_rate":               anyProp("Sampling rate of the event, if the project samples."),
	"server_received_time":      timestampProp("Time Amplitude's servers received the event."),
	"server_upload_time":        timestampProp("Time the event was uploaded to Amplitude's servers; the Export API selects events by it."),
	"session_id":                prop(synclet.Integer, "Start of the session in milliseconds since the epoch; -1 outside a session."),
	"source_id":                 prop(synclet.String, "Identifier of the source that sent the event."),
	"start_version":             prop(synclet.String, "App version the user first had."),
	"user_creation_time":        timestampProp("Time Amplitude first saw the user."),
	"user_id":                   prop(synclet.String, "User ID the sender set."),
	"user_properties":           openObjectProp("User properties at the time of the event."),
	"version_name":              prop(synclet.String, "App version of the device."),
}

var activeUsersProps = map[synclet.PropertyName]synclet.PropertySpec{
	"date":       dateProp("Day the counts cover, YYYY-MM-DD."),
	"statistics": {Description: "Active users of the day by country, or under the series label when not grouped.", PropertyType: typed(synclet.Object)},
}

var averageSessionLengthProps = map[synclet.PropertyName]synclet.PropertySpec{
	"date":   dateProp("Day the average covers, YYYY-MM-DD."),
	"length": prop(synclet.Number, "Average session length of the day, in seconds."),
}

var cohortProps = map[synclet.PropertyName]synclet.PropertySpec{
	"id":                  {Description: "Identifier of the cohort.", PropertyType: typed(synclet.String)},
	"appId":               prop(synclet.Integer, "Project ID the cohort belongs to."),
	"archived":            anyProp("Whether the cohort is archived."),
	"chart_id":            prop(synclet.String, "Chart the cohort was created from."),
	"createdAt":           prop(synclet.Integer, "Creation time in seconds since the epoch."),
	"definition":          openObjectProp("Definition of the cohort."),
	"description":         prop(synclet.String, "Description of the cohort."),
	"edit_id":             prop(synclet.String, "Identifier of the cohort's current edit."),
	"finished":            anyProp("Whether the last computation finished."),
	"hidden":              anyProp("Whether the cohort is hidden."),
	"is_official_content": anyProp("Whether the cohort is marked official."),
	"is_predictive":       anyProp("Whether the cohort is predictive."),
	"lastComputed":        prop(synclet.Integer, "Time of the last computation in seconds since the epoch."),
	"lastMod":             prop(synclet.Integer, "Time of the last change in seconds since the epoch."),
	"last_viewed":         prop(synclet.Integer, "Time the cohort was last viewed in seconds since the epoch."),
	"location_id":         prop(synclet.String, "Space the cohort is saved in."),
	"metadata":            anyProp("Metadata of the cohort."),
	"name":                prop(synclet.String, "Name of the cohort."),
	"owners":              {Description: "Owners of the cohort.", PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Array, synclet.Null}}, Items: map[string]any{"type": []string{"string", "null"}}},
	"popularity":          prop(synclet.Integer, "How often the cohort is used."),
	"published":           anyProp("Whether the cohort is published."),
	"shortcut_ids":        anyProp("Shortcuts to the cohort."),
	"size":                prop(synclet.Integer, "Number of users in the cohort."),
	"type":                prop(synclet.String, "Kind of cohort."),
	"view_count":          prop(synclet.Integer, "How often the cohort was viewed."),
	"viewers":             anyProp("Users who may view the cohort."),
}

var annotationProps = map[synclet.PropertyName]synclet.PropertySpec{
	"id":      {Description: "Identifier of the annotation.", PropertyType: typed(synclet.Integer)},
	"date":    prop(synclet.String, "Day the annotation marks, YYYY-MM-DD."),
	"label":   prop(synclet.String, "Title of the annotation."),
	"details": prop(synclet.String, "Details of the annotation."),
}

var eventsListProps = map[synclet.PropertyName]synclet.PropertySpec{
	"name":            {Description: "Name of the event type.", PropertyType: typed(synclet.String)},
	"value":           prop(synclet.String, "Name of the event type as the API selects it."),
	"display":         prop(synclet.String, "Display name of the event type."),
	"id":              prop(synclet.Integer, "Identifier of the event type."),
	"hidden":          anyProp("Whether the event type is hidden."),
	"deleted":         anyProp("Whether the event type is deleted."),
	"non_active":      anyProp("Whether the event type is inactive."),
	"flow_hidden":     anyProp("Whether the event type is hidden from Pathfinder."),
	"autohidden":      anyProp("Whether Amplitude hid the event type."),
	"in_waitroom":     anyProp("Whether the event type waits for approval in the tracking plan."),
	"clusters_hidden": anyProp("Whether the event type is hidden from clusters."),
	"totals":          prop(synclet.Integer, "Times the event was sent this week."),
	"totals_delta":    prop(synclet.Integer, "Change of totals against last week."),
}

// stream returns the catalog entry of a stream. Schemas allow undeclared
// fields because Amplitude adds fields to its responses.
func stream(name string) synclet.Stream {
	full := []synclet.SyncMode{synclet.SyncModeFullRefresh}
	both := []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental}
	var s synclet.Stream
	var props map[synclet.PropertyName]synclet.PropertySpec
	switch name {
	case streamEvents:
		s = synclet.Stream{SupportedSyncModes: both, SourceDefinedCursor: true, DefaultCursorField: []string{"server_upload_time"}, SourceDefinedPrimaryKey: [][]string{{"uuid"}}}
		props = eventProps
	case streamActiveUsers:
		s = synclet.Stream{SupportedSyncModes: both, SourceDefinedCursor: true, DefaultCursorField: []string{"date"}, SourceDefinedPrimaryKey: [][]string{{"date"}}}
		props = activeUsersProps
	case streamAverageSessionLength:
		s = synclet.Stream{SupportedSyncModes: both, SourceDefinedCursor: true, DefaultCursorField: []string{"date"}, SourceDefinedPrimaryKey: [][]string{{"date"}}}
		props = averageSessionLengthProps
	case streamCohorts:
		s = synclet.Stream{SupportedSyncModes: full, SourceDefinedPrimaryKey: [][]string{{"id"}}}
		props = cohortProps
	case streamAnnotations:
		s = synclet.Stream{SupportedSyncModes: full, SourceDefinedPrimaryKey: [][]string{{"id"}}}
		props = annotationProps
	case streamEventsList:
		s = synclet.Stream{SupportedSyncModes: full, SourceDefinedPrimaryKey: [][]string{{"name"}}}
		props = eventsListProps
	default:
		panic("unknown stream " + name)
	}
	raw, err := json.Marshal(map[string]any{"type": "object", "additionalProperties": true, "properties": props})
	if err != nil {
		panic(fmt.Sprintf("stream %s: schema: %v", name, err))
	}
	s.Name = name
	s.JSONSchema = raw
	return s
}

// Discover returns every stream; every project has the same ones, so
// nothing is requested.
func (Source) Discover(_ context.Context, cfg Config) (*synclet.Catalog, error) {
	if err := cfg.validate(); err != nil {
		return nil, synclet.ConfigError(err)
	}
	cat := &synclet.Catalog{}
	for _, name := range streamNames {
		cat.Streams = append(cat.Streams, stream(name))
	}
	return cat, nil
}
