package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "A service account of the organization (recommended), or the legacy API secret of the project.",
		PropertyType: typed(synclet.Object),
		Order:        order(0),
		OneOf: []synclet.PropertySpec{
			{
				Title:        authServiceAccount,
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"username", "secret", "project_id"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"option_title": {PropertyType: typed(synclet.String), Const: authServiceAccount, Order: order(0)},
					"username": {
						Title:        "Username",
						Description:  "Username of the service account (Organization settings > Service accounts).",
						PropertyType: typed(synclet.String), Order: order(1),
					},
					"secret": {
						Title:        "Secret",
						Description:  "Secret of the service account.",
						PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
					},
					"project_id": {
						Title:        "Project ID",
						Description:  "ID of the project to read (Project settings > Overview). The service account must be a member of it.",
						PropertyType: typed(synclet.Integer), Examples: []string{"2195193"}, Order: order(3),
					},
				},
			},
			{
				Title:        authProjectSecret,
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"api_secret"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"option_title": {PropertyType: typed(synclet.String), Const: authProjectSecret, Order: order(0)},
					"api_secret": {
						Title:        "Project Secret",
						Description:  "API secret of the project (Project settings > Overview). Mixpanel deprecates project secrets in favour of service accounts.",
						PropertyType: typed(synclet.String), IsSecret: true, Order: order(1),
					},
				},
			},
		},
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-mixpanel",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Mixpanel Source Spec",
			Description: "Reads raw events, user profiles, cohorts and their members, annotations, funnels and revenue of one Mixpanel project.",
			Type:        "object",
			Required:    []synclet.PropertyName{"credentials"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"credentials": credentialsSpec(),
				"region": {
					Title:        "Region",
					Description:  "Data residency of the project: US (mixpanel.com, data.mixpanel.com) or EU (eu.mixpanel.com, data-eu.mixpanel.com).",
					PropertyType: typed(synclet.String), Default: regionUS, Examples: []string{regionUS, regionEU}, Order: order(1),
				},
				"project_timezone": {
					Title:        "Project Timezone",
					Description:  "Time zone of the project (Project settings > Overview), as an IANA name. Mixpanel reads from_date and to_date as days of this zone.",
					PropertyType: typed(synclet.String), Default: defaultTimezone, Examples: []string{"US/Pacific", "UTC", "Europe/Berlin"}, Order: order(2),
				},
				"start_date": {
					Title:        "Start Date",
					Description:  "First day, YYYY-MM-DD, whose events, funnels, revenue and annotations are read; empty reads from a year before today.",
					PropertyType: typed(synclet.String), Examples: []string{"2026-01-01"}, Order: order(3),
				},
				"end_date": {
					Title:        "End Date",
					Description:  "Last day, YYYY-MM-DD, that is read; empty reads up to today in the project time zone.",
					PropertyType: typed(synclet.String), Examples: []string{"2026-12-31"}, Order: order(4),
				},
				"attribution_window": {
					Title:        "Attribution Window",
					Description:  "Days before the saved cursor an incremental read of export, funnels and revenue starts again, since Mixpanel keeps accepting late events for recent days.",
					PropertyType: typed(synclet.Integer), Default: defaultAttributionWindow, Order: order(5),
				},
				"date_window_size": {
					Title:        "Date Window Size",
					Description:  "Days requested at a time from the date-ranged endpoints. Smaller windows checkpoint more often but spend more of the hourly request limit.",
					PropertyType: typed(synclet.Integer), Default: defaultDateWindowSize, Order: order(6),
				},
				"page_size": {
					Title:        "Profiles Page Size",
					Description:  "Profiles per request of engage and cohort_members, at most 1000.",
					PropertyType: typed(synclet.Integer), Default: defaultPageSize, Order: order(7),
				},
			}},
		},
	}, nil
}
