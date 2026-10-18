package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// boolean is the JSON schema boolean type; synclet-go-sdk names no constant for it.
const boolean synclet.PropType = "boolean"

func typed(t ...synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes(t)}
}

// credentialsSpec is the credentials property: the three auth_type options
// of the original Airbyte Google Sheets connector.
func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "Credentials for connecting to the Google Sheets API",
		PropertyType: typed(synclet.Object),
		OneOf: []synclet.PropertySpec{
			{
				Title:        "Authenticate via Google (OAuth)",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type", "client_id", "client_secret", "refresh_token"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authClient},
					"client_id": {
						Title:        "Client ID",
						Description:  "Enter your Google application's Client ID",
						PropertyType: typed(synclet.String),
						IsSecret:     true,
					},
					"client_secret": {
						Title:        "Client Secret",
						Description:  "Enter your Google application's Client Secret",
						PropertyType: typed(synclet.String),
						IsSecret:     true,
					},
					"refresh_token": {
						Title:        "Refresh Token",
						Description:  "Enter your Google application's refresh token, authorized with the spreadsheets.readonly scope (and drive.metadata.readonly for incremental sync)",
						PropertyType: typed(synclet.String),
						IsSecret:     true,
					},
				},
			},
			{
				Title:        "Service Account Key Authentication",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type", "service_account_info"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authService},
					"service_account_info": {
						Title:        "Service Account Information",
						Description:  "The JSON key of the service account to use for authorization",
						PropertyType: typed(synclet.String),
						IsSecret:     true,
					},
				},
			},
			{
				Title:        "Application Default Credentials",
				Description:  "Use Google Cloud Application Default Credentials (no extra fields needed)",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authApplicationDefault},
				},
			},
		},
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-google-sheets",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Google Sheets Source Spec",
			Description: "Reads data from a Google Sheets spreadsheet",
			Type:        "object",
			Required:    []synclet.PropertyName{"spreadsheet_id", "credentials"},
			Properties: synclet.Properties{
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"spreadsheet_id": {
						Title:        "Spreadsheet Link",
						Description:  "The ID or full URL of the Google Sheets spreadsheet",
						PropertyType: typed(synclet.String),
					},
					"credentials": credentialsSpec(),
					"batch_size": {
						Title:        "Batch Size",
						Description:  "Number of rows to fetch per batch request (default: 10000)",
						PropertyType: typed(synclet.String),
					},
					"names_conversion": {
						Title:        "Convert Column Names",
						Description:  "Convert column names to snake_case",
						PropertyType: typed(synclet.String),
					},
					"column_naming": {
						Title:        "Column Naming",
						Description:  "legacy: the header stops at the first empty cell, and two columns whose names collide fail discover. stable: every column up to the last header cell is read, an empty header is column_<letter>, and colliding names get _<letter>.",
						PropertyType: typed(synclet.String),
						Default:      columnNamingLegacy,
						Examples:     []string{columnNamingLegacy, columnNamingStable},
					},
					"value_render": {
						Title:        "Value Rendering",
						Description:  "formatted: every value is the string the sheet displays. unformatted: numbers and booleans keep their type, dates and times stay formatted strings.",
						PropertyType: typed(synclet.String),
						Default:      valueRenderFormatted,
						Examples:     []string{valueRenderFormatted, valueRenderUnformatted},
					},
					"incremental": {
						Title:        "Incremental Sync",
						Description:  "Offer incremental sync with the _spreadsheet_modified_time cursor, read from Google Drive: a sheet is read again only when its spreadsheet changed since the last sync. Needs the drive.metadata.readonly scope.",
						PropertyType: typed(boolean),
						Default:      false,
					},
					"additional_spreadsheet_ids": {
						Title:        "Additional Spreadsheets",
						Description:  "IDs or URLs of more spreadsheets to read. Their streams have the spreadsheet ID as namespace.",
						PropertyType: typed(synclet.Array),
						Items:        map[string]any{"type": "string"},
					},
					"requests_per_minute": {
						Title:        "Requests per Minute",
						Description:  "Most Sheets and Drive API requests sent per minute; the default Sheets quota is 60 read requests per minute per user.",
						PropertyType: typed(synclet.Integer),
						Default:      defaultRequestsPerMinute,
					},
				},
			},
		},
	}, nil
}
