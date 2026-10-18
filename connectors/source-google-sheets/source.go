package main

import (
	"context"
	"fmt"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
	"google.golang.org/api/sheets/v4"
)

const spreadsheetsReadonlyScope = "https://www.googleapis.com/auth/spreadsheets.readonly"

// GoogleSheetsSource implements airbyte.Source for Google Sheets.
type GoogleSheetsSource struct{}

// NewGoogleSheetsSource creates a new Google Sheets source connector.
func NewGoogleSheetsSource() *GoogleSheetsSource {
	return &GoogleSheetsSource{}
}

// Spec returns the connector specification defining config fields.
func (s *GoogleSheetsSource) Spec(logTracker airbyte.LogTracker) (*airbyte.ConnectorSpecification, error) {
	return &airbyte.ConnectorSpecification{
		DocumentationURL:    "https://docs.google.com/spreadsheets",
		SupportsIncremental: false,
		SupportedDestinationSyncModes: []airbyte.DestinationSyncMode{
			airbyte.DestinationSyncModeOverwrite,
			airbyte.DestinationSyncModeAppend,
		},
		ConnectionSpecification: airbyte.ConnectionSpecification{
			Title:       "Google Sheets Source Spec",
			Description: "Reads data from a Google Sheets spreadsheet",
			Type:        "object",
			Required:    []airbyte.PropertyName{"spreadsheet_id", "credentials"},
			Properties: airbyte.Properties{
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"spreadsheet_id": {
						Title:       "Spreadsheet Link",
						Description: "The ID or full URL of the Google Sheets spreadsheet",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"credentials": credentialsSpec(),
					"batch_size": {
						Title:       "Batch Size",
						Description: "Number of rows to fetch per batch request (default: 1000000)",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"names_conversion": {
						Title:       "Convert Column Names",
						Description: "Convert column names to snake_case",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
				},
			},
		},
	}, nil
}

// Check validates the configuration by attempting to access the spreadsheet.
func (s *GoogleSheetsSource) Check(srcCfgPath string, logTracker airbyte.LogTracker) error {
	var config Config
	if err := airbyte.UnmarshalFromPath(srcCfgPath, &config); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	config.SpreadsheetID = parseSpreadsheetID(config.SpreadsheetID)

	client, err := NewSheetsClient(context.Background(), config.Credentials, spreadsheetsReadonlyScope)
	if err != nil {
		return fmt.Errorf("failed to create sheets client: %w", err)
	}

	_, err = client.GetSpreadsheet(config.SpreadsheetID)
	if err != nil {
		return fmt.Errorf("failed to access spreadsheet: %w", err)
	}

	logTracker.Log(airbyte.LogLevelInfo, "Successfully connected to spreadsheet")
	return nil
}

// Discover returns a catalog with one stream per GRID sheet in the spreadsheet.
func (s *GoogleSheetsSource) Discover(srcConfigPath string, logTracker airbyte.LogTracker) (*airbyte.Catalog, error) {
	var config Config
	if err := airbyte.UnmarshalFromPath(srcConfigPath, &config); err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	config.SpreadsheetID = parseSpreadsheetID(config.SpreadsheetID)

	client, err := NewSheetsClient(context.Background(), config.Credentials, spreadsheetsReadonlyScope)
	if err != nil {
		return nil, fmt.Errorf("failed to create sheets client: %w", err)
	}

	spreadsheet, err := client.GetSpreadsheet(config.SpreadsheetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get spreadsheet: %w", err)
	}

	var streams []airbyte.Stream
	for _, sheet := range spreadsheet.Sheets {
		props := sheet.Properties
		if props.SheetType != "GRID" || props.GridProperties == nil || props.GridProperties.RowCount <= 0 {
			continue
		}

		headerRow, err := client.GetHeaders(config.SpreadsheetID, props.Title)
		if err != nil {
			logTracker.Log(airbyte.LogLevelWarn, fmt.Sprintf("Skipping sheet %q: %v", props.Title, err))
			continue
		}

		headers, err := parseHeaders(headerRow)
		if err != nil {
			logTracker.Log(airbyte.LogLevelWarn, fmt.Sprintf("Skipping sheet %q: %v", props.Title, err))
			continue
		}

		if config.NamesConversion {
			for i, h := range headers {
				headers[i] = toSnakeCase(h)
			}
		}

		schema := buildStreamSchema(headers)
		streams = append(streams, airbyte.Stream{
			Name:       props.Title,
			JSONSchema: schema,
			SupportedSyncModes: []airbyte.SyncMode{
				airbyte.SyncModeFullRefresh,
			},
		})
	}

	return &airbyte.Catalog{Streams: streams}, nil
}

// Read emits RECORD messages for each data row in configured streams.
func (s *GoogleSheetsSource) Read(
	sourceCfgPath string,
	prevStatePath string,
	configuredCat *airbyte.ConfiguredCatalog,
	tracker airbyte.MessageTracker,
) error {
	var config Config
	if err := airbyte.UnmarshalFromPath(sourceCfgPath, &config); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	config.SpreadsheetID = parseSpreadsheetID(config.SpreadsheetID)

	client, err := NewSheetsClient(context.Background(), config.Credentials, spreadsheetsReadonlyScope)
	if err != nil {
		return fmt.Errorf("failed to create sheets client: %w", err)
	}

	for _, cs := range configuredCat.Streams {
		streamName := cs.Stream.Name

		tracker.Log(airbyte.LogLevelInfo, fmt.Sprintf("Reading stream: %s", streamName))

		if err := s.readStream(client, config, streamName, tracker); err != nil {
			return fmt.Errorf("reading stream %q: %w", streamName, err)
		}

		tracker.Log(airbyte.LogLevelInfo, fmt.Sprintf("Completed stream: %s", streamName))
	}

	return nil
}

// readStream reads all data rows from a single sheet and emits records.
func (s *GoogleSheetsSource) readStream(
	client *SheetsClient,
	config Config,
	sheetTitle string,
	tracker airbyte.MessageTracker,
) error {
	headerRow, err := client.GetHeaders(config.SpreadsheetID, sheetTitle)
	if err != nil {
		return fmt.Errorf("reading headers: %w", err)
	}

	headers, err := parseHeaders(headerRow)
	if err != nil {
		return fmt.Errorf("parsing headers: %w", err)
	}

	if config.NamesConversion {
		for i, h := range headers {
			headers[i] = toSnakeCase(h)
		}
	}

	startRow := 2
	batchSize := config.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	for {
		endRow := startRow + batchSize - 1
		rows, err := client.BatchGetRows(config.SpreadsheetID, sheetTitle, startRow, endRow)
		if err != nil {
			return fmt.Errorf("reading rows %d-%d: %w", startRow, endRow, err)
		}

		if len(rows) == 0 {
			break
		}

		for _, row := range rows {
			record := mapRowToRecord(headers, row)
			if record == nil {
				continue
			}

			if err := tracker.Record(record, sheetTitle, ""); err != nil {
				return fmt.Errorf("emitting record: %w", err)
			}
		}

		if len(rows) < batchSize {
			break
		}

		startRow = endRow + 1
	}

	return nil
}

// mapRowToRecord maps a data row's values to header names.
// Returns nil if all values are empty.
func mapRowToRecord(headers []string, row []interface{}) map[string]interface{} {
	record := make(map[string]interface{}, len(headers))
	hasData := false

	for i, h := range headers {
		if i < len(row) {
			val := row[i]
			record[h] = val
			if val != nil && val != "" {
				hasData = true
			}
		} else {
			record[h] = nil
		}
	}

	if !hasData {
		return nil
	}

	return record
}

// buildStreamSchema creates a JSON schema where all fields are typed as nullable strings.
func buildStreamSchema(headers []string) airbyte.Properties {
	props := make(map[airbyte.PropertyName]airbyte.PropertySpec, len(headers))
	for _, h := range headers {
		props[airbyte.PropertyName(h)] = airbyte.PropertySpec{
			PropertyType: airbyte.PropertyType{
				Type: []airbyte.PropType{airbyte.Null, airbyte.String},
			},
		}
	}
	return airbyte.Properties{Properties: props}
}

// discoverGridSheets filters spreadsheet sheets to only GRID type with rows.
func discoverGridSheets(spreadsheet *sheets.Spreadsheet) []*sheets.SheetProperties {
	var result []*sheets.SheetProperties
	for _, sheet := range spreadsheet.Sheets {
		props := sheet.Properties
		if props.SheetType == "GRID" && props.GridProperties != nil && props.GridProperties.RowCount > 0 {
			result = append(result, props)
		}
	}
	return result
}
