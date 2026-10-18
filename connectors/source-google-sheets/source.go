package main

import (
	"context"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const spreadsheetsReadonlyScope = "https://www.googleapis.com/auth/spreadsheets.readonly"

// GoogleSheetsSource implements synclet.Source for Google Sheets.
type GoogleSheetsSource struct{}

// NewGoogleSheetsSource creates a new Google Sheets source connector.
func NewGoogleSheetsSource() *GoogleSheetsSource {
	return &GoogleSheetsSource{}
}

// Spec returns the connector specification defining config fields.
func (s *GoogleSheetsSource) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://docs.google.com/spreadsheets",
		SupportsIncremental: false,
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Google Sheets Source Spec",
			Description: "Reads data from a Google Sheets spreadsheet",
			Type:        "object",
			Required:    []synclet.PropertyName{"spreadsheet_id", "credentials"},
			Properties: synclet.Properties{
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"spreadsheet_id": {
						Title:       "Spreadsheet Link",
						Description: "The ID or full URL of the Google Sheets spreadsheet",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"credentials": credentialsSpec(),
					"batch_size": {
						Title:       "Batch Size",
						Description: "Number of rows to fetch per batch request (default: 1000000)",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"names_conversion": {
						Title:       "Convert Column Names",
						Description: "Convert column names to snake_case",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
				},
			},
		},
	}, nil
}

// Check validates the configuration by attempting to access the spreadsheet.
func (s *GoogleSheetsSource) Check(ctx context.Context, config Config) error {
	config.SpreadsheetID = parseSpreadsheetID(config.SpreadsheetID)

	client, err := NewSheetsClient(ctx, config.Credentials, spreadsheetsReadonlyScope)
	if err != nil {
		return fmt.Errorf("failed to create sheets client: %w", err)
	}

	_, err = client.GetSpreadsheet(ctx, config.SpreadsheetID)
	if err != nil {
		return fmt.Errorf("failed to access spreadsheet: %w", err)
	}

	synclet.Logger(ctx).Info("Successfully connected to spreadsheet")
	return nil
}

// Discover returns a catalog with one stream per GRID sheet in the spreadsheet.
func (s *GoogleSheetsSource) Discover(ctx context.Context, config Config) (*synclet.Catalog, error) {
	config.SpreadsheetID = parseSpreadsheetID(config.SpreadsheetID)
	log := synclet.Logger(ctx)

	client, err := NewSheetsClient(ctx, config.Credentials, spreadsheetsReadonlyScope)
	if err != nil {
		return nil, fmt.Errorf("failed to create sheets client: %w", err)
	}

	spreadsheet, err := client.GetSpreadsheet(ctx, config.SpreadsheetID)
	if err != nil {
		return nil, fmt.Errorf("failed to get spreadsheet: %w", err)
	}

	var streams []synclet.Stream
	for _, sheet := range spreadsheet.Sheets {
		props := sheet.Properties
		if props.SheetType != "GRID" || props.GridProperties == nil || props.GridProperties.RowCount <= 0 {
			continue
		}

		headerRow, err := client.GetHeaders(ctx, config.SpreadsheetID, props.Title)
		if err != nil {
			log.Warn(fmt.Sprintf("Skipping sheet %q: %v", props.Title, err))
			continue
		}

		headers, err := parseHeaders(headerRow)
		if err != nil {
			log.Warn(fmt.Sprintf("Skipping sheet %q: %v", props.Title, err))
			continue
		}

		if config.NamesConversion {
			for i, h := range headers {
				headers[i] = toSnakeCase(h)
			}
		}

		streams = append(streams, synclet.Stream{
			Name:       props.Title,
			JSONSchema: synclet.ObjectSchema(buildStreamSchema(headers)),
			SupportedSyncModes: []synclet.SyncMode{
				synclet.SyncModeFullRefresh,
			},
		})
	}

	return &synclet.Catalog{Streams: streams}, nil
}

// Read emits RECORD messages for each data row in configured streams.
func (s *GoogleSheetsSource) Read(
	ctx context.Context,
	config Config,
	configuredCat *synclet.ConfiguredCatalog,
	_ *synclet.InputState,
	em *synclet.Emitter,
) error {
	config.SpreadsheetID = parseSpreadsheetID(config.SpreadsheetID)
	log := synclet.Logger(ctx)

	client, err := NewSheetsClient(ctx, config.Credentials, spreadsheetsReadonlyScope)
	if err != nil {
		return fmt.Errorf("failed to create sheets client: %w", err)
	}

	for _, cs := range configuredCat.Streams {
		desc := cs.Stream.Descriptor()

		log.Info(fmt.Sprintf("Reading stream: %s", desc.Name))

		if err := s.readStream(ctx, client, config, desc, em); err != nil {
			return fmt.Errorf("reading stream %q: %w", desc.Name, err)
		}

		log.Info(fmt.Sprintf("Completed stream: %s", desc.Name))
	}

	return nil
}

// readStream reads all data rows from a single sheet and emits records.
func (s *GoogleSheetsSource) readStream(
	ctx context.Context,
	client *SheetsClient,
	config Config,
	desc synclet.StreamDescriptor,
	em *synclet.Emitter,
) error {
	sheetTitle := desc.Name

	headerRow, err := client.GetHeaders(ctx, config.SpreadsheetID, sheetTitle)
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
		rows, err := client.BatchGetRows(ctx, config.SpreadsheetID, sheetTitle, startRow, endRow)
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

			if err := em.Record(desc, record); err != nil {
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

// buildStreamSchema returns the stream's properties, every field typed as a nullable string.
func buildStreamSchema(headers []string) map[synclet.PropertyName]synclet.PropertySpec {
	props := make(map[synclet.PropertyName]synclet.PropertySpec, len(headers))
	for _, h := range headers {
		props[synclet.PropertyName(h)] = synclet.PropertySpec{
			PropertyType: synclet.PropertyType{
				Type: []synclet.PropType{synclet.Null, synclet.String},
			},
		}
	}
	return props
}
