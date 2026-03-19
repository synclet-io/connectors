package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
	"google.golang.org/api/sheets/v4"
)

const spreadsheetsScope = "https://www.googleapis.com/auth/spreadsheets"

// GoogleSheetsDestination implements airbyte.Destination for Google Sheets.
type GoogleSheetsDestination struct{}

// NewGoogleSheetsDestination creates a new Google Sheets destination connector.
func NewGoogleSheetsDestination() *GoogleSheetsDestination {
	return &GoogleSheetsDestination{}
}

// Spec returns the connector specification defining config fields.
func (d *GoogleSheetsDestination) Spec(_ airbyte.LogTracker) (*airbyte.ConnectorSpecification, error) {
	return &airbyte.ConnectorSpecification{
		DocumentationURL: "https://docs.google.com/spreadsheets",
		SupportedDestinationSyncModes: []airbyte.DestinationSyncMode{
			airbyte.DestinationSyncModeOverwrite,
			airbyte.DestinationSyncModeAppend,
		},
		ConnectionSpecification: airbyte.ConnectionSpecification{
			Title:       "Google Sheets Destination Spec",
			Description: "Writes data to a Google Sheets spreadsheet",
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
				},
			},
		},
	}, nil
}

// Check validates the configuration by attempting to access the spreadsheet.
func (d *GoogleSheetsDestination) Check(dstCfgPath string, logTracker airbyte.LogTracker) error {
	var config Config
	if err := airbyte.UnmarshalFromPath(dstCfgPath, &config); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	opt, err := createClientOption(config.Credentials, spreadsheetsScope)
	if err != nil {
		return fmt.Errorf("failed to create auth: %w", err)
	}

	srv, err := sheets.NewService(context.Background(), opt)
	if err != nil {
		return fmt.Errorf("failed to create sheets service: %w", err)
	}

	_, err = srv.Spreadsheets.Get(config.SpreadsheetID).
		IncludeGridData(false).
		Fields("spreadsheetId").
		Do()
	if err != nil {
		return fmt.Errorf("failed to access spreadsheet: %w", err)
	}

	logTracker.Log(airbyte.LogLevelInfo, "Successfully connected to spreadsheet")
	return nil
}

// Write receives Airbyte RECORD and STATE messages from inputReader and writes records to Google Sheets.
func (d *GoogleSheetsDestination) Write(dstCfgPath string, catalogPath string, inputReader io.Reader, tracker airbyte.MessageTracker) error {
	var config Config
	if err := airbyte.UnmarshalFromPath(dstCfgPath, &config); err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Parse configured catalog to get stream -> destination_sync_mode mapping
	var catalog airbyte.ConfiguredCatalog
	if err := airbyte.UnmarshalFromPath(catalogPath, &catalog); err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	syncModes := make(map[string]string)
	for _, cs := range catalog.Streams {
		mode := "append"
		if cs.DestinationSyncMode == airbyte.DestinationSyncModeOverwrite {
			mode = "overwrite"
		}
		syncModes[cs.Stream.Name] = mode
	}

	opt, err := createClientOption(config.Credentials, spreadsheetsScope)
	if err != nil {
		return fmt.Errorf("failed to create auth: %w", err)
	}

	srv, err := sheets.NewService(context.Background(), opt)
	if err != nil {
		return fmt.Errorf("failed to create sheets service: %w", err)
	}

	writer := NewSheetWriter(srv, config.SpreadsheetID)

	scanner := bufio.NewScanner(inputReader)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024) // 1MB buffer

	for scanner.Scan() {
		line := scanner.Bytes()

		var msg struct {
			Type   string          `json:"type"`
			Record json.RawMessage `json:"record,omitempty"`
			State  json.RawMessage `json:"state,omitempty"`
		}
		if err := json.Unmarshal(line, &msg); err != nil {
			continue
		}

		switch msg.Type {
		case "RECORD":
			var rec struct {
				Stream string                 `json:"stream"`
				Data   map[string]interface{} `json:"data"`
			}
			if err := json.Unmarshal(msg.Record, &rec); err != nil {
				continue
			}

			syncMode := syncModes[rec.Stream]
			if syncMode == "" {
				syncMode = "append"
			}

			if err := writer.AddRecord(rec.Stream, rec.Data, syncMode); err != nil {
				return fmt.Errorf("adding record for stream %q: %w", rec.Stream, err)
			}

		case "STATE":
			// Re-emit state to confirm committed records
			var stateMsg struct {
				Type   string          `json:"type"`
				Stream json.RawMessage `json:"stream,omitempty"`
				Global json.RawMessage `json:"global,omitempty"`
				Data   json.RawMessage `json:"data,omitempty"`
			}
			if err := json.Unmarshal(msg.State, &stateMsg); err != nil {
				continue
			}

			if stateMsg.Type == "STREAM" && stateMsg.Stream != nil {
				var streamState airbyte.StreamState
				if err := json.Unmarshal(stateMsg.Stream, &streamState); err != nil {
					continue
				}
				if err := tracker.State(airbyte.StateTypeStream, &streamState); err != nil {
					return fmt.Errorf("emitting state confirmation: %w", err)
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("reading input: %w", err)
	}

	// Flush remaining buffered rows
	if err := writer.FlushAll(); err != nil {
		return fmt.Errorf("flushing remaining records: %w", err)
	}

	return nil
}
