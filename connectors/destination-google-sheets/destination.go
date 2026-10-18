package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"google.golang.org/api/sheets/v4"
)

const spreadsheetsScope = "https://www.googleapis.com/auth/spreadsheets"

// GoogleSheetsDestination implements synclet.Destination for Google Sheets.
type GoogleSheetsDestination struct{}

// NewGoogleSheetsDestination creates a new Google Sheets destination connector.
func NewGoogleSheetsDestination() *GoogleSheetsDestination {
	return &GoogleSheetsDestination{}
}

// Spec returns the connector specification defining config fields.
func (d *GoogleSheetsDestination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://docs.google.com/spreadsheets",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Google Sheets Destination Spec",
			Description: "Writes data to a Google Sheets spreadsheet",
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
				},
			},
		},
	}, nil
}

// Check validates the configuration by attempting to access the spreadsheet.
func (d *GoogleSheetsDestination) Check(ctx context.Context, config Config) error {
	opt, err := createClientOption(ctx, config.Credentials, spreadsheetsScope)
	if err != nil {
		return fmt.Errorf("failed to create auth: %w", err)
	}

	srv, err := sheets.NewService(ctx, opt)
	if err != nil {
		return fmt.Errorf("failed to create sheets service: %w", err)
	}

	_, err = srv.Spreadsheets.Get(config.SpreadsheetID).
		IncludeGridData(false).
		Fields("spreadsheetId").
		Context(ctx).
		Do()
	if err != nil {
		return fmt.Errorf("failed to access spreadsheet: %w", err)
	}

	synclet.Logger(ctx).Info("Successfully connected to spreadsheet")

	return nil
}

// Write reads RECORD and STATE messages from in and writes records to Google Sheets.
func (d *GoogleSheetsDestination) Write(
	ctx context.Context,
	config Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) error {
	syncModes := make(map[string]string)
	for _, cs := range catalog.Streams {
		mode := "append"
		if cs.DestinationSyncMode == synclet.DestinationSyncModeOverwrite {
			mode = "overwrite"
		}
		syncModes[cs.Stream.Name] = mode
	}

	opt, err := createClientOption(ctx, config.Credentials, spreadsheetsScope)
	if err != nil {
		return fmt.Errorf("failed to create auth: %w", err)
	}

	srv, err := sheets.NewService(ctx, opt)
	if err != nil {
		return fmt.Errorf("failed to create sheets service: %w", err)
	}

	return writeMessages(ctx, NewSheetWriter(srv, config.SpreadsheetID), syncModes, in, em)
}

// writeMessages adds every record of in to writer and echoes STREAM states;
// it flushes the remaining buffered rows at the end of the input.
func writeMessages(ctx context.Context, writer *SheetWriter, syncModes map[string]string, in *synclet.MessageReader, em *synclet.Emitter) error {
	for {
		msg, err := in.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}

		switch msg.Type {
		case synclet.MessageTypeRecord:
			if msg.Record == nil {
				continue
			}
			var data map[string]interface{}
			dec := json.NewDecoder(bytes.NewReader(msg.Record.Data))
			dec.UseNumber()
			if err := dec.Decode(&data); err != nil {
				continue
			}

			syncMode := syncModes[msg.Record.Stream]
			if syncMode == "" {
				syncMode = "append"
			}

			if err := writer.AddRecord(ctx, msg.Record.Stream, data, syncMode); err != nil {
				return fmt.Errorf("adding record for stream %q: %w", msg.Record.Stream, err)
			}

		case synclet.MessageTypeState:
			if msg.State == nil || msg.State.Type != synclet.StateTypeStream || msg.State.Stream == nil {
				continue
			}
			if err := em.State(synclet.StateMessage{Type: synclet.StateTypeStream, Stream: msg.State.Stream}); err != nil {
				return fmt.Errorf("emitting state confirmation: %w", err)
			}
		}
	}

	if err := writer.FlushAll(ctx); err != nil {
		return fmt.Errorf("flushing remaining records: %w", err)
	}

	return nil
}
