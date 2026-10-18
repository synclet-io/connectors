package main

import (
	"context"
	"fmt"

	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

// SheetsClient wraps the Google Sheets API service for connector operations.
type SheetsClient struct {
	srv *sheets.Service
}

// NewSheetsClient creates a Google Sheets API client; opts carry its
// authentication.
func NewSheetsClient(ctx context.Context, opts ...option.ClientOption) (*SheetsClient, error) {
	srv, err := sheets.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating sheets service: %w", err)
	}

	return &SheetsClient{srv: srv}, nil
}

// GetSpreadsheet retrieves spreadsheet metadata (sheet properties only).
func (c *SheetsClient) GetSpreadsheet(ctx context.Context, spreadsheetID string) (*sheets.Spreadsheet, error) {
	resp, err := c.srv.Spreadsheets.Get(spreadsheetID).
		IncludeGridData(false).
		Fields("sheets.properties").
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("getting spreadsheet: %w", err)
	}
	return resp, nil
}

// GetHeaders reads the first row (header row) of the given sheet.
func (c *SheetsClient) GetHeaders(ctx context.Context, spreadsheetID, sheetTitle string) ([]interface{}, error) {
	rangeStr := fmt.Sprintf("'%s'!1:1", sheetTitle)
	resp, err := c.srv.Spreadsheets.Values.Get(spreadsheetID, rangeStr).
		MajorDimension("ROWS").
		ValueRenderOption("FORMATTED_VALUE").
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("reading headers for sheet %q: %w", sheetTitle, err)
	}

	if len(resp.Values) == 0 || len(resp.Values[0]) == 0 {
		return nil, fmt.Errorf("no headers found in sheet %q", sheetTitle)
	}

	return resp.Values[0], nil
}

// BatchGetRows reads rows in the given range from the sheet using BatchGet.
func (c *SheetsClient) BatchGetRows(ctx context.Context, spreadsheetID, sheetTitle string, startRow, endRow int) ([][]interface{}, error) {
	rangeStr := fmt.Sprintf("'%s'!%d:%d", sheetTitle, startRow, endRow)
	resp, err := c.srv.Spreadsheets.Values.BatchGet(spreadsheetID).
		Ranges(rangeStr).
		MajorDimension("ROWS").
		ValueRenderOption("FORMATTED_VALUE").
		Context(ctx).
		Do()
	if err != nil {
		return nil, fmt.Errorf("reading data rows for sheet %q: %w", sheetTitle, err)
	}

	if len(resp.ValueRanges) == 0 {
		return nil, nil
	}
	return resp.ValueRanges[0].Values, nil
}
