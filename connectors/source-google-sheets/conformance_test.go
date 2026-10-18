package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"google.golang.org/api/option"
)

const fakeSpreadsheetID = "1AbCdEfGhIjKlMnOpQrStUvWxYz0123456789"

// fakeSheetsAPI serves one spreadsheet with a "People" grid sheet and a
// "Chart" sheet; any other spreadsheet ID is not found.
func fakeSheetsAPI(t *testing.T) *httptest.Server {
	t.Helper()
	rows := [][]any{{"Alice", "30", "alice@example.com"}, {"Bob", "", "bob@example.com"}, {"", "", ""}, {"Carol", "41"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prefix := "/v4/spreadsheets/" + fakeSpreadsheetID
		if !strings.HasPrefix(r.URL.Path, prefix) {
			http.Error(w, `{"error":{"code":404,"message":"Requested entity was not found.","status":"NOT_FOUND"}}`, http.StatusNotFound)
			return
		}
		var body any
		switch rest := strings.TrimPrefix(r.URL.Path, prefix); {
		case rest == "":
			body = map[string]any{"sheets": []any{
				map[string]any{"properties": map[string]any{"title": "People", "sheetType": "GRID", "gridProperties": map[string]any{"rowCount": 5}}},
				map[string]any{"properties": map[string]any{"title": "Chart", "sheetType": "OBJECT"}},
			}}
		case rest == "/values:batchGet":
			body = map[string]any{"valueRanges": []any{map[string]any{"values": rows}}}
		case strings.HasPrefix(rest, "/values/"):
			body = map[string]any{"values": [][]any{{"Full Name", "Age", "E-mail"}}}
		default:
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestConformance(t *testing.T) {
	srv := fakeSheetsAPI(t)
	src := &GoogleSheetsSource{clientOptions: func(context.Context, CredentialsConfig, string) ([]option.ClientOption, error) {
		return []option.ClientOption{option.WithEndpoint(srv.URL), option.WithoutAuthentication()}, nil
	}}
	config := json.RawMessage(`{"spreadsheet_id":"https://docs.google.com/spreadsheets/d/` + fakeSpreadsheetID + `/edit","credentials":{"auth_type":"Client"},"names_conversion":true}`)

	out, err := testkit.RunSource(t.Context(), src, synclet.Request{Command: synclet.CommandDiscover, Config: config})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	require.Len(t, out.Catalog().Streams, 1)

	testkit.RunSourceSuite(t, src, testkit.SourceSuite{
		Config:       config,
		BrokenConfig: json.RawMessage(`{"spreadsheet_id":"missing","credentials":{"auth_type":"Client"}}`),
		Catalog: &synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{{
			Stream:              out.Catalog().Streams[0],
			SyncMode:            synclet.SyncModeFullRefresh,
			DestinationSyncMode: synclet.DestinationSyncModeOverwrite,
		}}},
	})
}
