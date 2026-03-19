//go:build integration

package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	airbyte "github.com/saturn4er/airbyte-go-sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	projectID := os.Getenv("BIGQUERY_PROJECT_ID")
	if projectID == "" {
		t.Skip("BIGQUERY_PROJECT_ID not set, skipping integration test")
	}
	datasetID := os.Getenv("BIGQUERY_DATASET_ID")
	if datasetID == "" {
		datasetID = "integration_test"
	}
	location := os.Getenv("BIGQUERY_DATASET_LOCATION")
	if location == "" {
		location = "US"
	}
	return Config{
		ProjectID:       projectID,
		DatasetLocation: location,
		DatasetID:       datasetID,
		Credentials:     CredentialsConfig{AuthType: "ApplicationDefault"},
		RawDataDataset:  "airbyte_internal",
		CDCDeletionMode: "hard_delete",
	}
}

func writeConfigFile(t *testing.T, cfg Config) string {
	t.Helper()
	data, err := json.Marshal(cfg)
	require.NoError(t, err)

	f, err := os.CreateTemp(t.TempDir(), "bq-config-*.json")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func writeCatalogFile(t *testing.T, streams []catalogStream) string {
	t.Helper()
	catalog := rawConfiguredCatalog{Streams: streams}
	data, err := json.Marshal(catalog)
	require.NoError(t, err)

	f, err := os.CreateTemp(t.TempDir(), "bq-catalog-*.json")
	require.NoError(t, err)
	_, err = f.Write(data)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

func noopLogTracker() airbyte.LogTracker {
	return airbyte.LogTracker{
		Log: func(_ airbyte.LogLevel, _ string) error { return nil },
	}
}

func noopMessageTracker() airbyte.MessageTracker {
	return airbyte.MessageTracker{
		State:   func(_ airbyte.StateType, _ interface{}) error { return nil },
		Record:  func(_ interface{}, _ string, _ string) error { return nil },
		Log:     func(_ airbyte.LogLevel, _ string) error { return nil },
		Trace:   func(_ interface{}) error { return nil },
		Control: func(_ interface{}) error { return nil },
	}
}

func TestSpec(t *testing.T) {
	dest := NewBigQueryDestination()
	spec, err := dest.Spec(noopLogTracker())
	require.NoError(t, err)
	require.NotNil(t, spec)

	assert.Len(t, spec.SupportedDestinationSyncModes, 3, "expected 3 sync modes: overwrite, append, append_dedup")
	assert.NotNil(t, spec.ConnectionSpecification, "connection spec should not be nil")

	// Verify required config fields exist in the spec.
	specJSON, err := json.Marshal(spec.ConnectionSpecification)
	require.NoError(t, err)
	specStr := string(specJSON)
	assert.Contains(t, specStr, "project_id")
	assert.Contains(t, specStr, "dataset_id")
	assert.Contains(t, specStr, "dataset_location")
	assert.Contains(t, specStr, "credentials")
}

func TestCheckWithADC(t *testing.T) {
	cfg := testConfig(t)
	cfgPath := writeConfigFile(t, cfg)

	dest := NewBigQueryDestination()
	err := dest.Check(cfgPath, noopLogTracker())
	assert.NoError(t, err)
}

func TestWriteAppend(t *testing.T) {
	cfg := testConfig(t)
	cfgPath := writeConfigFile(t, cfg)

	streams := []catalogStream{{
		Stream: streamDef{
			Name:      "test_append",
			Namespace: cfg.DatasetID,
			JSONSchema: map[string]interface{}{
				"properties": map[string]interface{}{
					"id":   map[string]interface{}{"type": "integer"},
					"name": map[string]interface{}{"type": "string"},
				},
			},
		},
		DestinationSyncMode: "append",
	}}
	catalogPath := writeCatalogFile(t, streams)

	input := strings.NewReader(
		`{"type":"RECORD","record":{"stream":"test_append","namespace":"` + cfg.DatasetID + `","data":{"id":1,"name":"Alice"}}}` + "\n" +
			`{"type":"RECORD","record":{"stream":"test_append","namespace":"` + cfg.DatasetID + `","data":{"id":2,"name":"Bob"}}}` + "\n" +
			`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"test_append","namespace":"` + cfg.DatasetID + `"},"stream_state":{"cursor":"2"}}}}` + "\n",
	)

	dest := NewBigQueryDestination()
	tracker := noopMessageTracker()
	err := dest.Write(cfgPath, catalogPath, input, tracker)
	assert.NoError(t, err)
}

func TestWriteOverwrite(t *testing.T) {
	cfg := testConfig(t)
	cfgPath := writeConfigFile(t, cfg)

	streams := []catalogStream{{
		Stream: streamDef{
			Name:      "test_overwrite",
			Namespace: cfg.DatasetID,
			JSONSchema: map[string]interface{}{
				"properties": map[string]interface{}{
					"id":    map[string]interface{}{"type": "integer"},
					"value": map[string]interface{}{"type": "string"},
				},
			},
		},
		DestinationSyncMode: "overwrite",
	}}
	catalogPath := writeCatalogFile(t, streams)

	input := strings.NewReader(
		`{"type":"RECORD","record":{"stream":"test_overwrite","namespace":"` + cfg.DatasetID + `","data":{"id":1,"value":"new_data"}}}` + "\n" +
			`{"type":"STATE","state":{"data":{"cursor":"done"}}}` + "\n",
	)

	dest := NewBigQueryDestination()
	tracker := noopMessageTracker()
	err := dest.Write(cfgPath, catalogPath, input, tracker)
	assert.NoError(t, err)
}

func TestWriteAppendDedup(t *testing.T) {
	cfg := testConfig(t)
	cfgPath := writeConfigFile(t, cfg)

	streams := []catalogStream{{
		Stream: streamDef{
			Name:      "test_dedup",
			Namespace: cfg.DatasetID,
			JSONSchema: map[string]interface{}{
				"properties": map[string]interface{}{
					"id":   map[string]interface{}{"type": "integer"},
					"name": map[string]interface{}{"type": "string"},
				},
			},
		},
		DestinationSyncMode: "append_dedup",
		PrimaryKey:          [][]string{{"id"}},
		CursorField:         []string{"id"},
	}}
	catalogPath := writeCatalogFile(t, streams)

	input := strings.NewReader(
		`{"type":"RECORD","record":{"stream":"test_dedup","namespace":"` + cfg.DatasetID + `","data":{"id":1,"name":"Alice"}}}` + "\n" +
			`{"type":"RECORD","record":{"stream":"test_dedup","namespace":"` + cfg.DatasetID + `","data":{"id":1,"name":"Alice Updated"}}}` + "\n" +
			`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"test_dedup","namespace":"` + cfg.DatasetID + `"},"stream_state":{"cursor":"2"}}}}` + "\n",
	)

	dest := NewBigQueryDestination()
	tracker := noopMessageTracker()
	err := dest.Write(cfgPath, catalogPath, input, tracker)
	assert.NoError(t, err)
}
