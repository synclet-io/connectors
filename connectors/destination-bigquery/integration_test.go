//go:build integration

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func integrationConfig(t *testing.T) Config {
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

func configJSON(t *testing.T, cfg Config) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(cfg)
	require.NoError(t, err)
	return data
}

func catalogJSON(t *testing.T, streams []catalogStream) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(struct {
		Streams []catalogStream `json:"streams"`
	}{Streams: streams})
	require.NoError(t, err)
	return data
}

// serve runs one command of the destination and returns the messages it wrote.
func serve(t *testing.T, req synclet.Request) ([]synclet.Message, error) {
	t.Helper()
	var out bytes.Buffer
	err := synclet.ServeDestination(t.Context(), NewBigQueryDestination(), req, &out)
	var msgs []synclet.Message
	for _, line := range bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var msg synclet.Message
		require.NoError(t, json.Unmarshal(line, &msg))
		msgs = append(msgs, msg)
	}
	return msgs, err
}

func TestSpec(t *testing.T) {
	dest := NewBigQueryDestination()
	spec, err := dest.Spec(t.Context())
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
	cfg := integrationConfig(t)
	msgs, err := serve(t, synclet.Request{Command: synclet.CommandCheck, Config: configJSON(t, cfg)})
	require.NoError(t, err)
	last := msgs[len(msgs)-1]
	require.Equal(t, synclet.MessageTypeConnectionStatus, last.Type)
	assert.Equal(t, synclet.ConnectionStatusSucceeded, last.ConnectionStatus.Status, last.ConnectionStatus.Message)
}

func TestWriteAppend(t *testing.T) {
	cfg := integrationConfig(t)
	config := configJSON(t, cfg)

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
	catalog := catalogJSON(t, streams)

	input := strings.NewReader(
		`{"type":"RECORD","record":{"stream":"test_append","namespace":"` + cfg.DatasetID + `","data":{"id":1,"name":"Alice"}}}` + "\n" +
			`{"type":"RECORD","record":{"stream":"test_append","namespace":"` + cfg.DatasetID + `","data":{"id":2,"name":"Bob"}}}` + "\n" +
			`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"test_append","namespace":"` + cfg.DatasetID + `"},"stream_state":{"cursor":"2"}}}}` + "\n",
	)

	_, err := serve(t, synclet.Request{Command: synclet.CommandWrite, Config: config, Catalog: catalog, Input: input})
	assert.NoError(t, err)
}

func TestWriteOverwrite(t *testing.T) {
	cfg := integrationConfig(t)
	config := configJSON(t, cfg)

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
	catalog := catalogJSON(t, streams)

	input := strings.NewReader(
		`{"type":"RECORD","record":{"stream":"test_overwrite","namespace":"` + cfg.DatasetID + `","data":{"id":1,"value":"new_data"}}}` + "\n" +
			`{"type":"STATE","state":{"data":{"cursor":"done"}}}` + "\n",
	)

	_, err := serve(t, synclet.Request{Command: synclet.CommandWrite, Config: config, Catalog: catalog, Input: input})
	assert.NoError(t, err)
}

func TestWriteAppendDedup(t *testing.T) {
	cfg := integrationConfig(t)
	config := configJSON(t, cfg)

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
	catalog := catalogJSON(t, streams)

	input := strings.NewReader(
		`{"type":"RECORD","record":{"stream":"test_dedup","namespace":"` + cfg.DatasetID + `","data":{"id":1,"name":"Alice"}}}` + "\n" +
			`{"type":"RECORD","record":{"stream":"test_dedup","namespace":"` + cfg.DatasetID + `","data":{"id":1,"name":"Alice Updated"}}}` + "\n" +
			`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":"test_dedup","namespace":"` + cfg.DatasetID + `"},"stream_state":{"cursor":"2"}}}}` + "\n",
	)

	_, err := serve(t, synclet.Request{Command: synclet.CommandWrite, Config: config, Catalog: catalog, Input: input})
	assert.NoError(t, err)
}
