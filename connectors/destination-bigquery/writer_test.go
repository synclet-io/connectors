package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	airbyte "github.com/saturn4er/airbyte-go-sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockOps implements bigqueryOps for testing, tracking all calls.
type mockOps struct {
	ensuredDatasets []string
	ensureErr       error

	createTableCalls []createTableCall
	tableExistsResults    map[string]bool
	getTableSchemaResults map[string]bigquery.Schema
	getTableMetadataResults map[string]*bigquery.TableMetadata
	dropTableCalls   []dropTableCall
	copyTableCalls   []copyTableCall
	executeQueryCalls []string
}

type createTableCall struct {
	datasetID string
	tableName string
	schema    bigquery.Schema
}

type dropTableCall struct {
	datasetID string
	tableName string
}

type copyTableCall struct {
	srcDataset string
	srcTable   string
	dstDataset string
	dstTable   string
}

func newMockOps() *mockOps {
	return &mockOps{
		tableExistsResults:      make(map[string]bool),
		getTableSchemaResults:   make(map[string]bigquery.Schema),
		getTableMetadataResults: make(map[string]*bigquery.TableMetadata),
	}
}

func (m *mockOps) ensureDataset(_ context.Context, datasetID, _ string) error {
	m.ensuredDatasets = append(m.ensuredDatasets, datasetID)
	return m.ensureErr
}

func (m *mockOps) createTable(_ context.Context, datasetID, tableName string, schema bigquery.Schema, _ *bigquery.TimePartitioning, _ *bigquery.Clustering) error {
	m.createTableCalls = append(m.createTableCalls, createTableCall{datasetID, tableName, schema})
	return nil
}

func (m *mockOps) tableExists(_ context.Context, datasetID, tableName string) (bool, error) {
	return m.tableExistsResults[datasetID+"."+tableName], nil
}

func (m *mockOps) getTableSchema(_ context.Context, datasetID, tableName string) (bigquery.Schema, error) {
	return m.getTableSchemaResults[datasetID+"."+tableName], nil
}

func (m *mockOps) getTableMetadata(_ context.Context, datasetID, tableName string) (*bigquery.TableMetadata, error) {
	md := m.getTableMetadataResults[datasetID+"."+tableName]
	if md == nil {
		md = &bigquery.TableMetadata{}
	}
	return md, nil
}

func (m *mockOps) dropTable(_ context.Context, datasetID, tableName string) error {
	m.dropTableCalls = append(m.dropTableCalls, dropTableCall{datasetID, tableName})
	return nil
}

func (m *mockOps) copyTable(_ context.Context, srcDataset, srcTable, dstDataset, dstTable string) error {
	m.copyTableCalls = append(m.copyTableCalls, copyTableCall{srcDataset, srcTable, dstDataset, dstTable})
	return nil
}

func (m *mockOps) executeQuery(_ context.Context, sql string) error {
	m.executeQueryCalls = append(m.executeQueryCalls, sql)
	return nil
}

// mockLoader implements loader for testing, tracking dataset and table per call.
type mockLoader struct {
	loadCalls     []loadCall
	loadCallCount int
	loadErr       error
}

type loadCall struct {
	datasetID string
	tableName string
	schema    bigquery.Schema
	records   []map[string]interface{}
}

func (m *mockLoader) load(_ context.Context, datasetID, tableName string, schema bigquery.Schema, records []map[string]interface{}) error {
	m.loadCallCount++
	// Deep copy records to avoid mutation issues.
	copied := make([]map[string]interface{}, len(records))
	for i, rec := range records {
		c := make(map[string]interface{})
		for k, v := range rec {
			c[k] = v
		}
		copied[i] = c
	}
	m.loadCalls = append(m.loadCalls, loadCall{datasetID, tableName, schema, copied})
	return m.loadErr
}

func (m *mockLoader) close() error {
	return nil
}

// mockTracker creates a MessageTracker that captures state emissions.
type stateCapture struct {
	stateType airbyte.StateType
	stateData interface{}
}

func newMockTracker() (airbyte.MessageTracker, *[]stateCapture) {
	captures := &[]stateCapture{}
	return airbyte.MessageTracker{
		State: func(sType airbyte.StateType, stateData interface{}) error {
			*captures = append(*captures, stateCapture{stateType: sType, stateData: stateData})
			return nil
		},
		Log: func(_ airbyte.LogLevel, _ string) error {
			return nil
		},
		Record: func(_ interface{}, _ string, _ string) error {
			return nil
		},
	}, captures
}

func testConfig() *Config {
	return &Config{
		ProjectID:       "test-project",
		DatasetID:       "test_dataset",
		DatasetLocation: "US",
		CDCDeletionMode: "hard_delete",
		RawDataDataset:  "airbyte_internal",
	}
}

func testCatalog() []catalogStream {
	return []catalogStream{
		{
			Stream: streamDef{
				Name: "users",
				JSONSchema: map[string]interface{}{
					"properties": map[string]interface{}{
						"id":   map[string]interface{}{"type": "integer"},
						"name": map[string]interface{}{"type": "string"},
						"age":  map[string]interface{}{"type": "integer"},
					},
				},
			},
			DestinationSyncMode: "append",
			PrimaryKey:          [][]string{{"id"}},
		},
	}
}

func testCatalogWithMode(name, namespace, destMode string, pk [][]string, cursor []string, schema map[string]interface{}) []catalogStream {
	return []catalogStream{
		{
			Stream: streamDef{
				Name:       name,
				Namespace:  namespace,
				JSONSchema: schema,
			},
			DestinationSyncMode: destMode,
			PrimaryKey:          pk,
			CursorField:         cursor,
		},
	}
}

func simpleSchema() map[string]interface{} {
	return map[string]interface{}{
		"properties": map[string]interface{}{
			"id":   map[string]interface{}{"type": "integer"},
			"name": map[string]interface{}{"type": "string"},
		},
	}
}

func cdcSchema() map[string]interface{} {
	return map[string]interface{}{
		"properties": map[string]interface{}{
			"id":                 map[string]interface{}{"type": "integer"},
			"name":               map[string]interface{}{"type": "string"},
			"_ab_cdc_deleted_at": map[string]interface{}{"type": "string", "format": "date-time"},
			"_ab_cdc_updated_at": map[string]interface{}{"type": "string", "format": "date-time"},
			"_ab_cdc_log_pos":    map[string]interface{}{"type": "integer"},
		},
	}
}

// --- Existing tests (updated to use new mock fields) ---

func TestAddRecordBuffering(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()
	catalog := testCatalog()

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)
	w.batchSize = 3

	// Add 2 records (below threshold) -- no flush should happen.
	err := w.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice", "age": float64(30)})
	require.NoError(t, err)

	err = w.AddRecord("users", map[string]interface{}{"id": float64(2), "name": "Bob", "age": float64(25)})
	require.NoError(t, err)

	assert.Equal(t, 0, ldr.loadCallCount, "should not flush below threshold")
	assert.Len(t, w.buffers["users"].records, 2)

	// Add 1 more to hit threshold -- should auto-flush.
	err = w.AddRecord("users", map[string]interface{}{"id": float64(3), "name": "Charlie", "age": float64(35)})
	require.NoError(t, err)

	assert.Equal(t, 1, ldr.loadCallCount, "should flush at threshold")
	assert.Len(t, w.buffers["users"].records, 0, "buffer should be cleared after flush")
	assert.Len(t, ldr.loadCalls[0].records, 3, "should have loaded 3 records")
}

func TestFlushAllEmitsState(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()
	catalog := testCatalog()

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)

	err := w.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice", "age": float64(30)})
	require.NoError(t, err)

	stateJSON := json.RawMessage(`{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":"2024-01-01"}}}`)
	w.QueueState(stateJSON)

	tracker, captures := newMockTracker()
	err = w.FlushAll(tracker)
	require.NoError(t, err)

	assert.Equal(t, 1, ldr.loadCallCount)
	assert.Len(t, *captures, 1, "should emit 1 state after flush")
	assert.Equal(t, airbyte.StateTypeStream, (*captures)[0].stateType)
}

func TestStateNotEmittedBeforeFlush(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()
	catalog := testCatalog()

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)

	w.QueueState(json.RawMessage(`{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":"1"}}}`))
	w.QueueState(json.RawMessage(`{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":"2"}}}`))

	assert.Len(t, w.pendingStates, 2)

	_, captures := newMockTracker()
	assert.Len(t, *captures, 0, "no states emitted before flush")
}

func TestMetadataFieldsAdded(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()
	catalog := testCatalog()

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)
	w.batchSize = 1

	err := w.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice", "age": float64(30)})
	require.NoError(t, err)

	require.Equal(t, 1, ldr.loadCallCount)
	require.Len(t, ldr.loadCalls, 1)
	require.Len(t, ldr.loadCalls[0].records, 1)

	rec := ldr.loadCalls[0].records[0]

	assert.Contains(t, rec, "_airbyte_raw_id")
	assert.Contains(t, rec, "_airbyte_extracted_at")
	assert.Contains(t, rec, "_airbyte_meta")
	assert.Contains(t, rec, "_airbyte_generation_id")

	rawID, ok := rec["_airbyte_raw_id"].(string)
	assert.True(t, ok)
	assert.Len(t, rawID, 36, "should be a UUID")

	metaStr, ok := rec["_airbyte_meta"].(string)
	assert.True(t, ok)
	var meta map[string]interface{}
	err = json.Unmarshal([]byte(metaStr), &meta)
	require.NoError(t, err)
	assert.Contains(t, meta, "errors")
}

func TestValidationErrorsInMeta(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()
	catalog := testCatalog()

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)
	w.batchSize = 1

	err := w.AddRecord("users", map[string]interface{}{"id": float64(1e19), "name": "Overflow", "age": float64(25)})
	require.NoError(t, err)

	require.Equal(t, 1, ldr.loadCallCount)
	rec := ldr.loadCalls[0].records[0]

	metaStr, ok := rec["_airbyte_meta"].(string)
	assert.True(t, ok)
	var meta map[string]interface{}
	err = json.Unmarshal([]byte(metaStr), &meta)
	require.NoError(t, err)

	errors, ok := meta["errors"].([]interface{})
	assert.True(t, ok, "errors should be an array")
	assert.NotEmpty(t, errors, "should have validation errors")

	firstErr, ok := errors[0].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "id", firstErr["field"])
	assert.Equal(t, "DESTINATION_FIELD_SIZE_LIMITATION", firstErr["message"])
}

func TestWriterNamespaceResolution(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()

	catalog := []catalogStream{
		{
			Stream: streamDef{
				Name:      "orders",
				Namespace: "custom_dataset",
				JSONSchema: map[string]interface{}{
					"properties": map[string]interface{}{
						"id": map[string]interface{}{"type": "integer"},
					},
				},
			},
			DestinationSyncMode: "append",
		},
	}

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)

	sc := w.streams["custom_dataset.orders"]
	require.NotNil(t, sc)
	assert.Equal(t, "custom_dataset", sc.datasetID, "should use namespace as dataset")
}

func TestWriterCDCDetection(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()

	catalog := []catalogStream{
		{
			Stream: streamDef{
				Name: "cdc_stream",
				JSONSchema: map[string]interface{}{
					"properties": map[string]interface{}{
						"id":                 map[string]interface{}{"type": "integer"},
						"_ab_cdc_deleted_at": map[string]interface{}{"type": "string"},
						"_ab_cdc_updated_at": map[string]interface{}{"type": "string"},
					},
				},
			},
			DestinationSyncMode: "append_dedup",
		},
	}

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)

	sc := w.streams["cdc_stream"]
	require.NotNil(t, sc)
	assert.True(t, sc.cdcEnabled, "should detect CDC from _ab_cdc_deleted_at field")
}

func TestWriterUnknownStreamError(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()

	w := NewBigQueryWriter(ops, ldr, cfg, nil)

	err := w.AddRecord("nonexistent", map[string]interface{}{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown stream")
}

func TestFlushAllWithMultipleStates(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	cfg := testConfig()
	catalog := testCatalog()

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)

	err := w.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice", "age": float64(30)})
	require.NoError(t, err)

	w.QueueState(json.RawMessage(`{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":"1"}}}`))
	w.QueueState(json.RawMessage(`{"type":"LEGACY","data":{"position":42}}`))

	tracker, captures := newMockTracker()
	err = w.FlushAll(tracker)
	require.NoError(t, err)

	assert.Len(t, *captures, 2)
	assert.Equal(t, airbyte.StateTypeStream, (*captures)[0].stateType)
	assert.Equal(t, airbyte.StateTypeLegacy, (*captures)[1].stateType)

	assert.Nil(t, w.pendingStates)
}

func TestFlushAllFailureDoesNotEmitState(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{loadErr: fmt.Errorf("BQ load error")}
	cfg := testConfig()
	catalog := testCatalog()

	w := NewBigQueryWriter(ops, ldr, cfg, catalog)

	err := w.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice", "age": float64(30)})
	require.NoError(t, err)

	w.QueueState(json.RawMessage(`{"type":"STREAM","stream":{"stream_descriptor":{"name":"users"},"stream_state":{"cursor":"1"}}}`))

	tracker, captures := newMockTracker()
	err = w.FlushAll(tracker)
	assert.Error(t, err, "should return error on load failure")

	assert.Len(t, *captures, 0, "no state emitted on flush failure")
}

func TestStreamKeyFunction(t *testing.T) {
	assert.Equal(t, "ns.table", streamKey("ns", "table"))
	assert.Equal(t, "table", streamKey("", "table"))
}

// --- New sync mode tests ---

func TestOverwriteMode(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	config := testConfig()
	catalog := testCatalogWithMode("users", "", "overwrite", nil, nil, simpleSchema())

	writer := NewBigQueryWriter(ops, ldr, config, catalog)

	err := writer.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice"})
	require.NoError(t, err)
	err = writer.AddRecord("users", map[string]interface{}{"id": float64(2), "name": "Bob"})
	require.NoError(t, err)

	tracker, _ := newMockTracker()
	err = writer.FlushAll(tracker)
	require.NoError(t, err)

	// Verify records were loaded to temp table, not final.
	require.NotEmpty(t, ldr.loadCalls)
	for _, call := range ldr.loadCalls {
		assert.True(t, strings.HasPrefix(call.tableName, "_airbyte_tmp_"),
			"overwrite should load to temp table, got %q", call.tableName)
	}

	// Verify copyTable was called from temp to final.
	require.Len(t, ops.copyTableCalls, 1)
	assert.Equal(t, "test_dataset", ops.copyTableCalls[0].srcDataset)
	assert.True(t, strings.HasPrefix(ops.copyTableCalls[0].srcTable, "_airbyte_tmp_"))
	assert.Equal(t, "test_dataset", ops.copyTableCalls[0].dstDataset)
	assert.Equal(t, "users", ops.copyTableCalls[0].dstTable)

	// Verify temp table was dropped.
	require.NotEmpty(t, ops.dropTableCalls)
	found := false
	for _, d := range ops.dropTableCalls {
		if strings.HasPrefix(d.tableName, "_airbyte_tmp_") {
			found = true
		}
	}
	assert.True(t, found, "temp table should be dropped")
}

func TestAppendMode(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	config := testConfig()
	catalog := testCatalogWithMode("events", "", "append", nil, nil, simpleSchema())

	writer := NewBigQueryWriter(ops, ldr, config, catalog)

	err := writer.AddRecord("events", map[string]interface{}{"id": float64(1), "name": "event1"})
	require.NoError(t, err)

	tracker, _ := newMockTracker()
	err = writer.FlushAll(tracker)
	require.NoError(t, err)

	// Verify records loaded directly to final table.
	require.NotEmpty(t, ldr.loadCalls)
	assert.Equal(t, "events", ldr.loadCalls[0].tableName)
	assert.Equal(t, "test_dataset", ldr.loadCalls[0].datasetID)

	// No temp tables, no staging, no copy, no merge.
	assert.Empty(t, ops.copyTableCalls)
	// executeQueryCalls may contain CREATE TABLE from ensureTable -- that's expected.
	// But should NOT contain MERGE.
	for _, sql := range ops.executeQueryCalls {
		assert.NotContains(t, sql, "MERGE", "append mode should not execute MERGE")
	}
}

func TestAppendDedupMode(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	config := testConfig()
	catalog := testCatalogWithMode("users", "", "append_dedup",
		[][]string{{"id"}}, []string{"updated_at"}, simpleSchema())

	writer := NewBigQueryWriter(ops, ldr, config, catalog)

	err := writer.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice"})
	require.NoError(t, err)

	tracker, _ := newMockTracker()
	err = writer.FlushAll(tracker)
	require.NoError(t, err)

	// Verify records were loaded to staging table.
	require.NotEmpty(t, ldr.loadCalls)
	for _, call := range ldr.loadCalls {
		assert.True(t, strings.HasPrefix(call.tableName, "_airbyte_staging_"),
			"append_dedup should load to staging table, got %q", call.tableName)
	}

	// Verify MERGE SQL was executed (may also contain CREATE TABLE from ensureTable).
	var mergeSQL string
	for _, sql := range ops.executeQueryCalls {
		if strings.Contains(sql, "MERGE") {
			mergeSQL = sql
			break
		}
	}
	require.NotEmpty(t, mergeSQL, "should execute MERGE SQL")
	assert.Contains(t, mergeSQL, "users")

	// Verify staging table was dropped.
	require.NotEmpty(t, ops.dropTableCalls)
	found := false
	for _, d := range ops.dropTableCalls {
		if strings.HasPrefix(d.tableName, "_airbyte_staging_") {
			found = true
		}
	}
	assert.True(t, found, "staging table should be dropped")
}

func TestSchemaEvolution(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	config := testConfig()
	catalog := testCatalogWithMode("users", "", "append", nil, nil, simpleSchema())

	// Simulate existing table with different schema (missing "name" column).
	ops.tableExistsResults["test_dataset.users"] = true
	ops.getTableSchemaResults["test_dataset.users"] = bigquery.Schema{
		{Name: "_airbyte_raw_id", Type: bigquery.StringFieldType, Required: true},
		{Name: "_airbyte_extracted_at", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "_airbyte_meta", Type: bigquery.JSONFieldType, Required: true},
		{Name: "_airbyte_generation_id", Type: bigquery.IntegerFieldType},
		{Name: "id", Type: bigquery.IntegerFieldType},
		// "name" column is missing -- schema evolution should add it.
	}

	writer := NewBigQueryWriter(ops, ldr, config, catalog)

	err := writer.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice"})
	require.NoError(t, err)

	tracker, _ := newMockTracker()
	err = writer.FlushAll(tracker)
	require.NoError(t, err)

	// Verify ALTER TABLE was executed to add the missing column.
	require.NotEmpty(t, ops.executeQueryCalls, "schema evolution should execute ALTER TABLE")
	alterSQL := ops.executeQueryCalls[0]
	assert.Contains(t, alterSQL, "ALTER TABLE")
	assert.Contains(t, alterSQL, "name")
}

func TestNamespaceRouting(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	config := testConfig()

	catalog := testCatalogWithMode("users", "public", "append", nil, nil, simpleSchema())

	writer := NewBigQueryWriter(ops, ldr, config, catalog)

	err := writer.AddRecord("public.users", map[string]interface{}{"id": float64(1), "name": "Alice"})
	require.NoError(t, err)

	tracker, _ := newMockTracker()
	err = writer.FlushAll(tracker)
	require.NoError(t, err)

	// Verify dataset is "public" (from namespace), not "test_dataset".
	require.NotEmpty(t, ldr.loadCalls)
	assert.Equal(t, "public", ldr.loadCalls[0].datasetID)
}

func TestLegacyRawMode(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	config := testConfig()
	config.DisableTypeDedupe = true

	catalog := testCatalogWithMode("users", "", "append", nil, nil, simpleSchema())

	writer := NewBigQueryWriter(ops, ldr, config, catalog)

	err := writer.AddRecord("users", map[string]interface{}{"id": float64(1), "name": "Alice"})
	require.NoError(t, err)

	tracker, _ := newMockTracker()
	err = writer.FlushAll(tracker)
	require.NoError(t, err)

	// Verify records loaded to raw table in raw dataset.
	require.NotEmpty(t, ldr.loadCalls)
	assert.Equal(t, "_airbyte_raw_users", ldr.loadCalls[0].tableName)
	assert.Equal(t, "airbyte_internal", ldr.loadCalls[0].datasetID)
}

func TestCDCHardDeleteMerge(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	config := testConfig()
	config.CDCDeletionMode = "hard_delete"

	catalog := testCatalogWithMode("users", "", "append_dedup",
		[][]string{{"id"}}, []string{"_ab_cdc_log_pos"}, cdcSchema())

	writer := NewBigQueryWriter(ops, ldr, config, catalog)

	err := writer.AddRecord("users", map[string]interface{}{
		"id": float64(1), "name": "Alice",
		"_ab_cdc_deleted_at": "2024-01-01T00:00:00Z",
		"_ab_cdc_updated_at": "2024-01-01T00:00:00Z",
		"_ab_cdc_log_pos":    float64(100),
	})
	require.NoError(t, err)

	tracker, _ := newMockTracker()
	err = writer.FlushAll(tracker)
	require.NoError(t, err)

	// Verify MERGE SQL contains DELETE clause for hard delete.
	var mergeSQL string
	for _, sql := range ops.executeQueryCalls {
		if strings.Contains(sql, "MERGE") {
			mergeSQL = sql
			break
		}
	}
	require.NotEmpty(t, mergeSQL, "should execute MERGE SQL")
	assert.Contains(t, mergeSQL, "DELETE")
	assert.Contains(t, mergeSQL, "_ab_cdc_deleted_at")
}

func TestCDCSoftDeleteMerge(t *testing.T) {
	ops := newMockOps()
	ldr := &mockLoader{}
	config := testConfig()
	config.CDCDeletionMode = "soft_delete"

	catalog := testCatalogWithMode("users", "", "append_dedup",
		[][]string{{"id"}}, []string{"_ab_cdc_log_pos"}, cdcSchema())

	writer := NewBigQueryWriter(ops, ldr, config, catalog)

	err := writer.AddRecord("users", map[string]interface{}{
		"id": float64(1), "name": "Alice",
		"_ab_cdc_deleted_at": "2024-01-01T00:00:00Z",
		"_ab_cdc_updated_at": "2024-01-01T00:00:00Z",
		"_ab_cdc_log_pos":    float64(100),
	})
	require.NoError(t, err)

	tracker, _ := newMockTracker()
	err = writer.FlushAll(tracker)
	require.NoError(t, err)

	// Verify MERGE SQL does NOT contain DELETE clause for soft delete.
	var mergeSQL string
	for _, sql := range ops.executeQueryCalls {
		if strings.Contains(sql, "MERGE") {
			mergeSQL = sql
			break
		}
	}
	require.NotEmpty(t, mergeSQL, "should execute MERGE SQL")
	assert.NotContains(t, mergeSQL, "THEN DELETE")
}
