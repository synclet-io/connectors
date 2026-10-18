package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/google/uuid"
	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

const defaultBatchSize = 5000

// streamConfig holds per-stream configuration resolved from the catalog.
type streamConfig struct {
	namespace   string
	streamName  string
	syncMode    string // "overwrite", "append", "append_dedup"
	primaryKey  [][]string
	cursorField string
	jsonSchema  map[string]interface{}
	bqSchema    bigquery.Schema
	columns     []columnDef
	datasetID   string // resolved from namespace or default
	tableName   string
	cdcEnabled  bool

	// Tracking fields for sync mode implementations.
	tempTable    string // for overwrite: "_airbyte_tmp_{streamName}"
	stagingTable string // for append_dedup: "_airbyte_staging_{streamName}"
	tableCreated bool   // whether we've ensured the final table exists
}

// streamBuffer holds per-stream buffered records awaiting flush.
type streamBuffer struct {
	records []map[string]interface{}
}

// BigQueryWriter buffers records per stream and flushes to BigQuery at threshold.
type BigQueryWriter struct {
	ops           bigqueryOps
	loader        loader
	config        *Config
	streams       map[string]*streamConfig
	buffers       map[string]*streamBuffer
	batchSize     int
	pendingStates []pendingState
	generationID  int64
}

// catalogStream represents a stream entry from the configured catalog.
type catalogStream struct {
	Stream              streamDef  `json:"stream"`
	SyncMode            string     `json:"sync_mode"`
	DestinationSyncMode string     `json:"destination_sync_mode"`
	PrimaryKey          [][]string `json:"primary_key"`
	CursorField         []string   `json:"cursor_field"`
}

// streamDef represents a stream definition from the catalog.
type streamDef struct {
	Name       string                 `json:"name"`
	Namespace  string                 `json:"namespace"`
	JSONSchema map[string]interface{} `json:"json_schema"`
}

// NewBigQueryWriter creates a writer that parses the catalog into per-stream configs.
func NewBigQueryWriter(ops bigqueryOps, ldr loader, config *Config, catalog []catalogStream) *BigQueryWriter {
	w := &BigQueryWriter{
		ops:       ops,
		loader:    ldr,
		config:    config,
		streams:   make(map[string]*streamConfig),
		buffers:   make(map[string]*streamBuffer),
		batchSize: defaultBatchSize,
	}

	for _, cs := range catalog {
		key := streamKey(cs.Stream.Namespace, cs.Stream.Name)

		// Resolve dataset: use stream namespace if present (D-09), fallback to config default.
		datasetID := config.DatasetID
		if cs.Stream.Namespace != "" {
			datasetID = cs.Stream.Namespace
		}

		// Build BigQuery schema from JSON schema.
		bqSchema, columns := buildSchema(cs.Stream.JSONSchema)

		// Detect CDC by checking for _ab_cdc_deleted_at in primary key or source fields.
		cdcEnabled := hasCDCFields(cs.Stream.JSONSchema)

		var cursorField string
		if len(cs.CursorField) > 0 {
			cursorField = cs.CursorField[0]
		}

		sc := &streamConfig{
			namespace:   cs.Stream.Namespace,
			streamName:  cs.Stream.Name,
			syncMode:    cs.DestinationSyncMode,
			primaryKey:  cs.PrimaryKey,
			cursorField: cursorField,
			jsonSchema:  cs.Stream.JSONSchema,
			bqSchema:    bqSchema,
			columns:     columns,
			datasetID:   datasetID,
			tableName:   cs.Stream.Name,
			cdcEnabled:  cdcEnabled,
		}

		w.streams[key] = sc
		w.buffers[key] = &streamBuffer{}
	}

	return w
}

// AddRecord buffers a record for the given stream, enriching it with metadata fields.
// Auto-flushes when the buffer reaches batchSize.
func (w *BigQueryWriter) AddRecord(streamKey string, data map[string]interface{}) error {
	sc, ok := w.streams[streamKey]
	if !ok {
		return fmt.Errorf("unknown stream: %s", streamKey)
	}

	buf := w.buffers[streamKey]

	// Enrich record with metadata fields.
	data["_airbyte_raw_id"] = uuid.New().String()
	data["_airbyte_extracted_at"] = time.Now().UTC().Format(time.RFC3339Nano)
	data["_airbyte_generation_id"] = w.generationID

	// Validate and coerce field values, collecting validation errors.
	var validationErrors []map[string]string
	for _, col := range sc.columns {
		if val, exists := data[col.name]; exists {
			coerced, vErr := validateAndCoerce(col.name, val, col.bqType)
			if vErr != nil {
				validationErrors = append(validationErrors, map[string]string{
					"field":   vErr.field,
					"message": vErr.reason,
				})
				data[col.name] = nil
			} else {
				data[col.name] = coerced
			}
		}
	}

	// Set _airbyte_meta with validation errors.
	meta := map[string]interface{}{
		"errors": validationErrors,
	}
	if validationErrors == nil {
		meta["errors"] = []interface{}{}
	}
	metaJSON, _ := json.Marshal(meta)
	data["_airbyte_meta"] = string(metaJSON)

	buf.records = append(buf.records, data)

	// Auto-flush at threshold.
	if len(buf.records) >= w.batchSize {
		if err := w.flushStream(context.Background(), streamKey); err != nil {
			return fmt.Errorf("auto-flush stream %q: %w", streamKey, err)
		}
	}

	return nil
}

// QueueState stores a state message for emission after the next flush (D-13).
func (w *BigQueryWriter) QueueState(raw json.RawMessage) {
	w.pendingStates = append(w.pendingStates, pendingState{raw: raw})
}

// FlushAll flushes all stream buffers, finalizes sync modes, and emits pending states.
// State is only emitted after successful flush (D-13).
func (w *BigQueryWriter) FlushAll(tracker airbyte.MessageTracker) error {
	ctx := context.Background()

	// Flush all stream buffers.
	for key := range w.buffers {
		if len(w.buffers[key].records) > 0 {
			if err := w.flushStream(ctx, key); err != nil {
				return fmt.Errorf("flushing stream %q: %w", key, err)
			}
		}
	}

	// Finalize: execute post-flush operations per sync mode.
	if err := w.finalize(ctx); err != nil {
		return fmt.Errorf("finalizing: %w", err)
	}

	// Emit pending states only after all flushes succeed.
	if err := emitPendingStates(tracker, w.pendingStates); err != nil {
		return fmt.Errorf("emitting states: %w", err)
	}
	w.pendingStates = nil

	return nil
}

// ensureTable ensures the target table exists with the correct schema, applying
// schema evolution if needed (D-08). Handles legacy raw mode (D-14).
func (w *BigQueryWriter) ensureTable(ctx context.Context, sc *streamConfig) error {
	if sc.tableCreated {
		return nil
	}

	// Ensure the dataset exists.
	if err := w.ops.ensureDataset(ctx, sc.datasetID, w.config.DatasetLocation); err != nil {
		return fmt.Errorf("ensuring dataset %q: %w", sc.datasetID, err)
	}

	// D-14: Legacy raw mode -- load to raw table with rawTableSchema.
	if w.config.DisableTypeDedupe {
		sc.tableName = "_airbyte_raw_" + sc.streamName
		sc.datasetID = w.config.RawDataDataset

		if err := w.ops.ensureDataset(ctx, sc.datasetID, w.config.DatasetLocation); err != nil {
			return fmt.Errorf("ensuring raw dataset %q: %w", sc.datasetID, err)
		}

		exists, err := w.ops.tableExists(ctx, sc.datasetID, sc.tableName)
		if err != nil {
			return err
		}
		if !exists {
			if err := w.ops.createTable(ctx, sc.datasetID, sc.tableName, rawTableSchema(), nil, nil); err != nil {
				return fmt.Errorf("creating raw table: %w", err)
			}
		}

		sc.tableCreated = true
		return nil
	}

	// Check if final table exists; if so, apply schema evolution.
	exists, err := w.ops.tableExists(ctx, sc.datasetID, sc.tableName)
	if err != nil {
		return err
	}

	if exists {
		// Schema evolution: compare existing schema with desired (D-08).
		existing, err := w.ops.getTableSchema(ctx, sc.datasetID, sc.tableName)
		if err != nil {
			return fmt.Errorf("getting existing schema: %w", err)
		}

		diff := diffSchema(existing, sc.bqSchema)
		stmts := alterTableSQL(w.config.ProjectID, sc.datasetID, sc.tableName, diff)
		for _, stmt := range stmts {
			if err := w.ops.executeQuery(ctx, stmt); err != nil {
				return fmt.Errorf("schema evolution: %w", err)
			}
		}
	} else {
		// Create new table with partitioning (D-07) and clustering.
		columnTypes := make(map[string]bigquery.FieldType)
		for _, col := range sc.columns {
			columnTypes[col.name] = col.bqType
		}

		var pkCols []string
		for _, pk := range sc.primaryKey {
			if len(pk) > 0 {
				pkCols = append(pkCols, pk[0])
			}
		}

		sql := createTableSQL(w.config.ProjectID, sc.datasetID, sc.tableName, sc.columns, pkCols, columnTypes)
		if err := w.ops.executeQuery(ctx, sql); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
	}

	sc.tableCreated = true
	return nil
}

// flushStream loads buffered records for a single stream to BigQuery,
// routing to the appropriate table based on sync mode.
func (w *BigQueryWriter) flushStream(ctx context.Context, key string) error {
	sc := w.streams[key]
	buf := w.buffers[key]

	if len(buf.records) == 0 {
		return nil
	}

	// D-14: Legacy raw mode bypasses sync mode logic.
	if w.config.DisableTypeDedupe {
		if err := w.ensureTable(ctx, sc); err != nil {
			return err
		}
		if err := w.loader.load(ctx, sc.datasetID, sc.tableName, rawTableSchema(), buf.records); err != nil {
			return fmt.Errorf("loading records to raw table %q.%q: %w", sc.datasetID, sc.tableName, err)
		}
		buf.records = nil
		return nil
	}

	// Ensure the final table exists (with schema evolution).
	if err := w.ensureTable(ctx, sc); err != nil {
		return err
	}

	switch sc.syncMode {
	case "overwrite":
		return w.flushOverwrite(ctx, sc, buf)
	case "append_dedup":
		return w.flushAppendDedup(ctx, sc, buf)
	default: // "append"
		return w.flushAppend(ctx, sc, buf)
	}
}

// flushAppend loads records directly to the final table.
func (w *BigQueryWriter) flushAppend(ctx context.Context, sc *streamConfig, buf *streamBuffer) error {
	if err := w.loader.load(ctx, sc.datasetID, sc.tableName, sc.bqSchema, buf.records); err != nil {
		return fmt.Errorf("loading records to %q.%q: %w", sc.datasetID, sc.tableName, err)
	}
	buf.records = nil
	return nil
}

// flushOverwrite loads records to a temp table. On finalize, copyTable atomically
// replaces the final table (D-04).
func (w *BigQueryWriter) flushOverwrite(ctx context.Context, sc *streamConfig, buf *streamBuffer) error {
	// Create temp table on first flush.
	if sc.tempTable == "" {
		sc.tempTable = "_airbyte_tmp_" + sc.streamName

		// Create temp table with 24h expiration.
		expiration := time.Now().Add(24 * time.Hour)
		if err := w.ops.createTable(ctx, sc.datasetID, sc.tempTable, sc.bqSchema,
			nil, nil); err != nil {
			// Ignore error if table already exists; set expiration separately is not
			// supported via createTable, so we accept the default.
			_ = expiration
		}
	}

	if err := w.loader.load(ctx, sc.datasetID, sc.tempTable, sc.bqSchema, buf.records); err != nil {
		return fmt.Errorf("loading records to temp %q.%q: %w", sc.datasetID, sc.tempTable, err)
	}
	buf.records = nil
	return nil
}

// flushAppendDedup loads records to a staging table. On finalize, MERGE deduplicates
// into the final table (D-03).
func (w *BigQueryWriter) flushAppendDedup(ctx context.Context, sc *streamConfig, buf *streamBuffer) error {
	// Create staging table on first flush.
	if sc.stagingTable == "" {
		sc.stagingTable = "_airbyte_staging_" + sc.streamName

		if err := w.ops.createTable(ctx, sc.datasetID, sc.stagingTable, sc.bqSchema,
			nil, nil); err != nil {
			_ = err // ignore if already exists
		}
	}

	if err := w.loader.load(ctx, sc.datasetID, sc.stagingTable, sc.bqSchema, buf.records); err != nil {
		return fmt.Errorf("loading records to staging %q.%q: %w", sc.datasetID, sc.stagingTable, err)
	}
	buf.records = nil
	return nil
}

// finalize executes post-flush operations per sync mode:
// - Overwrite: copyTable from temp to final, drop temp.
// - Append_dedup: MERGE from staging to final, drop staging.
// - Append: no-op.
func (w *BigQueryWriter) finalize(ctx context.Context) error {
	for _, sc := range w.streams {
		switch sc.syncMode {
		case "overwrite":
			if sc.tempTable != "" {
				// Atomic copy from temp to final (D-04).
				if err := w.ops.copyTable(ctx, sc.datasetID, sc.tempTable, sc.datasetID, sc.tableName); err != nil {
					return fmt.Errorf("overwrite copy for %q: %w", sc.streamName, err)
				}
				// Drop temp table.
				if err := w.ops.dropTable(ctx, sc.datasetID, sc.tempTable); err != nil {
					return fmt.Errorf("dropping temp table for %q: %w", sc.streamName, err)
				}
			}

		case "append_dedup":
			if sc.stagingTable != "" {
				// Build MERGE SQL (D-03).
				var pkCols []string
				for _, pk := range sc.primaryKey {
					if len(pk) > 0 {
						pkCols = append(pkCols, pk[0])
					}
				}

				var colNames []string
				for _, col := range sc.columns {
					colNames = append(colNames, col.name)
				}

				cdcHardDelete := sc.cdcEnabled && w.config.CDCDeletionMode == "hard_delete"

				mergeSQL := generateMergeSQL(
					w.config.ProjectID,
					sc.datasetID, sc.tableName,
					sc.datasetID, sc.stagingTable,
					colNames, pkCols, sc.cursorField, cdcHardDelete,
				)

				if err := w.ops.executeQuery(ctx, mergeSQL); err != nil {
					return fmt.Errorf("MERGE for %q: %w", sc.streamName, err)
				}

				// Drop staging table.
				if err := w.ops.dropTable(ctx, sc.datasetID, sc.stagingTable); err != nil {
					return fmt.Errorf("dropping staging table for %q: %w", sc.streamName, err)
				}
			}

		default:
			// append: no-op
		}
	}
	return nil
}

// streamKey creates a unique key for a stream from its namespace and name.
func streamKey(namespace, name string) string {
	if namespace != "" {
		return namespace + "." + name
	}
	return name
}

// hasCDCFields checks if the JSON schema contains CDC-specific fields.
func hasCDCFields(jsonSchema map[string]interface{}) bool {
	props, ok := jsonSchema["properties"].(map[string]interface{})
	if !ok {
		return false
	}
	_, hasDeletedAt := props["_ab_cdc_deleted_at"]
	return hasDeletedAt
}
