package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// cdcHandler implements canal.EventHandler to process binlog events.
type cdcHandler struct {
	canal.DummyEventHandler

	tracker            airbyte.MessageTracker
	state              *CDCState
	streams            map[string]TableInfo // stream name -> table info
	checkpointInterval time.Duration
	lastCheckpoint     time.Time

	mu sync.Mutex
}

// newCDCHandler creates a new CDC event handler.
func newCDCHandler(tracker airbyte.MessageTracker, state *CDCState, streams map[string]TableInfo, checkpointInterval time.Duration) *cdcHandler {
	return &cdcHandler{
		tracker:            tracker,
		state:              state,
		streams:            streams,
		checkpointInterval: checkpointInterval,
		lastCheckpoint:     time.Now(),
	}
}

// OnRow handles INSERT, UPDATE, DELETE row events.
func (h *cdcHandler) OnRow(e *canal.RowsEvent) error {
	tableName := e.Table.Name
	tableInfo, ok := h.streams[tableName]
	if !ok {
		return nil // skip tables not in catalog
	}

	columns := columnNames(tableInfo.Columns)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			record := h.buildRecord(row, columns, tableInfo)
			record["_ab_cdc_updated_at"] = now
			record["_ab_cdc_deleted_at"] = nil
			record["_ab_cdc_log_file"] = h.state.BinlogFile
			record["_ab_cdc_log_pos"] = h.state.BinlogPos

			if err := h.tracker.Record(record, tableName, ""); err != nil {
				return fmt.Errorf("emitting insert record: %w", err)
			}
		}

	case canal.UpdateAction:
		// UpdateAction has pairs of rows: [before, after, before, after, ...]
		for i := 1; i < len(e.Rows); i += 2 {
			row := e.Rows[i] // after image
			record := h.buildRecord(row, columns, tableInfo)
			record["_ab_cdc_updated_at"] = now
			record["_ab_cdc_deleted_at"] = nil
			record["_ab_cdc_log_file"] = h.state.BinlogFile
			record["_ab_cdc_log_pos"] = h.state.BinlogPos

			if err := h.tracker.Record(record, tableName, ""); err != nil {
				return fmt.Errorf("emitting update record: %w", err)
			}
		}

	case canal.DeleteAction:
		for _, row := range e.Rows {
			record := h.buildRecord(row, columns, tableInfo)
			record["_ab_cdc_updated_at"] = nil
			record["_ab_cdc_deleted_at"] = now
			record["_ab_cdc_log_file"] = h.state.BinlogFile
			record["_ab_cdc_log_pos"] = h.state.BinlogPos

			if err := h.tracker.Record(record, tableName, ""); err != nil {
				return fmt.Errorf("emitting delete record: %w", err)
			}
		}
	}

	return nil
}

// OnPosSynced is called when the canal receives a position sync event.
func (h *cdcHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, gs mysql.GTIDSet, force bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.state.BinlogFile = pos.Name
	h.state.BinlogPos = pos.Pos

	if gs != nil {
		h.state.GTIDSet = gs.String()
	}

	if force || time.Since(h.lastCheckpoint) >= h.checkpointInterval {
		h.lastCheckpoint = time.Now()
		return emitCDCState(h.tracker, h.state)
	}

	return nil
}

// OnRotate handles binlog file rotation.
func (h *cdcHandler) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.state.BinlogFile = string(e.NextLogName)
	h.state.BinlogPos = uint32(e.Position)
	return nil
}

// String returns a description of the handler.
func (h *cdcHandler) String() string {
	return "mysqlCDCHandler"
}

// buildRecord converts a canal row event row into a record map.
func (h *cdcHandler) buildRecord(row []interface{}, columns []string, tableInfo TableInfo) map[string]interface{} {
	record := make(map[string]interface{}, len(columns)+4)

	for i, col := range columns {
		if i < len(row) {
			dataType := ""
			if i < len(tableInfo.Columns) {
				dataType = tableInfo.Columns[i].DataType
			}
			record[col] = convertCDCValue(row[i], dataType)
		} else {
			record[col] = nil
		}
	}

	return record
}
