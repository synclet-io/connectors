package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"regexp"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	airbyte "github.com/saturn4er/airbyte-go-sdk"
)

// CDCReader reads from MySQL using Change Data Capture (binlog streaming).
type CDCReader struct {
	db      *sql.DB
	cfg     Config
	tracker airbyte.MessageTracker
}

// NewCDCReader creates a new CDCReader.
func NewCDCReader(db *sql.DB, cfg Config, tracker airbyte.MessageTracker) *CDCReader {
	return &CDCReader{db: db, cfg: cfg, tracker: tracker}
}

// Read performs CDC replication for all configured streams.
func (r *CDCReader) Read(ctx context.Context, tables []TableInfo, prevState *CDCState) error {
	state := prevState
	if state == nil {
		state = &CDCState{
			SnapshotStreams: make(map[string]*CDCStreamSnapshotState),
		}
	}

	// Build stream lookup
	streams := make(map[string]TableInfo, len(tables))
	for _, t := range tables {
		streams[t.Name] = t
	}

	// Phase 1: Initial snapshot (if not done)
	if !state.SnapshotDone {
		if err := r.runInitialSnapshot(ctx, tables, state); err != nil {
			return fmt.Errorf("initial snapshot: %w", err)
		}
	}

	// Phase 2: Binlog streaming
	return r.streamBinlog(ctx, streams, state)
}

// runInitialSnapshot performs the initial snapshot for all streams.
func (r *CDCReader) runInitialSnapshot(ctx context.Context, tables []TableInfo, state *CDCState) error {
	// Capture binlog position before starting snapshot (if fresh start)
	if state.BinlogFile == "" {
		pos, err := r.getMasterPosition(ctx)
		if err != nil {
			return fmt.Errorf("getting master position: %w", err)
		}
		state.BinlogFile = pos.file
		state.BinlogPos = pos.pos
	}

	// Initialize snapshot state for streams that haven't started
	for _, table := range tables {
		if _, exists := state.SnapshotStreams[table.Name]; !exists {
			state.SnapshotStreams[table.Name] = &CDCStreamSnapshotState{}
		}
	}

	// Read each stream
	for _, table := range tables {
		ss := state.SnapshotStreams[table.Name]
		if ss.Done {
			continue
		}

		r.tracker.Log(airbyte.LogLevelInfo, fmt.Sprintf("CDC snapshot: reading %s", table.Name))

		if err := r.snapshotTable(ctx, table, ss, state); err != nil {
			return fmt.Errorf("snapshot table %s: %w", table.Name, err)
		}

		ss.Done = true
		if err := emitCDCState(r.tracker, state); err != nil {
			return fmt.Errorf("emitting state after table snapshot: %w", err)
		}
	}

	state.SnapshotDone = true
	return emitCDCState(r.tracker, state)
}

// snapshotTable reads all rows from a table for the CDC initial snapshot.
func (r *CDCReader) snapshotTable(ctx context.Context, table TableInfo, ss *CDCStreamSnapshotState, cdcState *CDCState) error {
	columns := columnNames(table.Columns)
	selectCols := quoteColumns(columns)
	pkCols := table.PrimaryKey
	streamName := table.Name
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for {
		var query string
		var args []interface{}

		if ss.LastPKVals != nil && len(pkCols) > 0 {
			whereParts := buildPKWhereClause(pkCols)
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s ORDER BY %s LIMIT %d",
				selectCols, table.Schema, table.Name, whereParts, quoteColumns(pkCols), DefaultChunkSize)
			for _, pk := range pkCols {
				args = append(args, ss.LastPKVals[pk])
			}
		} else if len(pkCols) > 0 {
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` ORDER BY %s LIMIT %d",
				selectCols, table.Schema, table.Name, quoteColumns(pkCols), DefaultChunkSize)
		} else {
			query = fmt.Sprintf("SELECT %s FROM `%s`.`%s`", selectCols, table.Schema, table.Name)
		}

		rows, err := r.db.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("snapshot query: %w", err)
		}

		count := 0
		var lastRecord map[string]interface{}

		for rows.Next() {
			record, err := scanRow(rows, columns)
			if err != nil {
				rows.Close()
				return fmt.Errorf("scanning row: %w", err)
			}

			// Add CDC meta-fields for snapshot records
			record["_ab_cdc_updated_at"] = now
			record["_ab_cdc_deleted_at"] = nil
			record["_ab_cdc_log_file"] = cdcState.BinlogFile
			record["_ab_cdc_log_pos"] = cdcState.BinlogPos

			if err := r.tracker.Record(record, streamName, ""); err != nil {
				rows.Close()
				return fmt.Errorf("emitting record: %w", err)
			}

			lastRecord = record
			count++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()

		if count == 0 || len(pkCols) == 0 {
			break
		}

		// Update snapshot progress
		pkVals := make(map[string]interface{}, len(pkCols))
		for _, pk := range pkCols {
			pkVals[pk] = lastRecord[pk]
		}
		ss.LastPKVals = pkVals

		if err := emitCDCState(r.tracker, cdcState); err != nil {
			return fmt.Errorf("emitting state: %w", err)
		}

		if count < DefaultChunkSize {
			break
		}
	}

	return nil
}

// streamBinlog starts the canal binlog streamer from the saved position.
func (r *CDCReader) streamBinlog(ctx context.Context, streams map[string]TableInfo, state *CDCState) error {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = fmt.Sprintf("%s:%d", r.cfg.Host, r.cfg.Port)
	canalCfg.User = r.cfg.Username
	canalCfg.Password = r.cfg.Password
	canalCfg.Dump.ExecutionPath = "" // disable mysqldump
	canalCfg.ParseTime = true
	canalCfg.UseDecimal = true

	// Set server ID (randomized in range 5400-6400)
	serverID := r.cfg.Replication.CDC.ServerID
	if serverID == 0 {
		serverID = uint32(5400 + rand.Intn(1000))
	}
	canalCfg.ServerID = serverID

	// Include only catalog streams
	var tableRegexes []string
	for _, table := range streams {
		tableRegexes = append(tableRegexes, regexp.QuoteMeta(table.Schema)+"\\."+regexp.QuoteMeta(table.Name))
	}
	canalCfg.IncludeTableRegex = tableRegexes

	c, err := canal.NewCanal(canalCfg)
	if err != nil {
		return fmt.Errorf("creating canal: %w", err)
	}

	checkpointInterval := r.cfg.Replication.CDC.CheckpointInterval
	if checkpointInterval == 0 {
		checkpointInterval = DefaultCheckpointInterval
	}

	handler := newCDCHandler(r.tracker, state, streams, checkpointInterval)
	c.SetEventHandler(handler)

	// Start canal in a goroutine and watch for context cancellation
	errCh := make(chan error, 1)
	go func() {
		pos := mysql.Position{
			Name: state.BinlogFile,
			Pos:  state.BinlogPos,
		}
		errCh <- c.RunFrom(pos)
	}()

	// Wait for context cancellation or canal error
	select {
	case <-ctx.Done():
		c.Close()
		// Drain the error channel
		<-errCh
		// Emit final state checkpoint
		return emitCDCState(r.tracker, state)
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("canal error: %w", err)
		}
		return nil
	}
}

type binlogPosition struct {
	file string
	pos  uint32
}

// getMasterPosition returns the current binlog position.
func (r *CDCReader) getMasterPosition(ctx context.Context) (binlogPosition, error) {
	var file string
	var pos uint32
	var binlogDoDB, binlogIgnoreDB, executedGTIDSet string

	row := r.db.QueryRowContext(ctx, "SHOW MASTER STATUS")
	if err := row.Scan(&file, &pos, &binlogDoDB, &binlogIgnoreDB, &executedGTIDSet); err != nil {
		return binlogPosition{}, fmt.Errorf("SHOW MASTER STATUS: %w", err)
	}

	return binlogPosition{file: file, pos: pos}, nil
}
