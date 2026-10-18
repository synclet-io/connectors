package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

// snapshotChunkRows is how many rows of a table with a primary key one
// query of the CDC snapshot reads.
const snapshotChunkRows = 10_000

// readCDC reads the configured streams from the binlog, after a snapshot of
// each table. A stream without a table is skipped with a warning.
func readCDC(ctx context.Context, conn *sql.DB, cfg Config, catalog *synclet.ConfiguredCatalog, prev *synclet.InputState, em *synclet.Emitter) error {
	all, err := loadTables(ctx, conn, cfg, "")
	if err != nil {
		return err
	}
	byName := make(map[string]*table, len(all))
	for _, t := range all {
		byName[t.name] = t
	}
	var tables []*table
	var names []string
	for _, cs := range catalog.Streams {
		t, ok := byName[cs.Stream.Name]
		if !ok {
			synclet.Logger(ctx).Warn("no such table, skipping the stream", "stream", streamName(cs.Stream.Descriptor()))
			continue
		}
		tables = append(tables, t)
		names = append(names, t.name)
	}
	prevCDC, err := loadCDCState(prev, names)
	if err != nil {
		synclet.Logger(ctx).Warn(fmt.Sprintf("Failed to load CDC state, starting fresh: %v", err))
	}
	return (&CDCReader{db: conn, cfg: cfg, em: em}).Read(ctx, tables, prevCDC)
}

// CDCReader reads from MySQL using Change Data Capture (binlog streaming).
type CDCReader struct {
	db  *sql.DB
	cfg Config
	em  *synclet.Emitter
}

// Read performs CDC replication for all configured streams.
func (r *CDCReader) Read(ctx context.Context, tables []*table, prevState *CDCState) error {
	state := prevState
	if state == nil {
		state = &CDCState{
			SnapshotStreams: make(map[string]*CDCStreamSnapshotState),
		}
	}

	streams := make(map[string]*table, len(tables))
	for _, t := range tables {
		streams[t.name] = t
	}

	if !state.SnapshotDone {
		if err := r.runInitialSnapshot(ctx, tables, state); err != nil {
			return fmt.Errorf("initial snapshot: %w", err)
		}
	}

	return r.streamBinlog(ctx, streams, state)
}

// runInitialSnapshot performs the initial snapshot for all streams.
func (r *CDCReader) runInitialSnapshot(ctx context.Context, tables []*table, state *CDCState) error {
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
	for _, t := range tables {
		if _, exists := state.SnapshotStreams[t.name]; !exists {
			state.SnapshotStreams[t.name] = &CDCStreamSnapshotState{}
		}
	}

	// Read each stream
	for _, t := range tables {
		ss := state.SnapshotStreams[t.name]
		if ss.Done {
			continue
		}

		synclet.Logger(ctx).Info(fmt.Sprintf("CDC snapshot: reading %s", t.name))

		if err := r.snapshotTable(ctx, t, ss, state); err != nil {
			return fmt.Errorf("snapshot table %s: %w", t.name, err)
		}

		ss.Done = true
		if err := emitCDCState(r.em, r.cfg.Database, state); err != nil {
			return fmt.Errorf("emitting state after table snapshot: %w", err)
		}
	}

	state.SnapshotDone = true
	return emitCDCState(r.em, r.cfg.Database, state)
}

// snapshotTable reads all rows of a table for the CDC initial snapshot: a
// table with a primary key in chunks ordered by it, so the snapshot resumes
// after the last key saved; a table without one in a single query.
func (r *CDCReader) snapshotTable(ctx context.Context, t *table, ss *CDCStreamSnapshotState, cdcState *CDCState) error {
	desc := streamDescriptor(t.schema, t.name)
	enc := newRowEncoder(desc, t, t.columns, r.cfg)
	pkIdx := make([]int, len(t.primaryKey))
	for i, pk := range t.primaryKey {
		pkIdx[i] = slices.IndexFunc(t.columns, func(c column) bool { return c.name == pk })
	}
	meta := cdcMeta(time.Now().UTC().Format(time.RFC3339Nano), "", cdcState.BinlogFile, cdcState.BinlogPos)

	for {
		query, args := snapshotQuery(t, ss.LastPKVals)
		count, last, err := r.snapshotChunk(ctx, desc, enc, meta, pkIdx, query, args)
		if err != nil {
			return err
		}
		if count == 0 || len(t.primaryKey) == 0 {
			return nil
		}
		ss.LastPKVals = make(map[string]any, len(t.primaryKey))
		for i, pk := range t.primaryKey {
			ss.LastPKVals[pk] = last[i]
		}
		if err := emitCDCState(r.em, r.cfg.Database, cdcState); err != nil {
			return fmt.Errorf("emitting state: %w", err)
		}
		if count < snapshotChunkRows {
			return nil
		}
	}
}

// snapshotChunk emits the rows of one snapshot query and returns how many
// there were and the primary key of the last one.
func (r *CDCReader) snapshotChunk(ctx context.Context, desc synclet.StreamDescriptor, enc *rowEncoder, meta []field, pkIdx []int, query string, args []any) (count int, last []string, rerr error) {
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, nil, fmt.Errorf("snapshot query: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(rows))
	raw := make([]sql.RawBytes, len(enc.columns))
	dest := make([]any, len(raw))
	for i := range raw {
		dest[i] = &raw[i]
	}
	last = make([]string, len(pkIdx))
	var buf []byte
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return 0, nil, fmt.Errorf("scanning row: %w", err)
		}
		fields, err := enc.fieldsOf(raw)
		if err != nil {
			return 0, nil, err
		}
		buf = appendRecord(buf[:0], append(fields, meta...))
		if err := r.em.Record(desc, json.RawMessage(buf)); err != nil {
			return 0, nil, fmt.Errorf("emitting record: %w", err)
		}
		for i, idx := range pkIdx {
			last[i] = string(raw[idx])
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return count, last, nil
}

// snapshotQuery selects the rows of t after the primary key after, in
// primary key order and at most snapshotChunkRows of them, or every row of a
// table without a primary key.
func snapshotQuery(t *table, after map[string]any) (string, []any) {
	exprs := make([]string, len(t.columns))
	for i, c := range t.columns {
		exprs[i] = c.expr() + " AS " + ident(c.name)
	}
	query := "SELECT " + strings.Join(exprs, ", ") + " FROM " + ident(t.schema, t.name)
	if len(t.primaryKey) == 0 {
		return query, nil
	}
	keys := make([]string, len(t.primaryKey))
	marks := make([]string, len(t.primaryKey))
	for i, pk := range t.primaryKey {
		keys[i] = ident(pk)
		marks[i] = "?"
	}
	var args []any
	if after != nil {
		query += " WHERE (" + strings.Join(keys, ", ") + ") > (" + strings.Join(marks, ", ") + ")"
		for _, pk := range t.primaryKey {
			args = append(args, after[pk])
		}
	}
	query += " ORDER BY " + strings.Join(keys, ", ") + " LIMIT " + strconv.Itoa(snapshotChunkRows)
	return query, args
}

// cdcMeta is the CDC metadata of a record: when it changed, when it was
// deleted, and the binlog position it was read at.
func cdcMeta(updatedAt, deletedAt, file string, pos uint32) []field {
	at := func(s string) json.RawMessage {
		if s == "" {
			return jsonNull
		}
		return jsonString([]byte(s))
	}
	return []field{
		{key: fieldKey("_ab_cdc_updated_at"), value: at(updatedAt)},
		{key: fieldKey("_ab_cdc_deleted_at"), value: at(deletedAt)},
		{key: fieldKey("_ab_cdc_log_file"), value: jsonString([]byte(file))},
		{key: fieldKey("_ab_cdc_log_pos"), value: json.RawMessage(strconv.FormatUint(uint64(pos), 10))},
	}
}

// streamBinlog starts the canal binlog streamer from the saved position.
func (r *CDCReader) streamBinlog(ctx context.Context, streams map[string]*table, state *CDCState) error {
	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = fmt.Sprintf("%s:%d", r.cfg.Host, r.cfg.Port)
	canalCfg.User = r.cfg.Username
	canalCfg.Password = r.cfg.Password
	canalCfg.Dump.ExecutionPath = "" // disable mysqldump
	canalCfg.ParseTime = false
	canalCfg.TimestampStringLocation = time.UTC
	canalCfg.UseDecimal = true

	// Set server ID (randomized in range 5400-6400)
	serverID := r.cfg.ReplicationMethod.ServerID
	if serverID == 0 {
		serverID = uint32(5400 + rand.Intn(1000))
	}
	canalCfg.ServerID = serverID

	// Include only catalog streams
	var tableRegexes []string
	for _, t := range streams {
		tableRegexes = append(tableRegexes, regexp.QuoteMeta(t.schema)+"\\."+regexp.QuoteMeta(t.name))
	}
	canalCfg.IncludeTableRegex = tableRegexes

	c, err := canal.NewCanal(canalCfg)
	if err != nil {
		return fmt.Errorf("creating canal: %w", err)
	}

	handler := newCDCHandler(r.em, r.cfg, state, streams)
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
		return emitCDCState(r.em, r.cfg.Database, state)
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
