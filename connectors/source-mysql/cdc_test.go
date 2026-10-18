package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

func openTestDB(t *testing.T, cfg Config) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	return db
}

func exec(t *testing.T, db *sql.DB, queries ...string) {
	t.Helper()
	for _, q := range queries {
		_, err := db.ExecContext(t.Context(), q)
		require.NoError(t, err, q)
	}
}

func createUsersTable(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	exec(t, db,
		"DROP TABLE IF EXISTS `"+table+"`",
		"CREATE TABLE `"+table+"` ("+`
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255),
			age INT,
			salary DECIMAL(10,2),
			active TINYINT(1) DEFAULT 1,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		)`,
		"INSERT INTO `"+table+"` (name, email, age, salary) VALUES "+`
			('Alice', 'alice@example.com', 30, 50000.50),
			('Bob', 'bob@example.com', 25, 60000.00),
			('Charlie', 'charlie@example.com', 35, 75000.25)`,
	)
}

// lockedBuffer is a bytes.Buffer safe to read while a CDC read writes to it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) messages(t *testing.T) []synclet.Message {
	b.mu.Lock()
	data := append([]byte(nil), b.buf.Bytes()...)
	b.mu.Unlock()
	return parseMessages(t, data)
}

func parseMessages(t *testing.T, data []byte) []synclet.Message {
	t.Helper()
	var msgs []synclet.Message
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var msg synclet.Message
		require.NoError(t, json.Unmarshal(sc.Bytes(), &msg), sc.Text())
		msgs = append(msgs, msg)
	}
	require.NoError(t, sc.Err())
	return msgs
}

func records(t *testing.T, msgs []synclet.Message) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, m := range msgs {
		if m.Type != synclet.MessageTypeRecord {
			continue
		}
		var data map[string]any
		require.NoError(t, json.Unmarshal(m.Record.Data, &data))
		out = append(out, data)
	}
	return out
}

func states(msgs []synclet.Message) []synclet.StateMessage {
	var out []synclet.StateMessage
	for _, m := range msgs {
		if m.Type == synclet.MessageTypeState {
			out = append(out, *m.State)
		}
	}
	return out
}

func names(recs []map[string]any) []string {
	var out []string
	for _, r := range recs {
		out = append(out, fmt.Sprint(r["name"]))
	}
	return out
}

func configuredCatalog(t *testing.T, table, syncMode, cursor string) json.RawMessage {
	t.Helper()
	cs := synclet.ConfiguredStream{
		Stream:              synclet.Stream{Name: table, Namespace: testDB, JSONSchema: json.RawMessage(`{}`)},
		SyncMode:            synclet.SyncMode(syncMode),
		DestinationSyncMode: synclet.DestinationSyncModeAppend,
	}
	if cursor != "" {
		cs.CursorField = []string{cursor}
	}
	b, err := json.Marshal(synclet.ConfiguredCatalog{Streams: []synclet.ConfiguredStream{cs}})
	require.NoError(t, err)
	return b
}

func lastStateMessage(t *testing.T, sts []synclet.StateMessage) json.RawMessage {
	t.Helper()
	require.NotEmpty(t, sts)
	b, err := json.Marshal([]synclet.StateMessage{sts[len(sts)-1]})
	require.NoError(t, err)
	return b
}

// runCDC runs a CDC read, calls during once the read streams the binlog (at
// once when resuming from state, else after the snapshot), and cancels the
// read, the way SIGTERM ends a CDC sync, once stop reports true for the
// messages emitted so far.
func runCDC(t *testing.T, cfg Config, catalog json.RawMessage, state json.RawMessage, stop func([]synclet.Message) bool, during func()) []synclet.Message {
	t.Helper()
	var cat synclet.ConfiguredCatalog
	require.NoError(t, json.Unmarshal(catalog, &cat))
	prev, err := synclet.ParseInputState(state)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out := &lockedBuffer{}
	done := make(chan error, 1)
	go func() {
		done <- Source{}.Read(ctx, cfg, &cat, prev, synclet.NewEmitter(out))
	}()

	ranDuring := false
	deadline := time.After(60 * time.Second)
	for {
		select {
		case err := <-done:
			require.NoError(t, err)
			t.Fatalf("CDC read returned before it was cancelled; messages: %v", out.messages(t))
		case <-deadline:
			cancel()
			<-done
			t.Fatalf("timed out; messages so far: %+v", out.messages(t))
		case <-time.After(200 * time.Millisecond):
		}
		msgs := out.messages(t)
		if !ranDuring && (state != nil || snapshotDone(t, msgs)) {
			ranDuring = true
			during()
		}
		if ranDuring && stop(msgs) {
			break
		}
	}
	cancel()
	require.NoError(t, <-done)
	return out.messages(t)
}

func snapshotDone(t *testing.T, msgs []synclet.Message) bool {
	t.Helper()
	for _, st := range states(msgs) {
		if st.Global == nil {
			continue
		}
		var shared cdcSharedState
		require.NoError(t, json.Unmarshal(st.Global.SharedState, &shared))
		if shared.SnapshotDone {
			return true
		}
	}
	return false
}

func TestIntegrationCDCRead(t *testing.T) {
	cfg := sharedMySQL(t)
	cfg.ReplicationMethod.Method = replicationCDC
	db := openTestDB(t, cfg)
	createUsersTable(t, db, "cdc_users")
	catalog := configuredCatalog(t, "cdc_users", "incremental", "")

	first := runCDC(t, cfg, catalog, nil,
		func(msgs []synclet.Message) bool { return len(records(t, msgs)) >= 6 },
		func() {
			exec(t, db,
				"INSERT INTO cdc_users (name) VALUES ('Dave')",
				"UPDATE cdc_users SET age = 31 WHERE name = 'Alice'",
				"DELETE FROM cdc_users WHERE name = 'Bob'",
			)
		})

	recs := records(t, first)
	require.Len(t, recs, 6)
	assert.Equal(t, []string{"Alice", "Bob", "Charlie", "Dave", "Alice", "Bob"}, names(recs))
	assert.Nil(t, recs[3]["_ab_cdc_deleted_at"], "insert")
	assert.Equal(t, float64(31), recs[4]["age"], "update after image")
	assert.NotNil(t, recs[5]["_ab_cdc_deleted_at"], "delete")
	for _, r := range recs {
		assert.Contains(t, r, "_ab_cdc_log_file")
		assert.Contains(t, r, "_ab_cdc_log_pos")
	}

	sts := states(first)
	require.NotEmpty(t, sts)
	for _, st := range sts {
		require.Equal(t, synclet.StateTypeGlobal, st.Type)
		require.Len(t, st.Global.StreamStates, 1)
		assert.Equal(t, streamDescriptor(testDB, "cdc_users"), st.Global.StreamStates[0].StreamDescriptor)
	}
	var shared cdcSharedState
	require.NoError(t, json.Unmarshal(sts[len(sts)-1].Global.SharedState, &shared))
	assert.True(t, shared.SnapshotDone)
	assert.NotEmpty(t, shared.BinlogFile)
	assert.NotZero(t, shared.BinlogPos)

	second := runCDC(t, cfg, catalog, lastStateMessage(t, sts),
		func(msgs []synclet.Message) bool { return len(records(t, msgs)) >= 1 },
		func() { exec(t, db, "INSERT INTO cdc_users (name) VALUES ('Erin')") })
	assert.Equal(t, []string{"Erin"}, names(records(t, second)))
	for _, st := range states(second) {
		assert.Equal(t, synclet.StateTypeGlobal, st.Type)
	}
}

func TestIntegrationCDCReadResumesFromLegacyState(t *testing.T) {
	cfg := sharedMySQL(t)
	cfg.ReplicationMethod.Method = replicationCDC
	db := openTestDB(t, cfg)
	createUsersTable(t, db, "cdc_legacy_users")
	catalog := configuredCatalog(t, "cdc_legacy_users", "incremental", "")

	var file string
	var pos uint32
	var doDB, ignoreDB, gtid string
	require.NoError(t, db.QueryRowContext(t.Context(), "SHOW MASTER STATUS").Scan(&file, &pos, &doDB, &ignoreDB, &gtid))
	legacy := `{"binlog_file":"` + file + `","binlog_pos":` + strconv.FormatUint(uint64(pos), 10) +
		`,"snapshot_done":true,"snapshot_streams":{"cdc_legacy_users":{"done":true}}}`

	msgs := runCDC(t, cfg, catalog, json.RawMessage(legacy),
		func(msgs []synclet.Message) bool { return len(records(t, msgs)) >= 1 },
		func() { exec(t, db, "INSERT INTO cdc_legacy_users (name) VALUES ('Frank')") })

	assert.Equal(t, []string{"Frank"}, names(records(t, msgs)))
	sts := states(msgs)
	require.NotEmpty(t, sts)
	for _, st := range sts {
		assert.Equal(t, synclet.StateTypeGlobal, st.Type)
	}
}

// cdcOnlyDifferences are the columns of the types table whose binlog value is
// not the one SELECT reads: geometry arrives as SRID and WKB, emitted
// base64-encoded.
var cdcOnlyDifferences = []string{"c_geometry", "c_point", "c_linestring", "c_polygon"}

func TestIntegrationCDCValuesMatchSelect(t *testing.T) {
	cfg := sharedMySQL(t)
	cfg.ReplicationMethod.Method = replicationCDC
	db := openTestDB(t, cfg)
	exec(t, db, "DROP TABLE IF EXISTS cdc_types", "CREATE TABLE cdc_types LIKE types")
	catalog := configuredCatalog(t, "cdc_types", "incremental", "")

	msgs := runCDC(t, cfg, catalog, nil,
		func(msgs []synclet.Message) bool { return len(records(t, msgs)) >= 2 },
		func() { exec(t, db, "INSERT INTO cdc_types SELECT * FROM types") })

	var fromBinlog []map[string]json.RawMessage
	for _, m := range msgs {
		if m.Type == synclet.MessageTypeRecord {
			var row map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(m.Record.Data, &row))
			fromBinlog = append(fromBinlog, row)
		}
	}
	require.Len(t, fromBinlog, 2)

	stream := discoverStream(t, sharedMySQL(t), "types")
	fromSelect := map[string]map[string]json.RawMessage{}
	for _, row := range rowsOf(t, runRead(t, sharedMySQL(t), fullRefresh(stream), nil), stream.Descriptor()) {
		fromSelect[string(row["id"])] = row
	}
	for _, got := range fromBinlog {
		want := fromSelect[string(got["id"])]
		require.NotNil(t, want)
		for col, v := range want {
			if col == "c_json" {
				require.JSONEq(t, string(v), string(got[col]), col)
				continue
			}
			if string(v) != "null" && slices.Contains(cdcOnlyDifferences, col) {
				continue
			}
			require.Equal(t, string(v), string(got[col]), "%s of row %s", col, got["id"])
		}
	}
}
