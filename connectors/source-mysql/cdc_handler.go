package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// cdcHandler implements canal.EventHandler to process binlog events.
type cdcHandler struct {
	canal.DummyEventHandler

	em                 *synclet.Emitter
	namespace          string // database of every stream
	failZeroDate       bool
	state              *CDCState
	streams            map[string]*table // stream name -> table
	checkpointInterval time.Duration
	lastCheckpoint     time.Time

	mu sync.Mutex
}

func newCDCHandler(em *synclet.Emitter, cfg Config, state *CDCState, streams map[string]*table) *cdcHandler {
	return &cdcHandler{
		em:                 em,
		namespace:          cfg.Database,
		failZeroDate:       cfg.ZeroDateHandling == zeroDateFail,
		state:              state,
		streams:            streams,
		checkpointInterval: cfg.checkpointInterval(),
		lastCheckpoint:     time.Now(),
	}
}

// OnRow handles INSERT, UPDATE, DELETE row events.
func (h *cdcHandler) OnRow(e *canal.RowsEvent) error {
	t, ok := h.streams[e.Table.Name]
	if !ok {
		return nil
	}
	desc := streamDescriptor(t.schema, t.name)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	rows := e.Rows
	updated, deleted := now, ""
	switch e.Action {
	case canal.InsertAction:
	case canal.UpdateAction:
		// pairs of rows, before and after; the after image is emitted
		rows = nil
		for i := 1; i < len(e.Rows); i += 2 {
			rows = append(rows, e.Rows[i])
		}
	case canal.DeleteAction:
		updated, deleted = "", now
	default:
		return nil
	}
	for _, row := range rows {
		fields, err := h.fields(t, row)
		if err != nil {
			return fmt.Errorf("stream %s: %w", streamName(desc), err)
		}
		fields = append(fields, cdcMeta(updated, deleted, h.state.BinlogFile, h.state.BinlogPos)...)
		if err := h.em.Record(desc, json.RawMessage(appendRecord(nil, fields))); err != nil {
			return fmt.Errorf("emitting %s record: %w", e.Action, err)
		}
	}
	return nil
}

func (h *cdcHandler) fields(t *table, row []any) ([]field, error) {
	fields := make([]field, len(t.columns))
	for i, c := range t.columns {
		fields[i].key = fieldKey(c.name)
		if i >= len(row) {
			fields[i].value = jsonNull
			continue
		}
		v, err := c.encodeCDC(row[i], h.failZeroDate)
		if err != nil {
			if errors.Is(err, errZeroDate) {
				return nil, synclet.ConfigError(fmt.Errorf("column %q holds the zero date %q; set zero_date_handling to %q to read zero dates as null", c.name, row[i], zeroDateNull))
			}
			return nil, err
		}
		fields[i].value = v
	}
	return fields, nil
}

// encodeCDC renders a value of a binlog row event as encode renders the same
// value read by SELECT. Geometry arrives as MySQL's internal value, SRID and
// WKB, and is emitted base64-encoded.
func (c column) encodeCDC(v any, failZeroDate bool) (json.RawMessage, error) {
	switch x := v.(type) {
	case nil:
		return jsonNull, nil
	case string:
		return c.encodeCDCBytes([]byte(x), failZeroDate)
	case []byte:
		return c.encodeCDCBytes(x, failZeroDate)
	case float32:
		return json.RawMessage(strconv.FormatFloat(float64(x), 'g', -1, 32)), nil
	case float64:
		return json.RawMessage(strconv.FormatFloat(x, 'g', -1, 64)), nil
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64:
		return c.encodeCDCInt(x)
	case interface{ StringFixed(int32) string }:
		return c.encode([]byte(x.StringFixed(c.scale)), failZeroDate)
	case fmt.Stringer:
		return c.encode([]byte(x.String()), failZeroDate)
	default:
		return c.encode([]byte(fmt.Sprint(x)), failZeroDate)
	}
}

func (c column) encodeCDCBytes(b []byte, failZeroDate bool) (json.RawMessage, error) {
	if c.kind == kindGeometry {
		c.kind = kindBinary
	}
	return c.encode(b, failZeroDate)
}

// encodeCDCInt renders an integer of a row event: BIT, ENUM and SET arrive
// as integers, the bits, the member's position and the members' bit mask.
func (c column) encodeCDCInt(x any) (json.RawMessage, error) {
	text := fmt.Sprint(x)
	var u uint64
	if i, ok := x.(int64); ok {
		u = uint64(i)
	} else {
		u, _ = strconv.ParseUint(text, 10, 64)
	}
	switch c.kind {
	case kindBool:
		return json.RawMessage(strconv.FormatBool(u != 0)), nil
	case kindBit:
		return json.RawMessage(strconv.FormatUint(u, 10)), nil
	case kindEnum:
		if u == 0 || u > uint64(len(c.values)) {
			return jsonString(nil), nil
		}
		return jsonString([]byte(c.values[u-1])), nil
	case kindSet:
		var members []string
		for i, m := range c.values {
			if i < 64 && u&(1<<uint(i)) != 0 {
				members = append(members, m)
			}
		}
		return jsonString([]byte(strings.Join(members, ","))), nil
	default:
		return c.encode([]byte(text), false)
	}
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
		return emitCDCState(h.em, h.namespace, h.state)
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
