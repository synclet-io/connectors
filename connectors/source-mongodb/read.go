package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	// checkpointRecords is how many records of an incremental stream go
	// between two STATE messages.
	checkpointRecords = 10_000
	// batchSize is how many documents the driver fetches per round trip.
	batchSize = 1000
)

// streamState is the STREAM state of an incremental stream: the greatest
// cursor read so far, as canonical Extended JSON so it keeps its BSON type.
type streamState struct {
	CursorField []string        `json:"cursor_field"`
	Cursor      json.RawMessage `json:"cursor,omitempty"`
}

// streamPlan is how one configured stream is read.
type streamPlan struct {
	coll *mongo.Collection
	// cursor is the path of the cursor field; nil for a full refresh.
	cursor []string
	// from is the saved cursor; reading starts at it, inclusive.
	from *bson.RawValue
}

func (p streamPlan) field() string { return strings.Join(p.cursor, ".") }

// Read emits every configured stream in catalog order: a full refresh reads
// the whole collection, an incremental read the documents whose cursor is
// at least the saved one, in cursor order. Documents are streamed in
// batches: each record is emitted as it is read.
func (s Source) Read(ctx context.Context, cfg Config, catalog *synclet.ConfiguredCatalog, state *synclet.InputState, em *synclet.Emitter) error {
	c, err := s.connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.close()
	for _, cs := range catalog.Streams {
		if err := readStream(ctx, c, cs, state, em); err != nil {
			return err
		}
	}
	return nil
}

func readStream(ctx context.Context, c *client, cs synclet.ConfiguredStream, state *synclet.InputState, em *synclet.Emitter) error {
	desc := cs.Stream.Descriptor()
	p, err := planStream(ctx, c, cs, state)
	if err != nil {
		return err
	}
	incremental := p.cursor != nil
	filter := bson.D{}
	opts := options.Find().SetBatchSize(batchSize)
	if incremental {
		if p.from != nil {
			filter = bson.D{{Key: p.field(), Value: bson.D{{Key: "$gte", Value: *p.from}}}}
		}
		opts.SetSort(bson.D{{Key: p.field(), Value: 1}}).SetAllowDiskUse(true)
	}
	cur, err := p.coll.Find(ctx, filter, opts)
	if err != nil {
		return readError(c, desc, err)
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()

	st := streamState{CursorField: p.cursor}
	last := p.from
	emitState := func() error {
		if last != nil {
			raw, err := canonicalExtJSON(*last)
			if err != nil {
				return fmt.Errorf("read %s: cursor: %w", streamName(desc), err)
			}
			st.Cursor = raw
		}
		return em.StreamState(desc, st)
	}
	var buf []byte
	var count int
	for cur.Next(ctx) {
		buf, err = appendDocument(buf[:0], cur.Current)
		if err != nil {
			return fmt.Errorf("read %s: document %s: %w", streamName(desc), docID(cur.Current), err)
		}
		if err := em.Record(desc, json.RawMessage(buf)); err != nil {
			return err
		}
		if !incremental {
			continue
		}
		if v, err := cur.Current.LookupErr(p.cursor...); err == nil && v.Type != bson.TypeNull && v.Type != bson.TypeUndefined {
			v.Value = bytes.Clone(v.Value)
			last = &v
		}
		count++
		if count%checkpointRecords == 0 {
			if err := emitState(); err != nil {
				return err
			}
		}
	}
	if err := cur.Err(); err != nil {
		return readError(c, desc, err)
	}
	if incremental {
		return emitState()
	}
	return nil
}

func readError(c *client, desc synclet.StreamDescriptor, err error) error {
	if isUnauthorized(err) {
		return synclet.ConfigError(fmt.Errorf("stream %s: %s may not read the collection", streamName(desc), c.who()))
	}
	return fmt.Errorf("read %s: %s", streamName(desc), redact(c.cfg, err.Error()))
}

// docID names a document in an error by its _id.
func docID(doc bson.Raw) string {
	v, err := doc.LookupErr("_id")
	if err != nil {
		return "without _id"
	}
	s, err := extJSON(v)
	if err != nil {
		return "with an unreadable _id"
	}
	return s
}

// planStream checks a configured stream against the database and the saved
// state and returns how to read it.
func planStream(ctx context.Context, c *client, cs synclet.ConfiguredStream, state *synclet.InputState) (streamPlan, error) {
	desc := cs.Stream.Descriptor()
	dbName := c.cfg.DatabaseConfig.Database
	if desc.Namespace != "" && desc.Namespace != dbName {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: namespace %q is not the configured database %q", streamName(desc), desc.Namespace, dbName))
	}
	names, _, err := listCollections(ctx, c.db(), bson.D{{Key: "name", Value: desc.Name}})
	if err != nil {
		return streamPlan{}, readError(c, desc, err)
	}
	if !slices.Contains(names, desc.Name) {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: no such collection in database %q, or %s may not read it", streamName(desc), dbName, c.who()))
	}
	p := streamPlan{coll: c.db().Collection(desc.Name)}
	if cs.SyncMode != synclet.SyncModeIncremental {
		return p, nil
	}
	field := cs.CursorField
	if len(field) == 0 {
		field = cs.Stream.DefaultCursorField
	}
	if len(field) == 0 {
		field = []string{"_id"}
	}
	if len(field) == 1 {
		field = strings.Split(field[0], ".")
	}
	for _, f := range field {
		if f == "" || strings.HasPrefix(f, "$") {
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: cursor field %q is not a field path", streamName(desc), strings.Join(cs.CursorField, ".")))
		}
	}
	p.cursor = field

	raw := state.Stream(desc)
	if raw == nil {
		return p, nil
	}
	var saved streamState
	if err := json.Unmarshal(raw, &saved); err != nil {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state: %w", streamName(desc), err))
	}
	if len(saved.CursorField) == 1 {
		saved.CursorField = strings.Split(saved.CursorField[0], ".")
	}
	if !slices.Equal(saved.CursorField, field) {
		synclet.Logger(ctx).Warn("cursor field changed, reading the stream from the start",
			"stream", streamName(desc), "saved", strings.Join(saved.CursorField, "."), "configured", p.field())
		return p, nil
	}
	if len(saved.Cursor) == 0 || string(saved.Cursor) == "null" {
		return p, nil
	}
	from, err := parseCanonicalExtJSON(saved.Cursor)
	if err != nil {
		return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: invalid state: cursor %s is not canonical Extended JSON: %w", streamName(desc), saved.Cursor, err))
	}
	p.from = &from
	return p, nil
}

func streamName(desc synclet.StreamDescriptor) string {
	if desc.Namespace == "" {
		return desc.Name
	}
	return desc.Namespace + "." + desc.Name
}
