package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
		if err := checkCursorValues(ctx, c, desc, p); err != nil {
			return err
		}
		if p.from != nil {
			filter = bson.D{{Key: p.field(), Value: bson.D{{Key: "$gte", Value: *p.from}}}}
		} else {
			filter = bson.D{{Key: p.field(), Value: bson.D{{Key: "$ne", Value: nil}}}}
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
		if v, err := cur.Current.LookupErr(p.cursor...); err == nil && !isNullish(v.Type) {
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
			return streamPlan{}, synclet.ConfigError(fmt.Errorf("stream %s: cursor field %q is not a field path", streamName(desc), strings.Join(field, ".")))
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

func isNullish(t bson.Type) bool { return t == bson.TypeNull || t == bson.TypeUndefined }

// typeNames are the BSON type aliases of the $type query operator.
var typeNames = map[bson.Type]string{
	bson.TypeDouble: "double", bson.TypeString: "string", bson.TypeEmbeddedDocument: "object",
	bson.TypeArray: "array", bson.TypeBinary: "binData", bson.TypeUndefined: "undefined",
	bson.TypeObjectID: "objectId", bson.TypeBoolean: "bool", bson.TypeDateTime: "date",
	bson.TypeNull: "null", bson.TypeRegex: "regex", bson.TypeDBPointer: "dbPointer",
	bson.TypeJavaScript: "javascript", bson.TypeSymbol: "symbol", bson.TypeCodeWithScope: "javascriptWithScope",
	bson.TypeInt32: "int", bson.TypeTimestamp: "timestamp", bson.TypeInt64: "long",
	bson.TypeDecimal128: "decimal", bson.TypeMinKey: "minKey", bson.TypeMaxKey: "maxKey",
}

func typeName(t bson.Type) string {
	if n, ok := typeNames[t]; ok {
		return n
	}
	return t.String()
}

// bracket returns the BSON types MongoDB compares with values of type t:
// all numeric types together, string with symbol, every other type alone.
func bracket(t bson.Type) []bson.Type {
	switch t {
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128:
		return []bson.Type{bson.TypeInt32, bson.TypeInt64, bson.TypeDouble, bson.TypeDecimal128}
	case bson.TypeString, bson.TypeSymbol:
		return []bson.Type{bson.TypeString, bson.TypeSymbol}
	}
	return []bson.Type{t}
}

// checkCursorValues fails an incremental read whose cursor field a range
// query on it would skip in some documents: documents where it is null or
// missing (unless allow_missing_cursor is set, then they are counted and
// skipped), and documents where its type is outside the comparison bracket
// of the saved cursor, or of the first cursor in sort order.
func checkCursorValues(ctx context.Context, c *client, desc synclet.StreamDescriptor, p streamPlan) error {
	field := p.field()
	missing, err := p.coll.CountDocuments(ctx, bson.D{{Key: field, Value: nil}})
	if err != nil {
		return readError(c, desc, err)
	}
	if missing > 0 {
		if !c.cfg.AllowMissingCursor {
			var doc bson.Raw
			if err := p.coll.FindOne(ctx, bson.D{{Key: field, Value: nil}}, options.FindOne().SetProjection(bson.D{{Key: "_id", Value: 1}})).Decode(&doc); err != nil {
				return readError(c, desc, err)
			}
			return synclet.ConfigError(fmt.Errorf("stream %s: %d documents of collection %q have no value in cursor field %q (for example _id %s); "+
				"an incremental read would skip them: set allow_missing_cursor to skip them knowingly, choose another cursor field or use full refresh",
				streamName(desc), missing, desc.Name, field, docID(doc)))
		}
		synclet.Logger(ctx).Warn("skipping documents without a cursor value",
			"stream", streamName(desc), "cursor_field", field, "count", missing)
	}

	ref := p.from
	if ref == nil {
		var doc bson.Raw
		err := p.coll.FindOne(ctx, bson.D{{Key: field, Value: bson.D{{Key: "$ne", Value: nil}}}},
			options.FindOne().SetSort(bson.D{{Key: field, Value: 1}}).SetProjection(bson.D{{Key: field, Value: 1}})).Decode(&doc)
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil
		}
		if err != nil {
			return readError(c, desc, err)
		}
		v, err := doc.LookupErr(p.cursor...)
		if err != nil {
			return nil
		}
		ref = &v
	}
	allowed := bson.A{"null", "undefined"}
	for _, t := range bracket(ref.Type) {
		allowed = append(allowed, typeName(t))
	}
	var doc bson.Raw
	err = p.coll.FindOne(ctx, bson.D{{Key: field, Value: bson.D{{Key: "$exists", Value: true}, {Key: "$not", Value: bson.D{{Key: "$type", Value: allowed}}}}}},
		options.FindOne().SetProjection(bson.D{{Key: field, Value: 1}})).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil
	}
	if err != nil {
		return readError(c, desc, err)
	}
	other := "array element"
	if v, err := doc.LookupErr(p.cursor...); err == nil {
		other = typeName(v.Type)
	}
	return synclet.ConfigError(fmt.Errorf("stream %s: cursor field %q of collection %q holds %s and %s values (for example _id %s); "+
		"MongoDB does not order them against each other, so an incremental read would skip documents: choose a cursor field with one type or use full refresh",
		streamName(desc), field, desc.Name, typeName(ref.Type), other, docID(doc)))
}
