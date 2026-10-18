package main

import (
	"context"
	"fmt"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// sampleShape merges a random sample of up to size documents of coll into
// a shape. Documents are streamed through a cursor, never held together.
func sampleShape(ctx context.Context, coll *mongo.Collection, size int) (*shape, error) {
	pipeline := mongo.Pipeline{{{Key: "$sample", Value: bson.D{{Key: "size", Value: size}}}}}
	cur, err := coll.Aggregate(ctx, pipeline, options.Aggregate().SetAllowDiskUse(true).SetBatchSize(batchSize))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(context.WithoutCancel(ctx)) }()
	s := &shape{}
	for cur.Next(ctx) {
		if err := s.addDocument(cur.Current); err != nil {
			return nil, err
		}
	}
	return s, cur.Err()
}

// Discover returns one stream per collection of the database, with a
// schema inferred from a sample of its documents.
func (s Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	c, err := s.connect(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer c.close()
	dbName := c.cfg.DatabaseConfig.Database
	names, _, err := listCollections(ctx, c.db(), nil)
	if err != nil {
		if isUnauthorized(err) {
			return nil, synclet.ConfigError(fmt.Errorf("%s may not read database %q", c.who(), dbName))
		}
		return nil, fmt.Errorf("list collections of database %q: %s", dbName, redact(c.cfg, err.Error()))
	}
	cat := &synclet.Catalog{Streams: make([]synclet.Stream, 0, len(names))}
	for _, name := range names {
		sh, err := sampleShape(ctx, c.db().Collection(name), c.cfg.DiscoverSampleSize)
		if err != nil {
			if isUnauthorized(err) {
				return nil, synclet.ConfigError(fmt.Errorf("%s may not read collection %q of database %q", c.who(), name, dbName))
			}
			return nil, fmt.Errorf("sample collection %q of database %q: %s", name, dbName, redact(c.cfg, err.Error()))
		}
		cat.Streams = append(cat.Streams, synclet.Stream{
			Name:                    name,
			Namespace:               dbName,
			JSONSchema:              sh.streamSchema(),
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
			DefaultCursorField:      []string{"_id"},
			SourceDefinedPrimaryKey: [][]string{{"_id"}},
		})
	}
	return cat, nil
}
