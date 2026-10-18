package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func isUnauthorized(err error) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && se.HasErrorCode(codeUnauthorized)
}

// listCollections returns the names of the collections of db, without
// views and system collections, in name order. A user without the
// listCollections privilege gets the collections it holds a privilege on;
// denied reports that the server refused the full list.
func listCollections(ctx context.Context, db *mongo.Database, filter bson.D) (names []string, denied bool, err error) {
	filter = append(bson.D{{Key: "type", Value: "collection"}}, filter...)
	names, err = db.ListCollectionNames(ctx, filter)
	if isUnauthorized(err) {
		denied = true
		names, err = db.ListCollectionNames(ctx, filter, options.ListCollections().SetAuthorizedCollections(true))
	}
	if err != nil {
		return nil, denied, err
	}
	kept := names[:0]
	for _, n := range names {
		if !strings.HasPrefix(n, "system.") {
			kept = append(kept, n)
		}
	}
	slices.Sort(kept)
	return kept, denied, nil
}

// Check connects, pings the servers and verifies that the user may list
// the collections of the database and read the first of them.
func (s Source) Check(ctx context.Context, cfg Config) error {
	c, err := s.connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer c.close()
	dbName := c.cfg.DatabaseConfig.Database
	names, denied, err := listCollections(ctx, c.db(), nil)
	switch {
	case isUnauthorized(err) || (denied && err == nil && len(names) == 0):
		return synclet.ConfigError(fmt.Errorf("%s may not read database %q", c.who(), dbName))
	case err != nil:
		return fmt.Errorf("list collections of database %q: %s", dbName, redact(c.cfg, err.Error()))
	case len(names) == 0:
		return synclet.ConfigError(fmt.Errorf("database %q does not exist or has no collections", dbName))
	}
	err = c.db().Collection(names[0]).FindOne(ctx, bson.D{}).Err()
	switch {
	case err == nil || errors.Is(err, mongo.ErrNoDocuments):
		return nil
	case isUnauthorized(err):
		return synclet.ConfigError(fmt.Errorf("%s may not read collection %q of database %q", c.who(), names[0], dbName))
	}
	return fmt.Errorf("read collection %q of database %q: %s", names[0], dbName, redact(c.cfg, err.Error()))
}
