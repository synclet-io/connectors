package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Discover returns one stream per configured glob. Its schema is the
// configured one, or inferred from the newest matching files.
func (src Source) Discover(ctx context.Context, cfg Config) (*synclet.Catalog, error) {
	store, err := src.open(ctx, &cfg)
	if err != nil {
		return nil, err
	}
	cat := &synclet.Catalog{}
	for _, s := range cfg.Streams {
		schema, err := streamSchema(ctx, store, s)
		if err != nil {
			return nil, err
		}
		cat.Streams = append(cat.Streams, synclet.Stream{
			Name:                    s.Name,
			JSONSchema:              schema,
			SupportedSyncModes:      []synclet.SyncMode{synclet.SyncModeFullRefresh, synclet.SyncModeIncremental},
			SourceDefinedCursor:     true,
			DefaultCursorField:      []string{fieldModified},
			SourceDefinedPrimaryKey: [][]string{{fieldFile}, {fieldRow}},
		})
	}
	return cat, nil
}

// validConfig applies the defaults and validates cfg.
func validConfig(cfg *Config) error {
	cfg.applyDefaults()
	return synclet.ConfigError(cfg.validate())
}

// open validates cfg and returns the store discover and read use.
func (src Source) open(ctx context.Context, cfg *Config) (objectStore, error) {
	if err := validConfig(cfg); err != nil {
		return nil, err
	}
	if src.store != nil {
		return src.store, nil
	}
	return newS3Store(ctx, *cfg)
}

func streamSchema(ctx context.Context, store objectStore, s StreamConfig) (json.RawMessage, error) {
	if s.Schema != "" {
		rs, err := parseSchema(json.RawMessage(s.Schema))
		if err != nil {
			return nil, synclet.ConfigError(fmt.Errorf("stream %q: schema: %w", s.Name, err))
		}
		return withMetadata(rs.raw), nil
	}
	objs, err := listMatching(ctx, store, s)
	if err != nil {
		return nil, err
	}
	if len(objs) == 0 {
		return nil, synclet.ConfigError(fmt.Errorf("stream %q: no object matches glob %q; add files or give the stream a schema", s.Name, s.Glob))
	}
	objs = newestFirst(objs)
	in := newInference(s.Format.Filetype)
	for _, o := range objs[:min(len(objs), s.SchemaSampleFiles)] {
		fi, err := inferFile(ctx, store, s, o.Key)
		if err != nil {
			return nil, fmt.Errorf("stream %q: infer schema: %w", s.Name, err)
		}
		in.merge(fi)
	}
	return withMetadata(in.properties()), nil
}

// newestFirst sorts objs by LastModified, newest first, then by key, the
// order schema inference samples them in.
func newestFirst(objs []object) []object {
	slices.SortFunc(objs, func(a, b object) int {
		return cmp.Or(b.LastModified.Compare(a.LastModified), strings.Compare(a.Key, b.Key))
	})
	return objs
}

// listMatching returns the objects whose key matches the stream's glob.
func listMatching(ctx context.Context, store objectStore, s StreamConfig) ([]object, error) {
	all, err := store.List(ctx, globPrefix(s.Glob))
	if err != nil {
		return nil, fmt.Errorf("stream %q: list %q: %w", s.Name, globPrefix(s.Glob), err)
	}
	var out []object
	for _, o := range all {
		if strings.HasSuffix(o.Key, "/") {
			continue
		}
		if ok, _ := doublestar.Match(s.Glob, o.Key); ok {
			out = append(out, o)
		}
	}
	return out, nil
}

// inferFile reads the schema of one file: the file's own for Parquet, the
// types of every value otherwise.
func inferFile(ctx context.Context, store objectStore, s StreamConfig, key string) (_ *inference, rerr error) {
	rs, err := openRows(ctx, store, s, key)
	if err != nil {
		return nil, err
	}
	defer func() { rerr = errors.Join(rerr, rs.Close()) }()
	in := newInference(s.Format.Filetype)
	if d, ok := rs.(schemaDeclarer); ok {
		d.declare(in)
		return in, nil
	}
	for row := 1; ; row++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fields, err := rs.Next()
		var bad *badRowError
		switch {
		case errors.Is(err, io.EOF):
			return in, nil
		case errors.As(err, &bad) && s.SkipBadRows:
			continue
		case err != nil:
			return nil, fmt.Errorf("file %q row %d: %w", key, row, err)
		}
		for name, v := range fields {
			in.observe(name, v)
		}
	}
}
