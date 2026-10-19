package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Destination is the S3 Data Lake destination connector. A batch is written
// when it holds maxRows records or maxBytes of record data, on a STATE when
// the last commit is maxAge old or older, and at the end of the input.
type Destination struct {
	maxRows  int
	maxBytes int
	maxAge   time.Duration
}

// NewDestination returns the connector with its default batch limits.
func NewDestination() Destination {
	return Destination{maxRows: 50_000, maxBytes: 64 << 20, maxAge: 60 * time.Second}
}

const cleanupTimeout = 30 * time.Second

// Spec returns the connector specification.
func (Destination) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-s3-data-lake",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: connectionSpec(),
	}, nil
}

// Write loads every RECORD of in into its stream's Iceberg table and echoes
// each STATE once the snapshot holding the records before it is committed.
// On failure the uncommitted files are removed, so overwrite streams keep
// their previous snapshot.
func (d Destination) Write(
	ctx context.Context,
	cfg Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) (rerr error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return err
	}
	plans, err := planStreams(catalog, cfg)
	if err != nil {
		return fmt.Errorf("catalog: %w", err)
	}
	l, err := openLake(ctx, cfg)
	if err != nil {
		return err
	}
	defer l.close()

	w := newWriter(em, d, plans)
	defer w.release()
	defer w.warnUnknown(ctx)
	defer func() {
		if rerr != nil {
			w.discardPending(ctx)
		}
	}()
	if err := w.prepare(ctx, l); err != nil {
		return err
	}

	for {
		msg, err := in.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read input: %w", err)
		}
		switch msg.Type {
		case synclet.MessageTypeRecord:
			if msg.Record == nil {
				return errors.New("RECORD message without record")
			}
			if err := w.addRecord(ctx, msg.Record); err != nil {
				return err
			}
		case synclet.MessageTypeState:
			if msg.State == nil {
				return errors.New("STATE message without state")
			}
			if err := w.addState(ctx, *msg.State); err != nil {
				return err
			}
		}
	}
	return w.finish(ctx)
}
