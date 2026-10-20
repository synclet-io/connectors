package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

// GCSDestination writes streams as JSONL, CSV or Parquet files into a
// Google Cloud Storage bucket.
type GCSDestination struct {
	now       func() time.Time
	newClient func(context.Context, Config) (*storage.Client, error)
}

// NewGCSDestination returns the destination with the real clock and
// Cloud Storage client.
func NewGCSDestination() *GCSDestination {
	return &GCSDestination{now: time.Now, newClient: newGCSClient}
}

// Spec returns the connector specification.
func (d *GCSDestination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-gcs",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
		},
		ConnectionSpecification: gcsConnectionSpec(),
	}, nil
}

// Write reads the record stream, writes records into files, uploads every
// open file before echoing each STATE, and on success removes the previous
// objects of overwrite streams.
func (d *GCSDestination) Write(
	ctx context.Context,
	cfg Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) (rerr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	client, err := d.newClient(ctx, cfg)
	if err != nil {
		return err
	}
	defer multierr.AppendInvoke(&rerr, multierr.Close(client))
	tmpDir, err := os.MkdirTemp("", "destination-gcs-*")
	if err != nil {
		return fmt.Errorf("creating temporary directory: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(func() error { return os.RemoveAll(tmpDir) }))

	w, err := newSyncWriter(cfg, catalog, newGCSStore(client, cfg.Bucket), d.now(), tmpDir)
	if err != nil {
		return err
	}
	return writeMessages(ctx, w, in, em)
}

// writeMessages adds every record of in to w, uploads every open file
// before echoing each STATE, and completes w at the end of the input. The
// states of an overwrite stream, and GLOBAL and LEGACY states while the
// catalog has an overwrite stream, are held until the previous objects are
// removed: a sync resumed from them would remove the files of this one.
func writeMessages(ctx context.Context, w *syncWriter, in *synclet.MessageReader, em *synclet.Emitter) (rerr error) {
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(w.abort))
	var held []synclet.StateMessage
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		msg, err := in.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("reading input: %w", err)
		}
		switch msg.Type {
		case synclet.MessageTypeRecord:
			if msg.Record == nil {
				continue
			}
			if err := w.add(ctx, msg.Record); err != nil {
				return err
			}
		case synclet.MessageTypeState:
			if msg.State == nil {
				continue
			}
			if err := w.flush(ctx); err != nil {
				return err
			}
			if w.holdsState(*msg.State) {
				held = append(held, *msg.State)
				continue
			}
			if err := em.State(*msg.State); err != nil {
				return err
			}
		}
	}
	if err := w.complete(ctx); err != nil {
		return err
	}
	for _, st := range held {
		if err := em.State(st); err != nil {
			return err
		}
	}
	return nil
}
