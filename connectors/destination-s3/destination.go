package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"go.uber.org/multierr"
)

// S3Destination writes streams as JSONL, CSV or Parquet files into an
// S3-compatible bucket.
type S3Destination struct {
	now func() time.Time
}

// NewS3Destination returns the destination with the real clock.
func NewS3Destination() *S3Destination {
	return &S3Destination{now: time.Now}
}

// Spec returns the connector specification.
func (d *S3Destination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-s3",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
		},
		ConnectionSpecification: s3ConnectionSpec(),
	}, nil
}

// Write reads the record stream, writes records into files, uploads every
// open file before echoing each STATE, and on success removes the previous
// objects of overwrite streams.
func (d *S3Destination) Write(
	ctx context.Context,
	cfg Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) (rerr error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	client, err := newS3Client(ctx, cfg)
	if err != nil {
		return err
	}
	tmpDir, err := os.MkdirTemp("", "destination-s3-*")
	if err != nil {
		return fmt.Errorf("creating temporary directory: %w", err)
	}
	defer multierr.AppendInvoke(&rerr, multierr.Invoke(func() error { return os.RemoveAll(tmpDir) }))

	w, err := newSyncWriter(cfg, catalog, newS3Store(client, cfg.Bucket), d.now(), tmpDir)
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
