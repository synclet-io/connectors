package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Destination is the SQL Server destination connector. A batch is committed
// when it holds maxRows records or maxBytes of record data, on a STATE when
// the last commit is maxAge old or older, and at the end of the input.
type Destination struct {
	maxRows  int
	maxBytes int
	maxAge   time.Duration
}

// NewDestination returns the connector with its default batch limits.
func NewDestination() Destination {
	return Destination{maxRows: 10_000, maxBytes: 32 << 20, maxAge: 30 * time.Second}
}

const cleanupTimeout = 10 * time.Second

// Write loads every RECORD of in into its stream's table and echoes each
// STATE once the records before it are committed. On failure the uncommitted
// batch is rolled back and the shadow tables of overwrite streams are
// dropped, so their old tables stay.
func (d Destination) Write(
	ctx context.Context,
	cfg Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) (rerr error) {
	cfg.applyDefaults()
	plans, err := planStreams(catalog)
	if err != nil {
		return fmt.Errorf("catalog: %w", err)
	}
	conn, err := connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer conn.close()

	w := newWriter(conn.Conn, cfg.Schema, em, d, plans)
	defer w.warnUnknown(ctx)
	defer func() {
		if rerr != nil {
			dropShadows(ctx, cfg, plans)
		}
	}()
	if err := w.prepare(ctx); err != nil {
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

// dropShadows drops the shadow tables of overwrite streams on a connection
// of its own, since the sync's connection may be broken or its context
// cancelled.
func dropShadows(ctx context.Context, cfg Config, plans []*streamPlan) {
	log := synclet.Logger(ctx)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	var shadows []*streamPlan
	for _, p := range plans {
		if p.shadow != "" {
			shadows = append(shadows, p)
		}
	}
	if len(shadows) == 0 {
		return
	}
	conn, err := connect(ctx, cfg)
	if err != nil {
		log.Warn(fmt.Sprintf("drop shadow tables: %v", err))
		return
	}
	defer conn.close()
	for _, p := range shadows {
		shadow := qualified(cfg.Schema, p.shadow)
		if _, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+shadow); err != nil {
			log.Warn(fmt.Sprintf("drop shadow table %s: %v", shadow, err))
		}
	}
}
