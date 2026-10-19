package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Destination is the Redshift destination connector. A batch is committed
// when it holds maxRows records or maxBytes of record data, on a STATE when
// the last commit is maxAge old or older, and at the end of the input.
type Destination struct {
	maxRows  int
	maxBytes int
	maxAge   time.Duration

	// open starts a warehouse session; tests replace it.
	open func(ctx context.Context, cfg Config) (warehouse, error)
	// s3Endpoint and stsEndpoint replace the AWS endpoints in tests.
	s3Endpoint  string
	stsEndpoint string
	// newID names the staging folder of one sync.
	newID func() string
}

// NewDestination returns the connector with its default batch limits.
func NewDestination() Destination {
	return Destination{
		maxRows:  100_000,
		maxBytes: 64 << 20,
		maxAge:   time.Minute,
		open:     connect,
		newID:    uuid.NewString,
	}
}

const cleanupTimeout = 10 * time.Second

// Write stages every RECORD of in, loads it into its stream's table and
// echoes each STATE once the records before it are committed. On failure
// the uncommitted batch is rolled back, its staged files are deleted and
// the shadow tables of overwrite streams are dropped, so their old tables
// stay.
func (d Destination) Write(
	ctx context.Context,
	cfg Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) (rerr error) {
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return synclet.ConfigError(err)
	}
	plans, err := planStreams(catalog)
	if err != nil {
		return fmt.Errorf("catalog: %w", err)
	}
	st, err := newStaging(ctx, cfg, d.s3Endpoint, d.stsEndpoint)
	if err != nil {
		return err
	}
	w, err := d.open(ctx, cfg)
	if err != nil {
		return err
	}
	defer w.close()

	wr := newWriter(w, st, cfg, em, d, d.newID(), plans)
	defer wr.warn(ctx)
	defer func() {
		if rerr != nil {
			d.dropShadows(ctx, cfg, plans)
		}
	}()
	if err := wr.prepare(ctx); err != nil {
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
			if err := wr.addRecord(ctx, msg.Record); err != nil {
				return err
			}
		case synclet.MessageTypeState:
			if msg.State == nil {
				return errors.New("STATE message without state")
			}
			if err := wr.addState(ctx, *msg.State); err != nil {
				return err
			}
		}
	}
	return wr.finish(ctx)
}

// dropShadows drops the shadow tables of overwrite streams on a session of
// its own, since the sync's session may be broken or its context
// cancelled.
func (d Destination) dropShadows(ctx context.Context, cfg Config, plans []*streamPlan) {
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
	w, err := d.open(ctx, cfg)
	if err != nil {
		log.Warn(fmt.Sprintf("drop shadow tables: %v", err))
		return
	}
	defer w.close()
	for _, p := range shadows {
		if err := w.exec(ctx, "DROP TABLE IF EXISTS "+quote(cfg.Schema, p.shadow)); err != nil {
			log.Warn(fmt.Sprintf("drop shadow table %s: %v", quote(cfg.Schema, p.shadow), err))
		}
	}
}
