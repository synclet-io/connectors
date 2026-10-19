package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Destination is the Databricks destination connector. A batch is committed
// when it holds maxRows records or maxBytes of record data, on a STATE when
// the last commit is maxAge old or older, and at the end of the input.
type Destination struct {
	maxRows  int
	maxBytes int
	maxAge   time.Duration
	retries  int

	httpClient *http.Client
	newRunID   func() string
	openSQL    func(ctx context.Context, cfg Config, client *http.Client, b *bearer, retries int) (sqlRunner, error)
}

// NewDestination returns the connector with its default batch limits.
func NewDestination() Destination {
	return Destination{maxRows: 50_000, maxBytes: 64 << 20, maxAge: 60 * time.Second, retries: 4}
}

// Write loads every RECORD of in into its stream's Delta table and echoes
// each STATE once the records before it are committed. On failure the
// shadow tables of overwrite streams are dropped, so their old tables stay,
// and nothing staged is left behind.
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
	newRunID := d.newRunID
	if newRunID == nil {
		newRunID = uuid.NewString
	}
	runID := newRunID()
	plans, err := planStreams(catalog, shortRunID(runID))
	if err != nil {
		return fmt.Errorf("catalog: %w", err)
	}
	client := d.client()
	b := newBearer(ctx, cfg, client)
	run, err := d.open(ctx, cfg, client, b)
	if err != nil {
		return err
	}
	defer func() { rerr = errors.Join(rerr, run.close()) }()

	w := newWriter(run, &filesClient{base: cfg.baseURL(), http: client, bearer: b}, cfg, em, d, plans, runID)
	defer w.warnUnknown(ctx)
	defer w.cleanup(ctx)
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

const (
	httpTimeout    = 10 * time.Minute
	cleanupTimeout = 30 * time.Second
)

// shortRunID is the first 12 hex digits of a run's UUID, which name its
// helper tables.
func shortRunID(runID string) string {
	id := strings.ReplaceAll(runID, "-", "")
	return id[:min(12, len(id))]
}
