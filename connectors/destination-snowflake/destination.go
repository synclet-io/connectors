package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Destination is the Snowflake destination connector. A batch is committed
// when it holds maxRows records or maxBytes of record data, on a STATE when
// the last commit is maxAge old or older, and at the end of the input.
type Destination struct {
	maxRows  int
	maxBytes int
	maxAge   time.Duration

	// loginTimeout bounds the driver's retries of a failed login.
	loginTimeout time.Duration
	endpoint     *url.URL
	transport    http.RoundTripper
}

// NewDestination returns the connector with its default batch limits.
func NewDestination() Destination {
	return Destination{maxRows: 100_000, maxBytes: 64 << 20, maxAge: time.Minute, loginTimeout: time.Minute}
}

const cleanupTimeout = 30 * time.Second

// Write loads every RECORD of in into its stream's table and echoes each
// STATE once the records before it are committed. On failure the open batch
// is rolled back and the shadow tables of overwrite streams are dropped, so
// their old tables stay.
func (d Destination) Write(
	ctx context.Context,
	cfg Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) (rerr error) {
	plans, err := planStreams(catalog)
	if err != nil {
		return fmt.Errorf("catalog: %w", err)
	}
	c, err := d.connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if err := c.close(); err != nil && rerr == nil {
			rerr = err
		}
	}()
	dir, err := os.MkdirTemp("", "destination-snowflake-")
	if err != nil {
		return fmt.Errorf("create batch directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	w := newWriter(c, em, d, plans, dir)
	defer w.warnUnknown(ctx)
	defer w.removeStaged(ctx)
	defer func() {
		if rerr != nil {
			c.dropShadows(ctx, plans)
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

// dropShadows drops the shadow tables of overwrite streams, even when the
// sync's context is cancelled.
func (c *conn) dropShadows(ctx context.Context, plans []*streamPlan) {
	log := synclet.Logger(ctx)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()
	for _, p := range plans {
		if p.shadow == "" {
			continue
		}
		if err := c.exec(ctx, "DROP TABLE IF EXISTS "+c.tableRef(p.shadow)); err != nil {
			log.Warn(fmt.Sprintf("drop shadow table %s: %v", c.tableRef(p.shadow), err))
		}
	}
}
