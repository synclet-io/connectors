package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Destination drops every record it reads and echoes every STATE.
type Destination struct{}

// Check validates the configuration; there is nothing to connect to.
func (Destination) Check(_ context.Context, cfg Config) error {
	_, err := cfg.mode()
	return err
}

// Write reads in to its end, handles each RECORD as the configured mode says
// and echoes each STATE right away, since no record is ever held back.
func (Destination) Write(
	ctx context.Context,
	cfg Config,
	_ *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) error {
	m, err := cfg.mode()
	if err != nil {
		return synclet.ConfigError(err)
	}
	streams := map[synclet.StreamDescriptor]*streamCounter{}
	for {
		msg, err := in.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read input: %w", err)
		}
		switch msg.Type {
		case synclet.MessageTypeRecord:
			if msg.Record == nil {
				return errors.New("RECORD message without record")
			}
			desc := synclet.StreamDescriptor{Name: msg.Record.Stream, Namespace: msg.Record.Namespace}
			sc := streams[desc]
			if sc == nil {
				sc = newStreamCounter(m)
				streams[desc] = sc
			}
			if err := m.accept(ctx, em, desc, sc, msg.Record); err != nil {
				return err
			}
		case synclet.MessageTypeState:
			if msg.State == nil {
				return errors.New("STATE message without state")
			}
			if err := em.State(*msg.State); err != nil {
				return err
			}
		}
	}
}

// streamCounter is the progress of one stream.
type streamCounter struct {
	records int64
	logged  int64
	rng     *rand.Rand
}

func newStreamCounter(m mode) *streamCounter {
	sc := &streamCounter{}
	if m.kind == typeLogging && m.ratio < 1 {
		seed := rand.Uint64()
		if m.seed != nil {
			seed = *m.seed
		}
		sc.rng = rand.New(rand.NewPCG(seed, seed))
	}
	return sc
}

func (m mode) accept(ctx context.Context, em *synclet.Emitter, desc synclet.StreamDescriptor, sc *streamCounter, rec *synclet.RecordMessage) error {
	idx := sc.records
	sc.records++
	switch m.kind {
	case typeLogging:
		if idx%m.every != 0 || sc.logged >= m.maxEntries {
			return nil
		}
		if sc.rng != nil && sc.rng.Float64() >= m.ratio {
			return nil
		}
		sc.logged++
		return em.Log(synclet.LogLevelInfo, fmt.Sprintf("Logging Destination(stream=%s, recordIndex=%d, logEntry=%d/%d): %s",
			streamName(desc), idx, sc.logged, m.maxEntries, rec.Data))
	case typeThrottled:
		if m.delay <= 0 {
			return nil
		}
		t := time.NewTimer(m.delay)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return nil
		}
	case typeFailing:
		if idx >= m.failAfter {
			return fmt.Errorf("failing destination: stream %s failed at record %d after %d records", streamName(desc), idx+1, m.failAfter)
		}
	}
	return nil
}

func streamName(desc synclet.StreamDescriptor) string {
	if desc.Namespace == "" {
		return desc.Name
	}
	return desc.Namespace + "." + desc.Name
}
