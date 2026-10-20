package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	checkTimeout = 60 * time.Second
	checkStream  = "connection_check"
)

// Destination produces every record as one JSON message to the topic its
// stream maps to.
type Destination struct {
	// rootCAs replaces the system roots for SASL_SSL; nil in main.
	rootCAs *x509.CertPool
}

// Check connects to the brokers and, when test_topic is set, produces one
// message to it.
func (d Destination) Check(ctx context.Context, cfg Config) error {
	s, err := cfg.settings()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	cl, err := d.newClient(ctx, s)
	if err != nil {
		return err
	}
	defer cl.Close()
	if err := cl.Ping(ctx); err != nil {
		return fmt.Errorf("connect to %s: %w", strings.Join(s.seeds, ","), err)
	}
	if s.testTopic == "" {
		return nil
	}
	id := newID()
	value, err := encodeValue(id, &synclet.RecordMessage{
		Stream:    checkStream,
		Data:      json.RawMessage(`{"test":"synclet connection check"}`),
		EmittedAt: time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: s.testTopic, Key: []byte(id), Value: value}).FirstErr(); err != nil {
		return fmt.Errorf("produce to test topic %s: %w", s.testTopic, err)
	}
	return nil
}

// streamTarget is where the records of one stream go.
type streamTarget struct {
	topic      string
	primaryKey [][]string
}

func targets(s settings, catalog *synclet.ConfiguredCatalog) (map[synclet.StreamDescriptor]*streamTarget, error) {
	out := map[synclet.StreamDescriptor]*streamTarget{}
	if catalog == nil {
		return out, nil
	}
	for _, cs := range catalog.Streams {
		desc := cs.Stream.Descriptor()
		if mode := cs.DestinationSyncMode; mode != "" && mode != synclet.DestinationSyncModeAppend {
			return nil, fmt.Errorf("stream %s: destination sync mode %s is not supported; Kafka topics are append-only",
				streamLabel(desc.Namespace, desc.Name), mode)
		}
		topic, err := topicName(s.topicPattern, desc.Namespace, desc.Name)
		if err != nil {
			return nil, err
		}
		pk := cs.PrimaryKey
		if len(pk) == 0 {
			pk = cs.Stream.SourceDefinedPrimaryKey
		}
		out[desc] = &streamTarget{topic: topic, primaryKey: pk}
	}
	return out, nil
}

// Write produces each RECORD and echoes each STATE once every record before
// it has been acknowledged by the brokers.
func (d Destination) Write(
	ctx context.Context,
	cfg Config,
	catalog *synclet.ConfiguredCatalog,
	in *synclet.MessageReader,
	em *synclet.Emitter,
) error {
	s, err := cfg.settings()
	if err != nil {
		return synclet.ConfigError(err)
	}
	streams, err := targets(s, catalog)
	if err != nil {
		return synclet.ConfigError(err)
	}
	cl, err := d.newClient(ctx, s)
	if err != nil {
		return synclet.ConfigError(err)
	}
	defer cl.Close()
	p := &producer{cl: cl, sync: s.syncProducer}

	for {
		msg, err := in.Next()
		if errors.Is(err, io.EOF) {
			return p.flush(ctx)
		}
		if err != nil {
			return fmt.Errorf("read input: %w", err)
		}
		switch msg.Type {
		case synclet.MessageTypeRecord:
			if msg.Record == nil {
				return errors.New("RECORD message without record")
			}
			rec := msg.Record
			desc := synclet.StreamDescriptor{Name: rec.Stream, Namespace: rec.Namespace}
			target := streams[desc]
			if target == nil {
				topic, err := topicName(s.topicPattern, desc.Namespace, desc.Name)
				if err != nil {
					return synclet.ConfigError(err)
				}
				target = &streamTarget{topic: topic}
				streams[desc] = target
			}
			r, err := target.record(rec)
			if err != nil {
				return err
			}
			if err := p.produce(ctx, r); err != nil {
				return err
			}
		case synclet.MessageTypeState:
			if msg.State == nil {
				return errors.New("STATE message without state")
			}
			if err := p.flush(ctx); err != nil {
				return err
			}
			if err := em.State(*msg.State); err != nil {
				return err
			}
		}
	}
}

func (t *streamTarget) record(rec *synclet.RecordMessage) (*kgo.Record, error) {
	id := newID()
	value, err := encodeValue(id, rec)
	if err != nil {
		return nil, err
	}
	key, err := messageKey(t.primaryKey, rec.Data)
	if err != nil {
		return nil, fmt.Errorf("key of a record of stream %s: %w", streamLabel(rec.Namespace, rec.Stream), err)
	}
	if key == nil {
		key = []byte(id)
	}
	return &kgo.Record{Topic: t.topic, Key: key, Value: value}, nil
}

// producer produces records asynchronously and keeps the first failure, which
// fails the write at the next record or flush.
type producer struct {
	cl   *kgo.Client
	sync bool

	mu  sync.Mutex
	err error
}

func (p *producer) produce(ctx context.Context, r *kgo.Record) error {
	if err := p.failure(); err != nil {
		return err
	}
	if p.sync {
		if err := p.cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			return fmt.Errorf("produce to topic %s: %w", r.Topic, err)
		}
		return nil
	}
	p.cl.Produce(ctx, r, p.done)
	return nil
}

func (p *producer) done(r *kgo.Record, err error) {
	if err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = fmt.Errorf("produce to topic %s: %w", r.Topic, err)
	}
}

func (p *producer) failure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// flush waits until every produced record is acknowledged or failed.
func (p *producer) flush(ctx context.Context) error {
	if err := p.cl.Flush(ctx); err != nil {
		return err
	}
	return p.failure()
}
