package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// clientOptions are the franz-go options of s. rootCAs replaces the system
// roots for SASL_SSL when it is set.
func (s settings) clientOptions(rootCAs *x509.CertPool) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.seeds...),
		kgo.ClientID(s.clientID),
		kgo.AllowAutoTopicCreation(),
		kgo.ProducerLinger(s.linger),
		kgo.ProducerBatchMaxBytes(s.maxRequestSize),
		kgo.BrokerMaxWriteBytes(max(s.maxRequestSize, 100<<20)),
		kgo.ProduceRequestTimeout(s.requestTimeout),
		kgo.RecordDeliveryTimeout(s.delivery),
		kgo.MaxBufferedBytes(s.bufferMemory),
	}
	switch s.acks {
	case acksNone:
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	case acksLeader:
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	if !s.idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite(), kgo.MaxProduceRequestsInflightPerBroker(s.maxInFlight))
	}
	if s.retries < unlimitedRetries {
		opts = append(opts, kgo.RecordRetries(s.retries))
	}
	switch s.compression {
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	default:
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	}
	if s.tls {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: rootCAs}))
	}
	switch s.mechanism {
	case mechanismPlain:
		opts = append(opts, kgo.SASL(plain.Auth{User: s.username, Pass: s.password}.AsMechanism()))
	case mechanismSCRAMSHA256:
		opts = append(opts, kgo.SASL(scram.Auth{User: s.username, Pass: s.password}.AsSha256Mechanism()))
	case mechanismSCRAMSHA512:
		opts = append(opts, kgo.SASL(scram.Auth{User: s.username, Pass: s.password}.AsSha512Mechanism()))
	}
	return opts
}

func (d Destination) newClient(ctx context.Context, s settings) (*kgo.Client, error) {
	opts := s.clientOptions(d.rootCAs)
	opts = append(opts, kgo.WithContext(ctx))
	return kgo.NewClient(opts...)
}
