package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/testkit"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// startCluster runs an in-process Kafka that creates topics on first use.
func startCluster(t *testing.T, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	opts = append([]kfake.Opt{kfake.NumBrokers(1), kfake.AllowAutoTopicCreation(), kfake.DefaultNumPartitions(3)}, opts...)
	c, err := kfake.NewCluster(opts...)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func bootstrap(c *kfake.Cluster) string { return strings.Join(c.ListenAddrs(), ",") }

func plainConfig(c *kfake.Cluster, extra string) string {
	cfg := fmt.Sprintf(`{"bootstrap_servers":%q,"topic_pattern":"{namespace}.{stream}"`, bootstrap(c))
	if extra != "" {
		cfg += "," + extra
	}
	return cfg + "}"
}

func rec(namespace, stream, data string) string {
	ns := ""
	if namespace != "" {
		ns = fmt.Sprintf(`,"namespace":%q`, namespace)
	}
	return fmt.Sprintf(`{"type":"RECORD","record":{"stream":%q%s,"data":%s,"emitted_at":1700000000000}}`, stream, ns, data)
}

func state(stream string, cursor int) string {
	return fmt.Sprintf(`{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"name":%q},"stream_state":{"cursor":%d}}}}`, stream, cursor)
}

// catalog is a configured catalog of append streams; each entry is
// "namespace.name" or "name", with an optional ":pk1,pk2" primary key.
func catalog(t *testing.T, streams ...string) string {
	t.Helper()
	var cat synclet.ConfiguredCatalog
	for _, s := range streams {
		name, pk, _ := strings.Cut(s, ":")
		ns := ""
		if before, after, ok := strings.Cut(name, "."); ok {
			ns, name = before, after
		}
		cs := synclet.ConfiguredStream{
			Stream:              synclet.Stream{Name: name, Namespace: ns, JSONSchema: json.RawMessage(`{"type":"object"}`)},
			SyncMode:            synclet.SyncModeIncremental,
			DestinationSyncMode: synclet.DestinationSyncModeAppend,
		}
		for _, f := range strings.Split(pk, ",") {
			if f != "" {
				cs.PrimaryKey = append(cs.PrimaryKey, []string{f})
			}
		}
		cat.Streams = append(cat.Streams, cs)
	}
	b, err := json.Marshal(cat)
	require.NoError(t, err)
	return string(b)
}

func runWrite(t *testing.T, d Destination, config, cat string, lines ...string) *testkit.Output {
	t.Helper()
	out, err := testkit.RunDestination(t.Context(), d, synclet.Request{
		Command: synclet.CommandWrite,
		Config:  json.RawMessage(config),
		Catalog: json.RawMessage(cat),
		Input:   strings.NewReader(strings.Join(lines, "\n") + "\n"),
	})
	require.NoError(t, err)
	return out
}

func runCheck(t *testing.T, d Destination, config string) *synclet.ConnectionStatus {
	t.Helper()
	out, err := testkit.RunDestination(t.Context(), d, synclet.Request{Command: synclet.CommandCheck, Config: json.RawMessage(config)})
	require.NoError(t, err)
	require.NoError(t, out.Err)
	status := out.ConnectionStatus()
	require.NotNil(t, status)
	return status
}

// consumed is one message read back from a topic.
type consumed struct {
	Key   string
	Value envelope
}

// consume reads want messages of topic from the start, sorted by partition
// and offset within a partition kept in order.
func consume(t *testing.T, seeds string, topic string, want int, opts ...kgo.Opt) []consumed {
	t.Helper()
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(strings.Split(seeds, ",")...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var recs []*kgo.Record
	for len(recs) < want {
		fetches := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "got %d of %d messages of %s", len(recs), want, topic)
		fetches.EachError(func(_ string, _ int32, err error) { require.NoError(t, err) })
		recs = append(recs, fetches.Records()...)
	}
	require.Len(t, recs, want)
	slices.SortStableFunc(recs, func(a, b *kgo.Record) int {
		if a.Partition != b.Partition {
			return int(a.Partition - b.Partition)
		}
		return int(a.Offset - b.Offset)
	})
	out := make([]consumed, len(recs))
	for i, r := range recs {
		out[i].Key = string(r.Key)
		require.NoError(t, json.Unmarshal(r.Value, &out[i].Value), string(r.Value))
	}
	return out
}

// topicSize is the number of messages in topic.
func topicSize(c *kfake.Cluster, topic string) int64 {
	var n int64
	for _, p := range c.PartitionInfos(topic) {
		n += p.HighWatermark - p.LogStartOffset
	}
	return n
}
