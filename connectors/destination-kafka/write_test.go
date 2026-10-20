package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestWriteProducesEnvelopesToStreamTopics(t *testing.T) {
	c := startCluster(t)
	out := runWrite(t, Destination{}, plainConfig(c, ""), catalog(t, "public.users:id", "public.events"),
		rec("public", "users", `{"id":1,"name":"Ada"}`),
		rec("public", "events", `{"kind":"click","n":1.50}`),
		rec("public", "users", `{"id":2,"name":"Grace"}`),
		state("users", 2),
	)
	require.NoError(t, out.Err)
	require.Len(t, out.States(), 1)

	users := consume(t, bootstrap(c), "public_users", 2)
	byKey := map[string]envelope{}
	for _, m := range users {
		byKey[m.Key] = m.Value
		assert.Equal(t, "users", m.Value.Stream)
		assert.Equal(t, int64(1700000000000), m.Value.EmittedAt)
		_, err := uuid.Parse(m.Value.ID)
		assert.NoError(t, err)
	}
	require.Contains(t, byKey, "1")
	require.Contains(t, byKey, "2")
	assert.JSONEq(t, `{"id":1,"name":"Ada"}`, string(byKey["1"].Data))
	assert.JSONEq(t, `{"id":2,"name":"Grace"}`, string(byKey["2"].Data))
	assert.NotEqual(t, byKey["1"].ID, byKey["2"].ID)

	events := consume(t, bootstrap(c), "public_events", 1)
	assert.Equal(t, events[0].Value.ID, events[0].Key, "a stream without primary key is keyed by the record id")
	assert.Equal(t, `{"kind":"click","n":1.50}`, string(events[0].Value.Data), "data is passed through unchanged")
}

func TestWriteMessageValueShape(t *testing.T) {
	c := startCluster(t)
	out := runWrite(t, Destination{}, plainConfig(c, `"topic_pattern":"raw"`), catalog(t, "users"),
		rec("", "users", `{"id":7}`))
	require.NoError(t, out.Err)
	msgs := consume(t, bootstrap(c), "raw", 1)
	raw, err := json.Marshal(msgs[0].Value)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	assert.ElementsMatch(t, []string{"_airbyte_ab_id", "_airbyte_stream", "_airbyte_emitted_at", "_airbyte_data"}, keys(fields))
}

func keys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestWriteSameKeyKeepsOrderInOnePartition(t *testing.T) {
	c := startCluster(t)
	lines := make([]string, 0, 50)
	for i := range 50 {
		lines = append(lines, rec("", "users", `{"id":"same","seq":`+itoa(i)+`}`))
	}
	out := runWrite(t, Destination{}, plainConfig(c, `"topic_pattern":"{stream}"`), catalog(t, "users:id"), lines...)
	require.NoError(t, out.Err)
	msgs := consume(t, bootstrap(c), "users", 50)
	for i, m := range msgs {
		assert.Equal(t, "same", m.Key)
		var data struct{ Seq int }
		require.NoError(t, json.Unmarshal(m.Value.Data, &data))
		assert.Equal(t, i, data.Seq)
	}
}

func itoa(i int) string { b, _ := json.Marshal(i); return string(b) }

func TestWriteStreamNotInCatalog(t *testing.T) {
	c := startCluster(t)
	out := runWrite(t, Destination{}, plainConfig(c, ""), `{"streams":[]}`, rec("crm", "contacts", `{"id":1}`))
	require.NoError(t, out.Err)
	msgs := consume(t, bootstrap(c), "crm_contacts", 1)
	assert.Equal(t, msgs[0].Value.ID, msgs[0].Key)
}

func TestWriteEmptyNamespace(t *testing.T) {
	c := startCluster(t)
	out := runWrite(t, Destination{}, plainConfig(c, ""), catalog(t, "users"), rec("", "users", `{"id":1}`))
	require.NoError(t, out.Err)
	consume(t, bootstrap(c), "_users", 1)
}

// stateProbe is an emitter output that records, for every STATE written, how
// many messages the cluster holds in topic at that moment.
type stateProbe struct {
	c     *kfake.Cluster
	topic string

	mu     sync.Mutex
	buf    bytes.Buffer
	counts []int64
}

func (p *stateProbe) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if bytes.Contains(b, []byte(`"type":"STATE"`)) {
		p.counts = append(p.counts, topicSize(p.c, p.topic))
	}
	return p.buf.Write(b)
}

func TestWriteStateEchoedOnlyAfterPriorRecordsAreAcked(t *testing.T) {
	for _, extra := range []string{`"linger_ms":"1000"`, `"sync_producer":true`, `"acks":"all","enable_idempotence":true`} {
		t.Run(extra, func(t *testing.T) {
			c := startCluster(t)
			var cfg Config
			require.NoError(t, json.Unmarshal([]byte(plainConfig(c, `"topic_pattern":"{stream}",`+extra)), &cfg))
			var lines []string
			for i := range 30 {
				lines = append(lines, rec("", "users", `{"id":`+itoa(i)+`}`))
				if i%10 == 9 {
					lines = append(lines, state("users", i))
				}
			}
			var cat synclet.ConfiguredCatalog
			require.NoError(t, json.Unmarshal([]byte(catalog(t, "users:id")), &cat))
			probe := &stateProbe{c: c, topic: "users"}
			in := synclet.NewMessageReader(strings.NewReader(strings.Join(lines, "\n") + "\n"))
			require.NoError(t, Destination{}.Write(t.Context(), cfg, &cat, in, synclet.NewEmitter(probe)))
			assert.Equal(t, []int64{10, 20, 30}, probe.counts)
		})
	}
}

func failProduces(c *kfake.Cluster, code int16) {
	c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		r := req.(*kmsg.ProduceRequest)
		resp := r.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range r.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic, st.TopicID = rt.Topic, rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
}

func TestWriteFailedProduceFailsBeforeState(t *testing.T) {
	c := startCluster(t)
	failProduces(c, kerr.TopicAuthorizationFailed.Code)
	out := runWrite(t, Destination{}, plainConfig(c, ""), catalog(t, "users"),
		rec("", "users", `{"id":1}`), state("users", 1), rec("", "users", `{"id":2}`), state("users", 2))
	require.Error(t, out.Err)
	assert.Empty(t, out.States())
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Contains(t, traces[0].Message, "produce to topic _users")
	assert.Contains(t, traces[0].Message, "TOPIC_AUTHORIZATION_FAILED")
}

func TestWriteFailedProduceAtEndFails(t *testing.T) {
	c := startCluster(t)
	failProduces(c, kerr.TopicAuthorizationFailed.Code)
	out := runWrite(t, Destination{}, plainConfig(c, ""), catalog(t, "users"), rec("", "users", `{"id":1}`))
	require.Error(t, out.Err)
	assert.Contains(t, out.Err.Error(), "TOPIC_AUTHORIZATION_FAILED")
}

func TestWriteFailedSyncProduceStopsAtTheRecord(t *testing.T) {
	c := startCluster(t)
	failProduces(c, kerr.TopicAuthorizationFailed.Code)
	out := runWrite(t, Destination{}, plainConfig(c, `"sync_producer":true`), catalog(t, "users"),
		rec("", "users", `{"id":1}`), state("users", 1))
	require.Error(t, out.Err)
	assert.Empty(t, out.States())
}

func TestWriteRecordLargerThanMaxRequestSizeFails(t *testing.T) {
	c := startCluster(t)
	big := `{"blob":"` + strings.Repeat("x", 4096) + `"}`
	out := runWrite(t, Destination{}, plainConfig(c, `"max_request_size":2048`), catalog(t, "users"),
		rec("", "users", `{"id":1}`), state("users", 1), rec("", "users", big), state("users", 2))
	require.Error(t, out.Err)
	require.Len(t, out.States(), 1)
	assert.Contains(t, out.Err.Error(), "MESSAGE_TOO_LARGE")
}

func TestWriteRejectsNonAppendStreams(t *testing.T) {
	c := startCluster(t)
	for _, mode := range []synclet.DestinationSyncMode{synclet.DestinationSyncModeOverwrite, synclet.DestinationSyncModeAppendDedup} {
		cat := strings.Replace(catalog(t, "users"), `"append"`, `"`+string(mode)+`"`, 1)
		out := runWrite(t, Destination{}, plainConfig(c, ""), cat, rec("", "users", `{"id":1}`))
		require.Error(t, out.Err)
		traces := out.ErrorTraces()
		require.Len(t, traces, 1)
		assert.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
		assert.Contains(t, traces[0].Message, "destination sync mode "+string(mode)+" is not supported")
	}
}

func TestWriteInvalidConfig(t *testing.T) {
	out := runWrite(t, Destination{}, `{"bootstrap_servers":"localhost:1","topic_pattern":"t","acks":"2"}`, `{"streams":[]}`)
	require.Error(t, out.Err)
	traces := out.ErrorTraces()
	require.Len(t, traces, 1)
	assert.Equal(t, synclet.FailureTypeConfigError, traces[0].FailureType)
}

func TestWriteCompressions(t *testing.T) {
	for _, codec := range compressions {
		t.Run(codec, func(t *testing.T) {
			c := startCluster(t)
			out := runWrite(t, Destination{}, plainConfig(c, `"topic_pattern":"{stream}","compression_type":"`+codec+`"`),
				catalog(t, "users:id"), rec("", "users", `{"id":1}`), rec("", "users", `{"id":2}`), state("users", 2))
			require.NoError(t, out.Err)
			require.Len(t, out.States(), 1)
			consume(t, bootstrap(c), "users", 2)
		})
	}
}

func TestWriteAcks(t *testing.T) {
	for _, acks := range []string{acksNone, acksLeader, acksAll} {
		t.Run(acks, func(t *testing.T) {
			c := startCluster(t)
			out := runWrite(t, Destination{}, plainConfig(c, `"topic_pattern":"{stream}","acks":"`+acks+`"`),
				catalog(t, "users"), rec("", "users", `{"id":1}`), state("users", 1))
			require.NoError(t, out.Err)
			require.Len(t, out.States(), 1)
			consume(t, bootstrap(c), "users", 1)
		})
	}
}

func TestWriteEmptyInput(t *testing.T) {
	c := startCluster(t)
	out := runWrite(t, Destination{}, plainConfig(c, ""), catalog(t, "users"))
	require.NoError(t, out.Err)
	assert.Empty(t, out.States())
}

func TestWriteGlobalAndLegacyStatesEchoed(t *testing.T) {
	c := startCluster(t)
	out := runWrite(t, Destination{}, plainConfig(c, ""), catalog(t, "users"),
		rec("", "users", `{"id":1}`),
		`{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{"lsn":5},"stream_states":[]}}}`,
		`{"type":"STATE","state":{"type":"LEGACY","data":{"cursor":1}}}`,
		`{"type":"LOG","log":{"level":"INFO","message":"ignored"}}`,
	)
	require.NoError(t, out.Err)
	states := out.States()
	require.Len(t, states, 2)
	assert.Equal(t, synclet.StateTypeGlobal, states[0].Type)
	assert.JSONEq(t, `{"cursor":1}`, string(states[1].Data))
}
