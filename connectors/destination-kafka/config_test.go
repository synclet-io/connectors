package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func parseSettings(t *testing.T, config string) (settings, error) {
	t.Helper()
	var cfg Config
	require.NoError(t, json.Unmarshal([]byte(config), &cfg))
	return cfg.settings()
}

func TestSettingsDefaults(t *testing.T) {
	s, err := parseSettings(t, `{"bootstrap_servers":" b1:9092, b2:9092 ,","topic_pattern":"{stream}"}`)
	require.NoError(t, err)
	assert.Equal(t, settings{
		seeds:          []string{"b1:9092", "b2:9092"},
		topicPattern:   "{stream}",
		clientID:       defaultClientID,
		acks:           acksLeader,
		compression:    "none",
		maxRequestSize: 1048576,
		maxInFlight:    5,
		retries:        unlimitedRetries,
		requestTimeout: 30 * time.Second,
		delivery:       2 * time.Minute,
		bufferMemory:   33554432,
	}, s)
}

// airbyteConfig is a configuration as Airbyte's destination-kafka form saves
// it, with every key it requires.
const airbyteConfig = `{
	"bootstrap_servers": "kafka:9092",
	"topic_pattern": "{namespace}.{stream}",
	"test_topic": "test.topic",
	"sync_producer": true,
	"protocol": {"security_protocol": "SASL_SSL", "sasl_mechanism": "SCRAM-SHA-512",
		"sasl_jaas_config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" password=\"p\\\"w\";"},
	"client_id": "airbyte-producer",
	"acks": "all",
	"enable_idempotence": true,
	"compression_type": "zstd",
	"batch_size": 16384,
	"linger_ms": "5",
	"max_in_flight_requests_per_connection": 5,
	"client_dns_lookup": "use_all_dns_ips",
	"buffer_memory": "33554432",
	"max_request_size": 2097152,
	"retries": 2147483647,
	"socket_connection_setup_timeout_ms": "10000",
	"socket_connection_setup_timeout_max_ms": "30000",
	"max_block_ms": "60000",
	"request_timeout_ms": 30000,
	"delivery_timeout_ms": 120000,
	"send_buffer_bytes": -1,
	"receive_buffer_bytes": -1
}`

func TestSettingsAirbyteConfig(t *testing.T) {
	s, err := parseSettings(t, airbyteConfig)
	require.NoError(t, err)
	assert.Equal(t, settings{
		seeds:          []string{"kafka:9092"},
		topicPattern:   "{namespace}.{stream}",
		testTopic:      "test.topic",
		syncProducer:   true,
		tls:            true,
		mechanism:      mechanismSCRAMSHA512,
		username:       "alice",
		password:       `p"w`,
		clientID:       "airbyte-producer",
		acks:           acksAll,
		idempotent:     true,
		compression:    "zstd",
		linger:         5 * time.Millisecond,
		maxRequestSize: 2097152,
		maxInFlight:    5,
		retries:        unlimitedRetries,
		requestTimeout: 30 * time.Second,
		delivery:       2 * time.Minute,
		bufferMemory:   33554432,
	}, s)
}

func TestSettingsNumbersAsStringsOrNumbers(t *testing.T) {
	s, err := parseSettings(t, `{"bootstrap_servers":"k:1","topic_pattern":"t","acks":1,"linger_ms":20,"buffer_memory":67108864,"retries":"3","max_request_size":"4096"}`)
	require.NoError(t, err)
	assert.Equal(t, acksLeader, s.acks)
	assert.Equal(t, 20*time.Millisecond, s.linger)
	assert.Equal(t, 67108864, s.bufferMemory)
	assert.Equal(t, 3, s.retries)
	assert.Equal(t, int32(4096), s.maxRequestSize)

	s, err = parseSettings(t, `{"bootstrap_servers":"k:1","topic_pattern":"t","acks":-1,"retries":9999999999,"linger_ms":""}`)
	require.NoError(t, err)
	assert.Equal(t, acksAll, s.acks)
	assert.Equal(t, unlimitedRetries, s.retries)
	assert.Zero(t, s.linger)
}

func TestSettingsSASLCredentials(t *testing.T) {
	s, err := parseSettings(t, `{"bootstrap_servers":"k:1","topic_pattern":"t","protocol":{"security_protocol":"SASL_PLAINTEXT","sasl_username":"u","sasl_password":"p","sasl_jaas_config":"x username=\"j\" password=\"k\";"}}`)
	require.NoError(t, err)
	assert.Equal(t, mechanismPlain, s.mechanism, "PLAIN is the default mechanism")
	assert.False(t, s.tls)
	assert.Equal(t, "u", s.username, "username and password win over the JAAS config")
	assert.Equal(t, "p", s.password)

	s, err = parseSettings(t, `{"bootstrap_servers":"k:1","topic_pattern":"t","protocol":{"security_protocol":"PLAINTEXT","sasl_username":"u"}}`)
	require.NoError(t, err)
	assert.Empty(t, s.mechanism, "PLAINTEXT ignores SASL keys")
}

func TestSettingsErrors(t *testing.T) {
	for config, want := range map[string]string{
		`{"topic_pattern":"t"}`:                                                                                                                     "bootstrap_servers is empty",
		`{"bootstrap_servers":" , ","topic_pattern":"t"}`:                                                                                           "bootstrap_servers is empty",
		`{"bootstrap_servers":"k:1"}`:                                                                                                               "topic_pattern is empty",
		`{"bootstrap_servers":"k:1","topic_pattern":"t","test_topic":"a b"}`:                                                                        `test_topic: topic name "a b" has ' '`,
		`{"bootstrap_servers":"k:1","topic_pattern":"t","acks":"2"}`:                                                                                `acks "2" is not one of 0, 1, all`,
		`{"bootstrap_servers":"k:1","topic_pattern":"t","enable_idempotence":true}`:                                                                 "enable_idempotence needs acks all, not 1",
		`{"bootstrap_servers":"k:1","topic_pattern":"t","compression_type":"brotli"}`:                                                               `compression_type "brotli" is not one of none, gzip, snappy, lz4, zstd`,
		`{"bootstrap_servers":"k:1","topic_pattern":"t","linger_ms":"soon"}`:                                                                        `linger_ms "soon" is not a whole number`,
		`{"bootstrap_servers":"k:1","topic_pattern":"t","linger_ms":-1}`:                                                                            "linger_ms -1 is less than 0",
		`{"bootstrap_servers":"k:1","topic_pattern":"t","max_request_size":100}`:                                                                    "max_request_size 100 is less than 1024",
		`{"bootstrap_servers":"k:1","topic_pattern":"t","max_request_size":2147483647}`:                                                             "max_request_size 2147483647 is larger than 1073741824",
		`{"bootstrap_servers":"k:1","topic_pattern":"t","max_in_flight_requests_per_connection":0}`:                                                 "max_in_flight_requests_per_connection 0 is less than 1",
		`{"bootstrap_servers":"k:1","topic_pattern":"t","buffer_memory":"1024"}`:                                                                    "buffer_memory 1024 is less than 1048576",
		`{"bootstrap_servers":"k:1","topic_pattern":"t","protocol":{"security_protocol":"SSL"}}`:                                                    `security_protocol "SSL" is not one of PLAINTEXT, SASL_PLAINTEXT, SASL_SSL`,
		`{"bootstrap_servers":"k:1","topic_pattern":"t","protocol":{"security_protocol":"SASL_SSL","sasl_mechanism":"GSSAPI","sasl_username":"u"}}`: `sasl_mechanism "GSSAPI" is not one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512`,
		`{"bootstrap_servers":"k:1","topic_pattern":"t","protocol":{"security_protocol":"SASL_SSL"}}`:                                               "SASL_SSL needs sasl_username and sasl_password, or sasl_jaas_config",
		`{"bootstrap_servers":"k:1","topic_pattern":"t","protocol":{"security_protocol":"SASL_SSL","sasl_jaas_config":"Module required;"}}`:         `sasl_jaas_config: it has no username="..." and password="..." options`,
	} {
		_, err := parseSettings(t, config)
		require.Error(t, err, config)
		assert.Contains(t, err.Error(), want, config)
	}
}

func TestClientOptions(t *testing.T) {
	s, err := parseSettings(t, `{"bootstrap_servers":"k:1","topic_pattern":"t","linger_ms":"7","max_request_size":2097152,"retries":3,
		"max_in_flight_requests_per_connection":2,"request_timeout_ms":5000,"delivery_timeout_ms":9000,"buffer_memory":"4194304","client_id":"me"}`)
	require.NoError(t, err)
	cl, err := kgo.NewClient(s.clientOptions(nil)...)
	require.NoError(t, err)
	defer cl.Close()
	assert.Equal(t, 7*time.Millisecond, cl.OptValue(kgo.ProducerLinger))
	assert.Equal(t, int64(3), cl.OptValue(kgo.RecordRetries))
	assert.Equal(t, 2, cl.OptValue(kgo.MaxProduceRequestsInflightPerBroker))
	assert.Equal(t, 5*time.Second, cl.OptValue(kgo.ProduceRequestTimeout))
	assert.Equal(t, 9*time.Second, cl.OptValue(kgo.RecordDeliveryTimeout))
	assert.Equal(t, int64(4194304), cl.OptValue(kgo.MaxBufferedBytes))
	assert.Equal(t, true, cl.OptValue(kgo.DisableIdempotentWrite))
	assert.Equal(t, kgo.LeaderAck(), cl.OptValue(kgo.RequiredAcks))
	assert.Equal(t, "me", cl.OptValue(kgo.ClientID))

	s, err = parseSettings(t, `{"bootstrap_servers":"k:1","topic_pattern":"t","acks":"all","enable_idempotence":true,"compression_type":"lz4"}`)
	require.NoError(t, err)
	cl, err = kgo.NewClient(s.clientOptions(nil)...)
	require.NoError(t, err)
	defer cl.Close()
	assert.Equal(t, false, cl.OptValue(kgo.DisableIdempotentWrite))
	assert.Equal(t, kgo.AllISRAcks(), cl.OptValue(kgo.RequiredAcks))
	assert.Equal(t, []kgo.CompressionCodec{kgo.Lz4Compression()}, cl.OptValue(kgo.ProducerBatchCompression))
}

func TestParseJAAS(t *testing.T) {
	user, pass, err := parseJAAS(`org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="se=cr \"et\\";`)
	require.NoError(t, err)
	assert.Equal(t, "alice", user)
	assert.Equal(t, `se=cr "et\`, pass)
}
