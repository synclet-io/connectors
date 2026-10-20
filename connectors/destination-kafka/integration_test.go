package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	synclet "github.com/synclet-io/synclet-go-sdk"
)

// TestIntegration runs check and write against a real broker, such as
// `docker run -d -p 19092:19092 redpandadata/redpanda redpanda start --mode dev-container
// --kafka-addr 0.0.0.0:19092 --advertise-kafka-addr 127.0.0.1:19092` with
// KAFKA_TEST_BOOTSTRAP_SERVERS=127.0.0.1:19092. The broker must create topics
// on first use.
func TestIntegration(t *testing.T) {
	seeds := os.Getenv("KAFKA_TEST_BOOTSTRAP_SERVERS")
	if seeds == "" {
		t.Skip("KAFKA_TEST_BOOTSTRAP_SERVERS is not set")
	}
	run := fmt.Sprintf("synclet_it_%d", time.Now().UnixNano())
	for _, extra := range []string{
		`"acks":"all","enable_idempotence":true,"compression_type":"zstd","linger_ms":"5"`,
		`"acks":"1","compression_type":"gzip","sync_producer":true`,
		`"acks":"0","compression_type":"snappy"`,
		`"compression_type":"lz4"`,
	} {
		t.Run(extra, func(t *testing.T) {
			ns := fmt.Sprintf("%s_%d", run, len(extra))
			config := fmt.Sprintf(`{"bootstrap_servers":%q,"topic_pattern":"{namespace}.{stream}","test_topic":%q,%s}`, seeds, ns+"_check", extra)

			status := runCheck(t, Destination{}, config)
			require.Equal(t, synclet.ConnectionStatusSucceeded, status.Status, status.Message)

			lines := make([]string, 0, 210)
			for i := range 200 {
				lines = append(lines, rec(ns, "users", fmt.Sprintf(`{"id":%d,"name":"user %d"}`, i, i)))
				if i%50 == 49 {
					lines = append(lines, state("users", i))
				}
			}
			out := runWrite(t, Destination{}, config, catalog(t, ns+".users:id"), lines...)
			require.NoError(t, out.Err)
			require.Len(t, out.States(), 4)

			msgs := consume(t, seeds, ns+"_users", 200)
			seen := map[string]bool{}
			for _, m := range msgs {
				seen[m.Key] = true
				assert.Equal(t, "users", m.Value.Stream)
			}
			assert.Len(t, seen, 200)
		})
	}
}
