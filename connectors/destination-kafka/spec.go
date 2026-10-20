package main

import (
	"context"
	"strings"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

const booleanType synclet.PropType = "boolean"

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func constant(value string) synclet.PropertySpec {
	return synclet.PropertySpec{PropertyType: typed(synclet.String), Const: value, Default: value, Order: order(0)}
}

func saslOption(title, protocol string) synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        title,
		PropertyType: typed(synclet.Object),
		Required:     []synclet.PropertyName{"security_protocol", "sasl_mechanism"},
		Properties: map[synclet.PropertyName]synclet.PropertySpec{
			"security_protocol": constant(protocol),
			"sasl_mechanism": {
				Title:        "SASL Mechanism",
				Description:  "One of " + strings.Join([]string{mechanismPlain, mechanismSCRAMSHA256, mechanismSCRAMSHA512}, ", ") + ".",
				PropertyType: typed(synclet.String), Default: mechanismPlain,
				Examples: []string{mechanismPlain, mechanismSCRAMSHA256, mechanismSCRAMSHA512}, Order: order(1),
			},
			"sasl_username": {
				Title: "SASL Username", PropertyType: typed(synclet.String), Order: order(2),
			},
			"sasl_password": {
				Title: "SASL Password", PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
			},
			"sasl_jaas_config": {
				Title:        "SASL JAAS Config",
				Description:  "Kafka sasl.jaas.config value whose username and password are used when SASL Username is empty, as in Airbyte's destination-kafka.",
				PropertyType: typed(synclet.String), IsSecret: true, Default: "",
				Examples: []string{`org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="secret";`},
				Order:    order(4),
			},
		},
	}
}

func integer(title, description string, def any, pos int) synclet.PropertySpec {
	return synclet.PropertySpec{Title: title, Description: description, PropertyType: typed(synclet.Integer), Default: def, Order: order(pos)}
}

// Spec returns the connector's capabilities and configuration schema.
func (Destination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:              "https://github.com/synclet-io/connectors/tree/main/connectors/destination-kafka",
		ProtocolVersion:               "0.5.2",
		SupportsIncremental:           true,
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{synclet.DestinationSyncModeAppend},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Kafka Destination Spec",
			Description: "Produces every record as a JSON message to a Kafka topic per stream.",
			Type:        "object",
			Required:    []synclet.PropertyName{"bootstrap_servers", "topic_pattern"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"bootstrap_servers": {
					Title:        "Bootstrap Servers",
					Description:  "Comma-separated host:port pairs of the brokers the client first connects to; it discovers the rest of the cluster from them.",
					PropertyType: typed(synclet.String), Examples: []string{"kafka-broker1:9092,kafka-broker2:9092"}, Order: order(0),
				},
				"topic_pattern": {
					Title: "Topic Pattern",
					Description: "Topic of each stream; {namespace} and {stream} are replaced by the stream's namespace and name. " +
						"As in Airbyte's destination-kafka, every character of the result other than letters, digits and underscores becomes an underscore.",
					PropertyType: typed(synclet.String), Examples: []string{"sample.topic", "{namespace}.{stream}.sample"}, Order: order(1),
				},
				"test_topic": {
					Title:        "Test Topic",
					Description:  "Topic check produces one message to; empty only connects to the brokers.",
					PropertyType: typed(synclet.String), Examples: []string{"test.topic"}, Order: order(2),
				},
				"sync_producer": {
					Title:        "Sync Producer",
					Description:  "Wait for each record to be acknowledged before producing the next one. Slow; records are acknowledged before each state either way.",
					PropertyType: typed(booleanType), Default: false, Order: order(3),
				},
				"protocol": {
					Title:        "Protocol",
					Description:  "Protocol used to communicate with the brokers.",
					PropertyType: typed(synclet.Object),
					Default:      map[string]string{"security_protocol": protocolPlaintext},
					Order:        order(4),
					OneOf: []synclet.PropertySpec{
						{
							Title: "PLAINTEXT", PropertyType: typed(synclet.Object),
							Required:   []synclet.PropertyName{"security_protocol"},
							Properties: map[synclet.PropertyName]synclet.PropertySpec{"security_protocol": constant(protocolPlaintext)},
						},
						saslOption("SASL PLAINTEXT", protocolSASLPlaintext),
						saslOption("SASL SSL", protocolSASLSSL),
					},
				},
				"client_id": {
					Title:        "Client ID",
					Description:  "Name the client gives the brokers, for their logs and quotas.",
					PropertyType: typed(synclet.String), Default: defaultClientID, Examples: []string{defaultClientID}, Order: order(5),
				},
				"acks": {
					Title:        "ACKs",
					Description:  "Acknowledgments the leader must receive before a write counts: 0 (none), 1 (the leader) or all (every in-sync replica).",
					PropertyType: typed(synclet.String), Default: defaultAcks, Examples: []string{acksNone, acksLeader, acksAll}, Order: order(6),
				},
				"enable_idempotence": {
					Title:        "Enable Idempotence",
					Description:  "Let the brokers drop duplicates of retried writes; needs acks all.",
					PropertyType: typed(booleanType), Default: false, Order: order(7),
				},
				"compression_type": {
					Title:        "Compression Type",
					Description:  "Compression of the produced batches: " + strings.Join(compressions, ", ") + ".",
					PropertyType: typed(synclet.String), Default: defaultCompression, Examples: compressions, Order: order(8),
				},
				"linger_ms": {
					Title:        "Linger ms",
					Description:  "Milliseconds a partition waits for more records before its batch is sent.",
					PropertyType: typed(synclet.String), Default: "0", Examples: []string{"0", "5"}, Order: order(9),
				},
				"max_request_size": integer("Max Request Size",
					"Largest record batch in bytes, before compression; a record larger than it fails the sync. Up to 1 GiB.",
					defaultMaxRequestSize, 10),
				"max_in_flight_requests_per_connection": integer("Max in Flight Requests per Connection",
					"Unacknowledged produce requests per broker. With idempotence the client allows 5.",
					defaultMaxInFlight, 11),
				"retries": integer("Retries",
					"Times a failed batch is retried; the default retries until the delivery timeout.",
					unlimitedRetries, 12),
				"request_timeout_ms": integer("Request Timeout",
					"Milliseconds the brokers have to answer a produce request.",
					defaultRequestTimeout, 13),
				"delivery_timeout_ms": integer("Delivery Timeout",
					"Milliseconds a record may take from being produced to being acknowledged, retries included; 0 is no limit.",
					defaultDeliveryTimeout, 14),
				"buffer_memory": {
					Title:        "Buffer Memory",
					Description:  "Bytes of records buffered while waiting to be sent; producing waits while the buffer is full.",
					PropertyType: typed(synclet.String), Default: "33554432", Examples: []string{"33554432"}, Order: order(15),
				},
			}},
		},
	}, nil
}
