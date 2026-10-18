package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// propBoolean is the JSON schema type the SDK has no constant for.
const propBoolean synclet.PropType = "boolean"

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

// Spec returns the connector's capabilities and configuration schema.
func (Destination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-clickhouse",
		ProtocolVersion:  "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "ClickHouse Destination Spec",
			Description: "Writes each stream into a typed MergeTree table of one database: overwrite, append or append_dedup.",
			Type:        "object",
			Required:    []synclet.PropertyName{"host", "database", "username"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"host": {
					Title: "Host", Description: "Hostname of the ClickHouse server.",
					PropertyType: typed(synclet.String), Order: order(0),
				},
				"port": {
					Title: "Port", Description: "Port of the server; when unset 9000 (native), 9440 (native with TLS), 8123 (http) or 8443 (http with TLS).",
					PropertyType: typed(synclet.Integer), Examples: []string{"9000", "9440", "8123", "8443"}, Order: order(1),
				},
				"protocol": {
					Title: "Protocol", Description: "Native TCP protocol or HTTP interface.",
					PropertyType: typed(synclet.String), Default: protocolNative, Order: order(2),
					OneOf: []synclet.PropertySpec{
						{Title: "Native", Const: protocolNative},
						{Title: "HTTP", Const: protocolHTTP},
					},
				},
				"database": {
					Title: "Database", Description: "Database every table is written to; it must exist.",
					PropertyType: typed(synclet.String), Order: order(3),
				},
				"username": {
					Title: "Username", Description: "User to log in as.",
					PropertyType: typed(synclet.String), Examples: []string{"default"}, Order: order(4),
				},
				"password": {
					Title: "Password", Description: "Password of the user.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(5),
				},
				"tls": {
					Title: "TLS", Description: "Encrypts the connection and verifies the server certificate.",
					PropertyType: typed(synclet.Object), Order: order(6),
					Properties: map[synclet.PropertyName]synclet.PropertySpec{
						"enabled": {
							Title: "Enabled", Description: "Connect over TLS.",
							PropertyType: typed(propBoolean), Default: false, Order: order(0),
						},
						"ca_certificate": {
							Title: "CA certificate", Description: "PEM certificate of the CA that signed the server certificate; the system roots when unset.",
							PropertyType: typed(synclet.String), IsSecret: true, Order: order(1),
						},
						"client_certificate": {
							Title: "Client certificate", Description: "PEM client certificate, when the server asks for one.",
							PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
						},
						"client_key": {
							Title: "Client key", Description: "PEM private key of the client certificate.",
							PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
						},
					},
				},
				"optimize_final": {
					Title:        "Optimize after sync",
					Description:  "Run OPTIMIZE TABLE ... FINAL on every append_dedup table after the sync, so it holds one row per primary key without FINAL.",
					PropertyType: typed(propBoolean), Default: false, Order: order(7),
				},
			}},
		},
	}, nil
}
