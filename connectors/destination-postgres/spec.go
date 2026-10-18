package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func sslOption(mode, description string, withCerts bool) synclet.PropertySpec {
	props := map[synclet.PropertyName]synclet.PropertySpec{
		"mode": {PropertyType: typed(synclet.String), Const: mode, Order: order(0)},
	}
	required := []synclet.PropertyName{"mode"}
	if withCerts {
		props["ca_certificate"] = synclet.PropertySpec{
			Title: "CA certificate", Description: "PEM certificate of the CA that signed the server certificate.",
			PropertyType: typed(synclet.String), IsSecret: true, Order: order(1),
		}
		props["client_certificate"] = synclet.PropertySpec{
			Title: "Client certificate", Description: "PEM client certificate, when the server asks for one.",
			PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
		}
		props["client_key"] = synclet.PropertySpec{
			Title: "Client key", Description: "PEM private key of the client certificate.",
			PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
		}
		required = append(required, "ca_certificate")
	}
	return synclet.PropertySpec{
		Title: mode, Description: description, PropertyType: typed(synclet.Object),
		Properties: props, Required: required,
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Destination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-postgres",
		ProtocolVersion:  "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Postgres Destination Spec",
			Description: "Writes each stream into a typed table of one schema: overwrite, append or append_dedup.",
			Type:        "object",
			Required:    []synclet.PropertyName{"host", "port", "database", "schema", "username"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"host": {
					Title: "Host", Description: "Hostname of the database.",
					PropertyType: typed(synclet.String), Order: order(0),
				},
				"port": {
					Title: "Port", Description: "Port of the database.",
					PropertyType: typed(synclet.Integer), Default: 5432, Examples: []string{"5432"}, Order: order(1),
				},
				"database": {
					Title: "Database Name", Description: "Name of the database.",
					PropertyType: typed(synclet.String), Order: order(2),
				},
				"schema": {
					Title: "Default Schema", Description: "Schema every table is written to; it is created when missing.",
					PropertyType: typed(synclet.String), Default: "public", Examples: []string{"public"}, Order: order(3),
				},
				"username": {
					Title: "Username", Description: "Username to access the database.",
					PropertyType: typed(synclet.String), Order: order(4),
				},
				"password": {
					Title: "Password", Description: "Password associated with the username.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(5),
				},
				"ssl_mode": {
					Title:        "SSL Modes",
					Description:  "How the connection is encrypted and which server certificates are trusted; the modes behave like libpq's sslmode.",
					PropertyType: typed(synclet.Object),
					Order:        order(6),
					OneOf: []synclet.PropertySpec{
						sslOption(sslDisable, "Disables encryption of communication between the connector and the database.", false),
						sslOption(sslAllow, "Connects without encryption, and with encryption when the server requires it.", false),
						sslOption(sslPrefer, "Connects with encryption when the server supports it, without otherwise.", false),
						sslOption(sslRequire, "Always encrypts; the server certificate is not verified.", false),
						sslOption(sslVerifyCA, "Always encrypts and verifies that the server certificate is signed by the given CA.", true),
						sslOption(sslVerifyFull, "Always encrypts and verifies the server certificate's CA and its hostname.", true),
					},
				},
			}},
		},
	}, nil
}
