package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func sslOption(title, method, description string, withCert bool) synclet.PropertySpec {
	props := map[synclet.PropertyName]synclet.PropertySpec{
		"ssl_method": {PropertyType: typed(synclet.String), Const: method, Order: order(0)},
	}
	if withCert {
		props["hostNameInCertificate"] = synclet.PropertySpec{
			Title: "Host Name In Certificate",
			Description: "The host name the server certificate must name. Empty: the configured host. " +
				"Set it when the connector reaches the server by another name, such as an IP address.",
			PropertyType: typed(synclet.String), Order: order(1),
		}
		props["certificate"] = synclet.PropertySpec{
			Title: "CA Certificate",
			Description: "PEM certificate of the CA that signed the server certificate. " +
				"Empty: the CAs trusted by the connector image.",
			PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
		}
	}
	return synclet.PropertySpec{
		Title: title, Description: description, PropertyType: typed(synclet.Object),
		Properties: props, Required: []synclet.PropertyName{"ssl_method"},
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Destination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-mssql",
		ProtocolVersion:  "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "MSSQL Destination Spec",
			Description: "Writes each stream into a typed table of one SQL Server 2016+ or Azure SQL database: overwrite, append or append_dedup.",
			Type:        "object",
			Required:    []synclet.PropertyName{"host", "port", "database", "schema", "username"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"host": {
					Title: "Host", Description: "The host name of the database.",
					PropertyType: typed(synclet.String), Order: order(0),
				},
				"port": {
					Title: "Port", Description: "The port of the database.",
					PropertyType: typed(synclet.Integer), Default: 1433, Examples: []string{"1433"}, Order: order(1),
				},
				"database": {
					Title: "DB Name", Description: "The name of the database; it must exist.",
					PropertyType: typed(synclet.String), Order: order(2),
				},
				"schema": {
					Title: "Default Schema", Description: "The schema every table is written to; created when missing.",
					PropertyType: typed(synclet.String), Default: "dbo", Examples: []string{"dbo"}, Order: order(3),
				},
				"username": {
					Title: "User", Description: "The SQL Server login used to access the database.",
					PropertyType: typed(synclet.String), Order: order(4),
				},
				"password": {
					Title: "Password", Description: "The password associated with this username.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(5),
				},
				"ssl_method": {
					Title:        "SSL Method",
					Description:  "The encryption method which is used to communicate with the database.",
					PropertyType: typed(synclet.Object),
					Order:        order(6),
					OneOf: []synclet.PropertySpec{
						sslOption("Unencrypted", sslUnencrypted,
							"Data transfer will not be encrypted; only the login is, when the server supports encryption.", false),
						sslOption("Encrypted (trust server certificate)", sslTrust,
							"Use the certificate provided by the server without verification. (For testing purposes only!)", false),
						sslOption("Encrypted (verify certificate)", sslVerify,
							"Verify and use the certificate provided by the server.", true),
					},
				},
			}},
		},
	}, nil
}
