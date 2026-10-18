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

func tunnelOption(title, method, description string, secret synclet.PropertyName, secretSpec synclet.PropertySpec) synclet.PropertySpec {
	props := map[synclet.PropertyName]synclet.PropertySpec{
		"tunnel_method": {Description: description, PropertyType: typed(synclet.String), Const: method, Order: order(0)},
	}
	required := []synclet.PropertyName{"tunnel_method"}
	if secret != "" {
		props["tunnel_host"] = synclet.PropertySpec{
			Title: "SSH Tunnel Jump Server Host", Description: "Hostname of the jump server host that allows inbound ssh tunnel.",
			PropertyType: typed(synclet.String), Order: order(1),
		}
		props["tunnel_port"] = synclet.PropertySpec{
			Title: "SSH Connection Port", Description: "Port on the proxy/jump server that accepts inbound ssh connections.",
			PropertyType: typed(synclet.Integer), Default: 22, Examples: []string{"22"}, Order: order(2),
		}
		props["tunnel_user"] = synclet.PropertySpec{
			Title: "SSH Login Username", Description: "OS-level username for logging into the jump server host.",
			PropertyType: typed(synclet.String), Order: order(3),
		}
		props[secret] = secretSpec
		props["tunnel_host_key"] = synclet.PropertySpec{
			Title: "SSH Host Key",
			Description: "Public host keys of the jump server, one per line, each in authorized_keys format (\"ssh-ed25519 AAAA...\") " +
				"or as a known_hosts line (\"bastion.example.com ssh-ed25519 AAAA...\"); the whole output of ssh-keyscan works. " +
				"The tunnel is refused when the server presents none of them. When empty, the host key is not verified.",
			PropertyType: typed(synclet.String), Order: order(5),
		}
		required = append(required, "tunnel_host", "tunnel_port", "tunnel_user", secret)
	}
	return synclet.PropertySpec{
		Title: title, PropertyType: typed(synclet.Object), Properties: props, Required: required,
	}
}

func replicationOption(title, method, description string, cdc bool) synclet.PropertySpec {
	props := map[synclet.PropertyName]synclet.PropertySpec{
		"method": {PropertyType: typed(synclet.String), Const: method, Order: order(0)},
	}
	if cdc {
		props["server_id"] = synclet.PropertySpec{
			Title:        "Server ID",
			Description:  "A numeric ID for this replication client, unique among the server's replicas and binlog readers (1-4294967295). When empty, a random ID from 5400-6399 is used.",
			PropertyType: typed(synclet.Integer), Order: order(1),
		}
		props["initial_waiting_seconds"] = synclet.PropertySpec{
			Title:        "Initial Waiting Time in Seconds",
			Description:  "Accepted for compatibility with Airbyte's configuration; the binlog is read until the sync is stopped.",
			PropertyType: typed(synclet.Integer), Default: 300, Order: order(2),
		}
		props["checkpoint_seconds"] = synclet.PropertySpec{
			Title:        "Checkpoint Interval in Seconds",
			Description:  "How often the binlog position is saved while the binlog is read.",
			PropertyType: typed(synclet.Integer), Default: defaultCheckpointSeconds, Order: order(3),
		}
	}
	return synclet.PropertySpec{
		Title: title, Description: description, PropertyType: typed(synclet.Object),
		Properties: props, Required: []synclet.PropertyName{"method"},
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-mysql",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "MySQL Source Spec",
			Description: "Reads tables and views of a MySQL database: full refresh, incremental by a cursor column, or CDC from the binlog.",
			Type:        "object",
			Required:    []synclet.PropertyName{"host", "port", "database", "username"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"host": {
					Title: "Host", Description: "Hostname of the database.",
					PropertyType: typed(synclet.String), Order: order(0),
				},
				"port": {
					Title: "Port", Description: "Port of the database.",
					PropertyType: typed(synclet.Integer), Default: 3306, Examples: []string{"3306"}, Order: order(1),
				},
				"database": {
					Title: "Database", Description: "Name of the database.",
					PropertyType: typed(synclet.String), Order: order(2),
				},
				"username": {
					Title: "Username", Description: "Username to access the database.",
					PropertyType: typed(synclet.String), Order: order(3),
				},
				"password": {
					Title: "Password", Description: "Password associated with the username.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(4),
				},
				"ssl_mode": {
					Title:        "SSL Modes",
					Description:  "How the connection is encrypted and which server certificates are trusted.",
					PropertyType: typed(synclet.Object),
					Order:        order(5),
					OneOf: []synclet.PropertySpec{
						sslOption(sslPreferred, "Connects with encryption when the server supports it, without otherwise.", false),
						sslOption(sslRequired, "Always encrypts; the server certificate is not verified.", false),
						sslOption(sslVerifyCA, "Always encrypts and verifies that the server certificate is signed by the given CA.", true),
						sslOption(sslVerifyIdentity, "Always encrypts and verifies the server certificate's CA and that it names the host.", true),
					},
				},
				"tunnel_method": {
					Title:        "SSH Tunnel Method",
					Description:  "Whether to initiate an SSH tunnel before connecting to the database, and if so, which kind of authentication to use.",
					PropertyType: typed(synclet.Object),
					Order:        order(6),
					OneOf: []synclet.PropertySpec{
						tunnelOption("No Tunnel", tunnelNone, "No ssh tunnel needed to connect to database", "", synclet.PropertySpec{}),
						tunnelOption("SSH Key Authentication", tunnelKey, "Connect through a jump server tunnel host using username and ssh key",
							"ssh_key", synclet.PropertySpec{
								Title: "SSH Private Key", Description: "OS-level user account ssh key credentials in PEM or OpenSSH format (RSA, ECDSA or Ed25519)",
								PropertyType: typed(synclet.String), IsSecret: true, Order: order(4),
							}),
						tunnelOption("Password Authentication", tunnelPassword, "Connect through a jump server tunnel host using username and password authentication",
							"tunnel_user_password", synclet.PropertySpec{
								Title: "Password", Description: "OS-level password for logging into the jump server host",
								PropertyType: typed(synclet.String), IsSecret: true, Order: order(4),
							}),
					},
				},
				"replication_method": {
					Title:        "Update Method",
					Description:  "How rows are read: by SELECT queries (full refresh, or incremental by a cursor column), or as changes from the binlog.",
					PropertyType: typed(synclet.Object),
					Order:        order(7),
					OneOf: []synclet.PropertySpec{
						replicationOption("Scan Changes with User Defined Cursor", replicationStandard, "Reads rows with SELECT queries; incremental streams read the rows whose cursor column is at least the saved value.", false),
						replicationOption("Read Changes using Binary Log (CDC)", replicationCDC, "Reads a snapshot of each table, then inserts, updates and deletes from the binlog. Needs log_bin=ON, binlog_format=ROW, binlog_row_image=FULL and the REPLICATION CLIENT and REPLICATION SLAVE privileges.", true),
					},
				},
				"table_filter": {
					Title:        "Table Filter",
					Description:  "Which tables discovery lists.",
					PropertyType: typed(synclet.Object),
					Order:        order(8),
					Properties: map[synclet.PropertyName]synclet.PropertySpec{
						"schema_patterns": {
							Title:        "Schema Patterns",
							Description:  "Accepted for compatibility; only the configured database is read.",
							PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"},
						},
						"table_patterns": {
							Title:        "Table Patterns",
							Description:  "SQL LIKE patterns of the tables to list (e.g. [\"users%\", \"orders%\"]). Empty lists every table.",
							PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"},
						},
					},
				},
				"zero_date_handling": {
					Title: "Zero Dates",
					Description: "What a DATE, DATETIME or TIMESTAMP whose month or day is zero (0000-00-00, 2024-00-15) reads as: " +
						"null reads it as null, fail stops the sync with an error naming the stream, column and row.",
					PropertyType: typed(synclet.String), Default: zeroDateNull, Order: order(9),
					OneOf: []synclet.PropertySpec{
						{Title: "Read as null", Const: zeroDateNull},
						{Title: "Fail the read", Const: zeroDateFail},
					},
				},
			}},
		},
	}, nil
}
