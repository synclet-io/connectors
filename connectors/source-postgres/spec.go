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

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-postgres",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Postgres Source Spec",
			Description: "Reads tables and views of a Postgres database: full refresh, or incremental by a cursor column.",
			Type:        "object",
			Required:    []synclet.PropertyName{"host", "port", "database", "username"},
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
				"schemas": {
					Title: "Schemas", Description: "The list of schemas to sync from.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"},
					Default: []string{"public"}, Order: order(3),
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
						sslOption(sslDisable, "Disables encryption of communication between the connector and the source database.", false),
						sslOption(sslAllow, "Connects without encryption, and with encryption when the server requires it.", false),
						sslOption(sslPrefer, "Connects with encryption when the server supports it, without otherwise.", false),
						sslOption(sslRequire, "Always encrypts; the server certificate is not verified.", false),
						sslOption(sslVerifyCA, "Always encrypts and verifies that the server certificate is signed by the given CA.", true),
						sslOption(sslVerifyFull, "Always encrypts and verifies the server certificate's CA and its hostname.", true),
					},
				},
				"tunnel_method": {
					Title:        "SSH Tunnel Method",
					Description:  "Whether to initiate an SSH tunnel before connecting to the database, and if so, which kind of authentication to use.",
					PropertyType: typed(synclet.Object),
					Order:        order(7),
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
			}},
		},
	}, nil
}
