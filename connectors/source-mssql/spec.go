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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-mssql",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "MSSQL Source Spec",
			Description: "Reads tables and views of a Microsoft SQL Server database: full refresh, or incremental by a cursor column.",
			Type:        "object",
			Required:    []synclet.PropertyName{"host", "port", "database", "username"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"host": {
					Title: "Host", Description: "The hostname of the database.",
					PropertyType: typed(synclet.String), Order: order(0),
				},
				"port": {
					Title: "Port", Description: "The port of the database.",
					PropertyType: typed(synclet.Integer), Default: 1433, Examples: []string{"1433"}, Order: order(1),
				},
				"database": {
					Title: "Database", Description: "The name of the database.",
					PropertyType: typed(synclet.String), Examples: []string{"master"}, Order: order(2),
				},
				"schemas": {
					Title: "Schemas", Description: "The list of schemas to sync from.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"},
					Default: []string{"dbo"}, Order: order(3),
				},
				"username": {
					Title: "Username",
					Description: "The username which is used to access the database. " +
						`With NTLM authentication, the Windows account as DOMAIN\user.`,
					PropertyType: typed(synclet.String), Order: order(4),
				},
				"password": {
					Title: "Password", Description: "The password associated with the username.",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(5),
				},
				"authentication": {
					Title: "Authentication",
					Description: "SQL: a SQL Server login. NTLM: a Windows domain account (username DOMAIN\\user), " +
						"for a server joined to a Windows domain.",
					PropertyType: typed(synclet.String), Default: authSQL, Examples: []string{authSQL, authNTLM}, Order: order(6),
				},
				"ssl_method": {
					Title:        "SSL Method",
					Description:  "The encryption method which is used when communicating with the database.",
					PropertyType: typed(synclet.Object),
					Order:        order(7),
					OneOf: []synclet.PropertySpec{
						sslOption("Unencrypted", sslUnencrypted,
							"Data transfer will not be encrypted; only the login is, when the server supports encryption.", false),
						sslOption("Encrypted (trust server certificate)", sslTrust,
							"Use the certificate provided by the server without verification. (For testing purposes only!)", false),
						sslOption("Encrypted (verify certificate)", sslVerify,
							"Verify and use the certificate provided by the server.", true),
					},
				},
				"tunnel_method": {
					Title:        "SSH Tunnel Method",
					Description:  "Whether to initiate an SSH tunnel before connecting to the database, and if so, which kind of authentication to use.",
					PropertyType: typed(synclet.Object),
					Order:        order(8),
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
