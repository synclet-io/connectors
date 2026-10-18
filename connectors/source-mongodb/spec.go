package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func clusterOption(title, clusterType, description, connectionExample string) synclet.PropertySpec {
	return synclet.PropertySpec{
		Title: title, Description: description, PropertyType: typed(synclet.Object),
		Required: []synclet.PropertyName{"cluster_type", "connection_string", "database"},
		Properties: map[synclet.PropertyName]synclet.PropertySpec{
			"cluster_type": {PropertyType: typed(synclet.String), Const: clusterType, Order: order(0)},
			"connection_string": {
				Title: "Connection String",
				Description: "A mongodb:// URI, with several hosts separated by commas for a replica set, or a mongodb+srv:// URI " +
					"whose SRV and TXT records list the hosts and options. Options in the URI (replicaSet, tls, readPreference, " +
					"authMechanism, ...) apply. Leave the user name and password out and set them below.",
				PropertyType: typed(synclet.String), Examples: []string{connectionExample}, Order: order(1),
			},
			"database": {
				Title: "Database Name", Description: "The database whose collections are synced.",
				PropertyType: typed(synclet.String), Order: order(2),
			},
			"username": {
				Title: "Username", Description: "The user to log in as. Empty: no authentication, or the user named in the connection string.",
				PropertyType: typed(synclet.String), Order: order(3),
			},
			"password": {
				Title: "Password", Description: "The password of the user.",
				PropertyType: typed(synclet.String), IsSecret: true, Order: order(4),
			},
			"auth_source": {
				Title: "Authentication Source",
				Description: "The database that holds the user. Empty: the authSource of the connection string, " +
					"else admin.",
				PropertyType: typed(synclet.String), Default: "admin", Examples: []string{"admin"}, Order: order(5),
			},
		},
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
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-mongodb",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "MongoDb Source Spec",
			Description: "Reads the collections of a MongoDB database: full refresh, or incremental by a cursor field.",
			Type:        "object",
			Required:    []synclet.PropertyName{"database_config"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"database_config": {
					Title:        "Cluster Type",
					Description:  "Configures the MongoDB cluster type.",
					PropertyType: typed(synclet.Object),
					Order:        order(0),
					OneOf: []synclet.PropertySpec{
						clusterOption("MongoDB Atlas Replica Set", clusterAtlas,
							"MongoDB Atlas-hosted cluster configured as a replica set",
							"mongodb+srv://cluster0.abcd1.mongodb.net/"),
						clusterOption("Self-Managed Replica Set", clusterSelfManaged,
							"MongoDB self-hosted cluster configured as a replica set",
							"mongodb://example1.host.com:27017,example2.host.com:27017/?replicaSet=rs0"),
					},
				},
				"discover_sample_size": {
					Title: "Document discovery sample size",
					Description: "How many documents of each collection discover samples to infer the collection's schema. " +
						"Between 1 and 100000. Fields seen in no sampled document are not part of the schema.",
					PropertyType: typed(synclet.Integer), Default: defaultSampleSize, Order: order(1),
				},
				"tls_ca_certificate": {
					Title: "TLS CA Certificate",
					Description: "PEM certificate of the CA that signed the server certificates. Setting it turns TLS on. " +
						"Empty: the CAs trusted by the connector image, and TLS as the connection string says (on for mongodb+srv://).",
					PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
				},
				"tunnel_method": {
					Title:        "SSH Tunnel Method",
					Description:  "Whether to initiate an SSH tunnel before connecting to the database, and if so, which kind of authentication to use.",
					PropertyType: typed(synclet.Object),
					Order:        order(3),
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
