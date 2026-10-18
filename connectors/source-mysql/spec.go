package main

import airbyte "github.com/saturn4er/airbyte-go-sdk"

// sslModeSpec returns the oneOf-based SSL mode property spec.
func sslModeSpec() airbyte.PropertySpec {
	return airbyte.PropertySpec{
		Title:       "SSL Mode",
		Description: "SSL connection mode",
		PropertyType: airbyte.PropertyType{
			Type: []airbyte.PropType{airbyte.Object},
		},
		OneOf: []airbyte.PropertySpec{
			{
				Title: "preferred",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"mode"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"mode": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const:   "preferred",
						Default: "preferred",
					},
				},
			},
			{
				Title: "required",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"mode"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"mode": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "required",
					},
				},
			},
			{
				Title:       "verify_ca",
				Description: "Verify the server certificate against a CA",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"mode", "ca_certificate"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"mode": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "verify_ca",
					},
					"ca_certificate": {
						Title:       "CA Certificate",
						Description: "PEM-encoded CA certificate to verify the server",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
				},
			},
			{
				Title:       "verify_identity",
				Description: "Verify the server certificate and hostname",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"mode", "ca_certificate", "client_certificate", "client_key"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"mode": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "verify_identity",
					},
					"ca_certificate": {
						Title:       "CA Certificate",
						Description: "PEM-encoded CA certificate to verify the server",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
					"client_certificate": {
						Title:       "Client Certificate",
						Description: "PEM-encoded client certificate for mutual TLS",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
					"client_key": {
						Title:       "Client Key",
						Description: "PEM-encoded client private key for mutual TLS",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
				},
			},
		},
	}
}

// tunnelMethodSpec returns the oneOf-based SSH tunnel method property spec.
func tunnelMethodSpec() airbyte.PropertySpec {
	return airbyte.PropertySpec{
		Title:       "SSH Tunnel Method",
		Description: "Whether to use an SSH tunnel to connect to the database",
		PropertyType: airbyte.PropertyType{
			Type: []airbyte.PropType{airbyte.Object},
		},
		OneOf: []airbyte.PropertySpec{
			{
				Title: "No Tunnel",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"method"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"method": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const:   "no_tunnel",
						Default: "no_tunnel",
					},
				},
			},
			{
				Title:       "SSH Key Authentication",
				Description: "Connect through an SSH tunnel using a private key",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"method", "tunnel_host", "tunnel_port", "tunnel_user", "ssh_key"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"method": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "ssh_key",
					},
					"tunnel_host": {
						Title:       "SSH Tunnel Host",
						Description: "Hostname of the SSH tunnel server",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"tunnel_port": {
						Title:       "SSH Tunnel Port",
						Description: "Port of the SSH tunnel server",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
						Default: 22,
					},
					"tunnel_user": {
						Title:       "SSH Tunnel User",
						Description: "OS-level user account for the SSH tunnel",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"ssh_key": {
						Title:       "SSH Private Key",
						Description: "PEM-encoded private key for SSH authentication",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
				},
			},
			{
				Title:       "SSH Password Authentication",
				Description: "Connect through an SSH tunnel using a password",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"method", "tunnel_host", "tunnel_port", "tunnel_user", "tunnel_password"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"method": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "ssh_password",
					},
					"tunnel_host": {
						Title:       "SSH Tunnel Host",
						Description: "Hostname of the SSH tunnel server",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"tunnel_port": {
						Title:       "SSH Tunnel Port",
						Description: "Port of the SSH tunnel server",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
						Default: 22,
					},
					"tunnel_user": {
						Title:       "SSH Tunnel User",
						Description: "OS-level user account for the SSH tunnel",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"tunnel_password": {
						Title:       "SSH Tunnel Password",
						Description: "Password for SSH authentication",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						IsSecret: true,
					},
				},
			},
		},
	}
}

// replicationMethodSpec returns the oneOf-based replication method property spec.
func replicationMethodSpec() airbyte.PropertySpec {
	return airbyte.PropertySpec{
		Title:       "Replication Method",
		Description: "Replication method to use for extracting data from the database",
		PropertyType: airbyte.PropertyType{
			Type: []airbyte.PropType{airbyte.Object},
		},
		OneOf: []airbyte.PropertySpec{
			{
				Title:       "Standard",
				Description: "Standard replication uses SELECT queries to read data",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"method"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"method": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const:   "STANDARD",
						Default: "STANDARD",
					},
				},
			},
			{
				Title:       "CDC (Change Data Capture)",
				Description: "CDC uses MySQL binary log to capture row-level changes",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"method"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"method": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "CDC",
					},
					"server_id": {
						Title:       "Server ID",
						Description: "A unique numeric ID for this replication client (1-4294967295). Must be unique across all replication clients.",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
					},
					"initial_wait_seconds": {
						Title:       "Initial Wait Time (seconds)",
						Description: "Time to wait for initial CDC snapshot before timing out",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
						Default: 300,
					},
					"checkpoint_seconds": {
						Title:       "Checkpoint Interval (seconds)",
						Description: "How often to emit state checkpoints during CDC replication",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Integer},
						},
						Default: 300,
					},
				},
			},
		},
	}
}

// tableFilterSpec returns the table filter property spec.
func tableFilterSpec() airbyte.PropertySpec {
	return airbyte.PropertySpec{
		Title:       "Table Filter",
		Description: "Filter which schemas and tables to include in discovery",
		PropertyType: airbyte.PropertyType{
			Type: []airbyte.PropType{airbyte.Object},
		},
		Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
			"schema_patterns": {
				Title:       "Schema Patterns",
				Description: "SQL LIKE patterns to filter databases/schemas (e.g. [\"my_db%\"]). Empty means all.",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Array},
				},
				Items: map[string]interface{}{"type": "string"},
			},
			"table_patterns": {
				Title:       "Table Patterns",
				Description: "SQL LIKE patterns to filter tables (e.g. [\"users%\", \"orders%\"]). Empty means all.",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Array},
				},
				Items: map[string]interface{}{"type": "string"},
			},
		},
	}
}
