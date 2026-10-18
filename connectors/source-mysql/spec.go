package main

import synclet "github.com/synclet-io/synclet-go-sdk"

// sslModeSpec returns the oneOf-based SSL mode property spec.
func sslModeSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:       "SSL Mode",
		Description: "SSL connection mode",
		PropertyType: synclet.PropertyType{
			Type: []synclet.PropType{synclet.Object},
		},
		OneOf: []synclet.PropertySpec{
			{
				Title: "preferred",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"mode"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"mode": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const:   "preferred",
						Default: "preferred",
					},
				},
			},
			{
				Title: "required",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"mode"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"mode": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "required",
					},
				},
			},
			{
				Title:       "verify_ca",
				Description: "Verify the server certificate against a CA",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"mode", "ca_certificate"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"mode": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "verify_ca",
					},
					"ca_certificate": {
						Title:       "CA Certificate",
						Description: "PEM-encoded CA certificate to verify the server",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
				},
			},
			{
				Title:       "verify_identity",
				Description: "Verify the server certificate and hostname",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"mode", "ca_certificate", "client_certificate", "client_key"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"mode": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "verify_identity",
					},
					"ca_certificate": {
						Title:       "CA Certificate",
						Description: "PEM-encoded CA certificate to verify the server",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
					"client_certificate": {
						Title:       "Client Certificate",
						Description: "PEM-encoded client certificate for mutual TLS",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
					"client_key": {
						Title:       "Client Key",
						Description: "PEM-encoded client private key for mutual TLS",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
				},
			},
		},
	}
}

// tunnelMethodSpec returns the oneOf-based SSH tunnel method property spec.
func tunnelMethodSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:       "SSH Tunnel Method",
		Description: "Whether to use an SSH tunnel to connect to the database",
		PropertyType: synclet.PropertyType{
			Type: []synclet.PropType{synclet.Object},
		},
		OneOf: []synclet.PropertySpec{
			{
				Title: "No Tunnel",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"method"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"method": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const:   "no_tunnel",
						Default: "no_tunnel",
					},
				},
			},
			{
				Title:       "SSH Key Authentication",
				Description: "Connect through an SSH tunnel using a private key",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"method", "tunnel_host", "tunnel_port", "tunnel_user", "ssh_key"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"method": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "ssh_key",
					},
					"tunnel_host": {
						Title:       "SSH Tunnel Host",
						Description: "Hostname of the SSH tunnel server",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"tunnel_port": {
						Title:       "SSH Tunnel Port",
						Description: "Port of the SSH tunnel server",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.Integer},
						},
						Default: 22,
					},
					"tunnel_user": {
						Title:       "SSH Tunnel User",
						Description: "OS-level user account for the SSH tunnel",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"ssh_key": {
						Title:       "SSH Private Key",
						Description: "PEM-encoded private key for SSH authentication",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
				},
			},
			{
				Title:       "SSH Password Authentication",
				Description: "Connect through an SSH tunnel using a password",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"method", "tunnel_host", "tunnel_port", "tunnel_user", "tunnel_password"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"method": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "ssh_password",
					},
					"tunnel_host": {
						Title:       "SSH Tunnel Host",
						Description: "Hostname of the SSH tunnel server",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"tunnel_port": {
						Title:       "SSH Tunnel Port",
						Description: "Port of the SSH tunnel server",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.Integer},
						},
						Default: 22,
					},
					"tunnel_user": {
						Title:       "SSH Tunnel User",
						Description: "OS-level user account for the SSH tunnel",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"tunnel_password": {
						Title:       "SSH Tunnel Password",
						Description: "Password for SSH authentication",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						IsSecret: true,
					},
				},
			},
		},
	}
}

// replicationMethodSpec returns the oneOf-based replication method property spec.
func replicationMethodSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:       "Replication Method",
		Description: "Replication method to use for extracting data from the database",
		PropertyType: synclet.PropertyType{
			Type: []synclet.PropType{synclet.Object},
		},
		OneOf: []synclet.PropertySpec{
			{
				Title:       "Standard",
				Description: "Standard replication uses SELECT queries to read data",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"method"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"method": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const:   "STANDARD",
						Default: "STANDARD",
					},
				},
			},
			{
				Title:       "CDC (Change Data Capture)",
				Description: "CDC uses MySQL binary log to capture row-level changes",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"method"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"method": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "CDC",
					},
					"server_id": {
						Title:       "Server ID",
						Description: "A unique numeric ID for this replication client (1-4294967295). Must be unique across all replication clients.",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.Integer},
						},
					},
					"initial_wait_seconds": {
						Title:       "Initial Wait Time (seconds)",
						Description: "Time to wait for initial CDC snapshot before timing out",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.Integer},
						},
						Default: 300,
					},
					"checkpoint_seconds": {
						Title:       "Checkpoint Interval (seconds)",
						Description: "How often to emit state checkpoints during CDC replication",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.Integer},
						},
						Default: 300,
					},
				},
			},
		},
	}
}

// tableFilterSpec returns the table filter property spec.
func tableFilterSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:       "Table Filter",
		Description: "Filter which schemas and tables to include in discovery",
		PropertyType: synclet.PropertyType{
			Type: []synclet.PropType{synclet.Object},
		},
		Properties: map[synclet.PropertyName]synclet.PropertySpec{
			"schema_patterns": {
				Title:       "Schema Patterns",
				Description: "SQL LIKE patterns to filter databases/schemas (e.g. [\"my_db%\"]). Empty means all.",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Array},
				},
				Items: map[string]interface{}{"type": "string"},
			},
			"table_patterns": {
				Title:       "Table Patterns",
				Description: "SQL LIKE patterns to filter tables (e.g. [\"users%\", \"orders%\"]). Empty means all.",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Array},
				},
				Items: map[string]interface{}{"type": "string"},
			},
		},
	}
}
