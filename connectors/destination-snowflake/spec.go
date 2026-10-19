package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

// Spec returns the connector's capabilities and configuration schema.
func (Destination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	keyPair := synclet.PropertySpec{
		Title:        authKeyPair,
		Description:  "Logs in with a JWT signed by the user's RSA private key; the user's RSA_PUBLIC_KEY must be its public key.",
		PropertyType: typed(synclet.Object),
		Required:     []synclet.PropertyName{"auth_type", "username", "private_key"},
		Properties: map[synclet.PropertyName]synclet.PropertySpec{
			"auth_type": {PropertyType: typed(synclet.String), Const: authKeyPair, Order: order(0)},
			"username": {
				Title: "Username", Description: "Snowflake user the connector logs in as.",
				PropertyType: typed(synclet.String), Examples: []string{"SYNCLET_LOADER"}, Order: order(1),
			},
			"private_key": {
				Title: "Private Key", Description: "RSA private key in PEM: PKCS#8 (-----BEGIN PRIVATE KEY-----), encrypted PKCS#8 or PKCS#1.",
				PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
			},
			"private_key_password": {
				Title: "Private Key Password", Description: "Passphrase of an encrypted private key.",
				PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
			},
		},
	}
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-snowflake",
		ProtocolVersion:  "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Snowflake Destination Spec",
			Description: "Writes each stream into a typed table of one schema through PUT and COPY INTO: overwrite, append or append_dedup.",
			Type:        "object",
			Required:    []synclet.PropertyName{"account", "warehouse", "database", "credentials"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"account": {
					Title: "Account", Description: "Account identifier, orgname-accountname, or the account locator with its region.",
					PropertyType: typed(synclet.String), Examples: []string{"myorg-myaccount", "xy12345.eu-central-1"}, Order: order(0),
				},
				"warehouse": {
					Title: "Warehouse", Description: "Warehouse that runs the loads; the role needs USAGE on it.",
					PropertyType: typed(synclet.String), Examples: []string{"LOADING_WH"}, Order: order(1),
				},
				"database": {
					Title: "Database", Description: "Database the tables are written to.",
					PropertyType: typed(synclet.String), Examples: []string{"RAW"}, Order: order(2),
				},
				"schema": {
					Title: "Default Schema", Description: "Schema every table is written to, in upper case; it is created when missing.",
					PropertyType: typed(synclet.String), Default: defaultSchema, Examples: []string{defaultSchema}, Order: order(3),
				},
				"role": {
					Title: "Role", Description: "Role of the session; the user's default role when empty.",
					PropertyType: typed(synclet.String), Examples: []string{"SYNCLET_ROLE"}, Order: order(4),
				},
				"credentials": {
					Title: "Authorization Method", Description: "How the connector logs in.",
					PropertyType: typed(synclet.Object), Order: order(5),
					OneOf: []synclet.PropertySpec{keyPair},
				},
			}},
		},
	}, nil
}
