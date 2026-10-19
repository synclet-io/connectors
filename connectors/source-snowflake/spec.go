package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func credentialsSpec() synclet.PropertySpec {
	username := synclet.PropertySpec{
		Title: "Username", Description: "The Snowflake user to log in as.",
		PropertyType: typed(synclet.String), Order: order(1),
	}
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "How the connector logs in to Snowflake.",
		PropertyType: typed(synclet.Object),
		Order:        order(5),
		OneOf: []synclet.PropertySpec{
			{
				Title:        "Key Pair Authentication",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type", "username", "private_key"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authKeyPair, Order: order(0)},
					"username":  username,
					"private_key": {
						Title:        "Private Key",
						Description:  "RSA private key in PEM (PKCS#8 or PKCS#1) whose public key is set as the user's RSA_PUBLIC_KEY.",
						PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
					},
					"private_key_password": {
						Title:        "Private Key Password",
						Description:  "Password of an encrypted PKCS#8 private key.",
						PropertyType: typed(synclet.String), IsSecret: true, Order: order(3),
					},
				},
			},
			{
				Title:        "Username and Password",
				PropertyType: typed(synclet.Object),
				Required:     []synclet.PropertyName{"auth_type", "username", "password"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: typed(synclet.String), Const: authPassword, Order: order(0)},
					"username":  username,
					"password": {
						Title: "Password", PropertyType: typed(synclet.String), IsSecret: true, Order: order(2),
					},
				},
			},
		},
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Source) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL:    "https://github.com/synclet-io/connectors/tree/main/connectors/source-snowflake",
		SupportsIncremental: true,
		ProtocolVersion:     "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Snowflake Source Spec",
			Description: "Reads tables and views of a Snowflake database: full refresh, or incremental by a cursor column.",
			Type:        "object",
			Required:    []synclet.PropertyName{"account", "warehouse", "database", "credentials"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"account": {
					Title:        "Account",
					Description:  "Account identifier, orgname-accountname or the account locator with its region (the part before .snowflakecomputing.com).",
					PropertyType: typed(synclet.String), Examples: []string{"myorg-myaccount", "xy12345.eu-central-1"}, Order: order(0),
				},
				"warehouse": {
					Title: "Warehouse", Description: "Warehouse that runs the queries. The role needs USAGE on it.",
					PropertyType: typed(synclet.String), Order: order(1),
				},
				"database": {
					Title: "Database", Description: "Database to read. The role needs USAGE on it.",
					PropertyType: typed(synclet.String), Order: order(2),
				},
				"schemas": {
					Title:        "Schemas",
					Description:  "Schemas to sync, as Snowflake stores their names (unquoted names are upper case). When empty, every schema of the database.",
					PropertyType: typed(synclet.Array), Items: map[string]any{"type": "string"}, Order: order(3),
				},
				"role": {
					Title: "Role", Description: "Role of the session. When empty, the user's default role.",
					PropertyType: typed(synclet.String), Order: order(4),
				},
				"credentials": credentialsSpec(),
			}},
		},
	}, nil
}
