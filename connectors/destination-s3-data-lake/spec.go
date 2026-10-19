package main

import synclet "github.com/synclet-io/synclet-go-sdk"

const booleanType synclet.PropType = "boolean"

func connectionSpec() synclet.ConnectionSpecification {
	return synclet.ConnectionSpecification{
		Title:       "S3 Data Lake Destination Spec",
		Description: "Writes streams as Apache Iceberg tables on S3-compatible storage through an Iceberg REST catalog",
		Type:        "object",
		Required:    []synclet.PropertyName{"catalog", "namespace", "s3"},
		Properties: synclet.Properties{
			Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"catalog": catalogSpec(),
				"namespace": {
					Title:        "Namespace",
					Description:  "Iceberg namespace every table is written to; dots separate levels. It must exist in the catalog",
					PropertyType: stringType(),
					Examples:     []string{"synclet", "analytics.raw"},
					Order:        intPtr(1),
				},
				"s3": s3Spec(),
				"number_type": {
					Title:        "Number Type",
					Description:  "decimal writes JSON numbers as decimal(38, decimal_scale) exactly; double writes them as double",
					PropertyType: stringType(),
					Default:      numberTypeDecimal,
					Examples:     []string{numberTypeDecimal, numberTypeDouble},
					Order:        intPtr(3),
				},
				"decimal_scale": {
					Title:        "Decimal Scale",
					Description:  "Digits after the decimal point of number columns (0-38); a number with more digits fails the sync",
					PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}},
					Default:      defaultDecimalScale,
					Order:        intPtr(4),
				},
				"dedup_key_limit": {
					Title: "Dedup Key Limit",
					Description: "Most distinct primary keys per append_dedup stream with a cursor whose highest cursor one sync remembers, " +
						"so a record with a lower cursor than one already written is skipped; a sync with more keys fails",
					PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}},
					Default:      defaultDedupKeyLimit,
					Order:        intPtr(5),
				},
			},
		},
	}
}

func catalogSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Catalog",
		Description:  "The Iceberg catalog the tables are registered in",
		PropertyType: objectType(),
		Order:        intPtr(0),
		OneOf: []synclet.PropertySpec{
			{
				Title:        "REST Catalog",
				Description:  "An Iceberg REST catalog: Apache Polaris, Lakekeeper, Gravitino, Nessie's REST API, Unity Catalog and others",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"catalog_type", "uri"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"catalog_type": {PropertyType: stringType(), Const: catalogREST},
					"uri": {
						Title:        "URI",
						Description:  "Base URL of the catalog's REST API, without /v1",
						PropertyType: stringType(),
						Examples:     []string{"http://iceberg-rest:8181", "https://polaris.example.com/api/catalog"},
						Order:        intPtr(0),
					},
					"warehouse": {
						Title:        "Warehouse",
						Description:  "Warehouse name or location sent to the catalog's /v1/config; empty when the catalog needs none",
						PropertyType: stringType(),
						Order:        intPtr(1),
					},
					"credential": {
						Title:        "OAuth2 Credential",
						Description:  "client_id:client_secret exchanged for a token at the catalog's token endpoint",
						PropertyType: stringType(),
						IsSecret:     true,
						Order:        intPtr(2),
					},
					"token": {
						Title:        "Bearer Token",
						Description:  "A static bearer token; set this or credential, not both",
						PropertyType: stringType(),
						IsSecret:     true,
						Order:        intPtr(3),
					},
					"scope": {
						Title:        "OAuth2 Scope",
						Description:  "Scope requested with credential; catalog when empty",
						PropertyType: stringType(),
						Order:        intPtr(4),
					},
				},
			},
		},
	}
}

func s3Spec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "S3 Storage",
		Description:  "Where the tables' data and metadata files are written",
		PropertyType: objectType(),
		Required:     []synclet.PropertyName{"warehouse_location", "credentials"},
		Order:        intPtr(2),
		Properties: map[synclet.PropertyName]synclet.PropertySpec{
			"warehouse_location": {
				Title:        "Warehouse Location",
				Description:  "s3://bucket/prefix under which each table is created at <namespace levels>/<table>",
				PropertyType: stringType(),
				Examples:     []string{"s3://lake/warehouse"},
				Order:        intPtr(0),
			},
			"region": {
				Title:        "Region",
				Description:  "Region of the bucket",
				PropertyType: stringType(),
				Default:      defaultRegion,
				Examples:     []string{"us-east-1", "eu-central-1", "auto"},
				Order:        intPtr(1),
			},
			"credentials": credentialsSpec(),
			"endpoint": {
				Title:        "Endpoint",
				Description:  "URL of an S3-compatible service such as MinIO or Cloudflare R2; empty for AWS S3",
				PropertyType: stringType(),
				Examples:     []string{"http://minio:9000", "https://<account_id>.r2.cloudflarestorage.com"},
				Order:        intPtr(3),
			},
			"force_path_style": {
				Title:        "Path-Style Access",
				Description:  "Address the bucket in the URL path instead of the host name; MinIO needs it",
				PropertyType: synclet.PropertyType{Type: []synclet.PropType{booleanType}},
				Default:      false,
				Order:        intPtr(4),
			},
		},
	}
}

func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "How the connector authenticates to the bucket",
		PropertyType: objectType(),
		Order:        intPtr(2),
		OneOf: []synclet.PropertySpec{
			{
				Title:        "Access Key",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"auth_type", "access_key_id", "secret_access_key"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: stringType(), Const: authAccessKey},
					"access_key_id": {
						Title:        "Access Key ID",
						PropertyType: stringType(),
						IsSecret:     true,
						Order:        intPtr(0),
					},
					"secret_access_key": {
						Title:        "Secret Access Key",
						PropertyType: stringType(),
						IsSecret:     true,
						Order:        intPtr(1),
					},
				},
			},
			{
				Title:        "Default Credential Chain",
				Description:  "Environment variables, shared configuration, web identity, container or instance role",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"auth_type"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: stringType(), Const: authDefaultChain},
				},
			},
			{
				Title:        "Assume Role",
				Description:  "Assumes a role with the credentials of the default chain",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"auth_type", "role_arn"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: stringType(), Const: authAssumeRole},
					"role_arn": {
						Title:        "Role ARN",
						PropertyType: stringType(),
						Examples:     []string{"arn:aws:iam::123456789012:role/synclet-writer"},
						Order:        intPtr(0),
					},
					"external_id": {
						Title:        "External ID",
						PropertyType: stringType(),
						IsSecret:     true,
						Order:        intPtr(1),
					},
				},
			},
		},
	}
}

func stringType() synclet.PropertyType {
	return synclet.PropertyType{Type: []synclet.PropType{synclet.String}}
}

func objectType() synclet.PropertyType {
	return synclet.PropertyType{Type: []synclet.PropType{synclet.Object}}
}

func intPtr(i int) *int {
	return &i
}
