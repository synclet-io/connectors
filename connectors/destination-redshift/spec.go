package main

import (
	"context"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

func order(n int) *int { return &n }

func typed(t synclet.PropType) synclet.PropertyType {
	return synclet.PropertyType{Type: synclet.PropTypes{t}}
}

func str(title, description string, n int) synclet.PropertySpec {
	return synclet.PropertySpec{Title: title, Description: description, PropertyType: typed(synclet.String), Order: order(n)}
}

func secret(title, description string, n int) synclet.PropertySpec {
	p := str(title, description, n)
	p.IsSecret = true
	return p
}

func sslOption(mode, description string) synclet.PropertySpec {
	return synclet.PropertySpec{
		Title: mode, Description: description, PropertyType: typed(synclet.Object),
		Properties: map[synclet.PropertyName]synclet.PropertySpec{
			"mode": {PropertyType: typed(synclet.String), Const: mode, Order: order(0)},
		},
		Required: []synclet.PropertyName{"mode"},
	}
}

// Spec returns the connector's capabilities and configuration schema.
func (Destination) Spec(_ context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://github.com/synclet-io/connectors/tree/main/connectors/destination-redshift",
		ProtocolVersion:  "0.5.2",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
			synclet.DestinationSyncModeAppend,
			synclet.DestinationSyncModeAppendDedup,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Redshift Destination Spec",
			Description: "Stages each batch as gzipped CSV in S3 and loads it with COPY into a typed table: overwrite, append or append_dedup.",
			Type:        "object",
			Required:    []synclet.PropertyName{"host", "port", "database", "schema", "username", "password", "uploading_method"},
			Properties: synclet.Properties{Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"host": str("Host", "Endpoint of the Redshift cluster or Serverless workgroup, without the port and database.", 0),
				"port": {
					Title: "Port", Description: "Port of the cluster.",
					PropertyType: typed(synclet.Integer), Default: 5439, Examples: []string{"5439"}, Order: order(1),
				},
				"database": str("Database", "Name of the database.", 2),
				"schema": {
					Title: "Default Schema", Description: "Schema every table is written to; it is created when missing. Redshift folds it to lower case.",
					PropertyType: typed(synclet.String), Default: "public", Examples: []string{"public"}, Order: order(3),
				},
				"username": str("Username", "Database user.", 4),
				"password": secret("Password", "Password of the user.", 5),
				"ssl_mode": {
					Title:        "SSL Mode",
					Description:  "How the connection is encrypted.",
					PropertyType: typed(synclet.Object),
					Order:        order(6),
					OneOf: []synclet.PropertySpec{
						sslOption(sslRequire, "Always encrypts; the server certificate is not verified."),
						sslOption(sslVerifyFull, "Always encrypts and verifies the server certificate against the system roots and the host name."),
						sslOption(sslDisable, "No encryption. Only for local test servers."),
					},
				},
				"uploading_method": {
					Title:        "Uploading Method",
					Description:  "Batches are staged as gzipped CSV files in an S3 bucket and loaded with COPY.",
					PropertyType: typed(synclet.Object),
					Order:        order(7),
					OneOf: []synclet.PropertySpec{{
						Title:        methodS3Staging,
						PropertyType: typed(synclet.Object),
						Required:     []synclet.PropertyName{"method", "s3_bucket_name", "s3_bucket_region"},
						Properties: map[synclet.PropertyName]synclet.PropertySpec{
							"method":            {PropertyType: typed(synclet.String), Const: methodS3Staging, Order: order(0)},
							"s3_bucket_name":    str("S3 Bucket Name", "Bucket the batches are staged in.", 1),
							"s3_bucket_path":    str("S3 Bucket Path", "Key prefix of the staged files.", 2),
							"s3_bucket_region":  {Title: "S3 Bucket Region", Description: "Region of the bucket.", PropertyType: typed(synclet.String), Default: defaultRegion, Order: order(3)},
							"access_key_id":     secret("S3 Access Key Id", "Access key of an IAM user that may write, read and delete under the path. Empty uses the default AWS credential chain.", 4),
							"secret_access_key": secret("S3 Secret Access Key", "Secret of the access key.", 5),
							"role_arn":          str("Assumed Role ARN", "Role the connector assumes on top of the keys or the default chain to upload; COPY then uses the role's temporary credentials unless iam_role_arn is set.", 6),
							"external_id":       secret("External ID", "External ID the assumed role requires.", 7),
							"iam_role_arn":      str("COPY IAM Role ARN", "Role attached to the cluster that COPY uses to read the staged files (IAM_ROLE); a comma-separated list chains roles. Empty makes COPY use the upload credentials.", 8),
							"purge_staging_data": {
								Title: "Purge Staging Files", Description: "Delete the staged files once they are loaded.",
								PropertyType: typed(synclet.PropType("boolean")), Default: true, Order: order(9),
							},
						},
					}},
				},
			}},
		},
	}, nil
}
