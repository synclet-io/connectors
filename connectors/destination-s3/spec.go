package main

import synclet "github.com/synclet-io/synclet-go-sdk"

const booleanType synclet.PropType = "boolean"

func s3ConnectionSpec() synclet.ConnectionSpecification {
	return synclet.ConnectionSpecification{
		Title:       "S3 Destination Spec",
		Description: "Writes streams as JSONL, CSV or Parquet files into an S3-compatible bucket (AWS S3, MinIO, Cloudflare R2)",
		Type:        "object",
		Required:    []synclet.PropertyName{"bucket", "credentials"},
		Properties: synclet.Properties{
			Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"bucket": {
					Title:        "Bucket",
					Description:  "Name of the bucket the files are written to",
					PropertyType: stringType(),
					Order:        intPtr(0),
				},
				"bucket_path": {
					Title:        "Bucket Path",
					Description:  "Key prefix every object is written under, without leading or trailing slash",
					PropertyType: stringType(),
					Examples:     []string{"synclet", "exports/raw"},
					Order:        intPtr(1),
				},
				"region": {
					Title:        "Region",
					Description:  "Region of the bucket",
					PropertyType: stringType(),
					Default:      defaultRegion,
					Examples:     []string{"us-east-1", "eu-central-1", "auto"},
					Order:        intPtr(2),
				},
				"credentials": credentialsSpec(),
				"format":      formatSpec(),
				"path_template": {
					Title: "Path Template",
					Description: "Object key below bucket_path. Tokens: {namespace}, {stream}, {yyyy}, {mm}, {dd}, {hh} (sync start, UTC), " +
						"{field:<name>} (a top-level record field; null or missing is __null__, an empty string __empty__) and {part} " +
						"(required in the last segment). {stream} is required in a directory segment, and only {namespace} and {stream} " +
						"may come before the end of that segment. Empty segments are dropped; the format's extension is appended.",
					PropertyType: stringType(),
					Default:      defaultPathTemplate,
					Examples:     []string{defaultPathTemplate, "{stream}/country={field:country}/{yyyy}/{mm}/{dd}/{part}"},
					Order:        intPtr(5),
				},
				"file_size_mb": {
					Title:        "File Size (MB)",
					Description:  "A file is uploaded and a new one started once this many megabytes are written before compression",
					PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}},
					Default:      defaultFileSizeMB,
					Order:        intPtr(6),
				},
				"max_records_per_file": {
					Title:        "Max Records per File",
					Description:  "A file is uploaded and a new one started once it holds this many records; 0 is no limit",
					PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}},
					Default:      0,
					Order:        intPtr(7),
				},
				"endpoint": {
					Title:        "Endpoint",
					Description:  "URL of an S3-compatible service such as MinIO or Cloudflare R2; empty for AWS S3",
					PropertyType: stringType(),
					Examples:     []string{"http://minio:9000", "https://<account_id>.r2.cloudflarestorage.com"},
					Order:        intPtr(8),
				},
				"force_path_style": {
					Title:        "Path-Style Access",
					Description:  "Address the bucket in the URL path instead of the host name; MinIO needs it",
					PropertyType: synclet.PropertyType{Type: []synclet.PropType{booleanType}},
					Default:      false,
					Order:        intPtr(9),
				},
			},
		},
	}
}

func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "How the connector authenticates to the bucket",
		PropertyType: objectType(),
		Order:        intPtr(3),
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

func formatSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Output Format",
		PropertyType: objectType(),
		Order:        intPtr(4),
		OneOf: []synclet.PropertySpec{
			{
				Title:        "JSON Lines",
				Description:  "One object per record: _airbyte_raw_id, _airbyte_extracted_at and the record data verbatim under _airbyte_data",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"format_type"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"format_type": {PropertyType: stringType(), Const: formatJSONL},
					"compression": textCompressionSpec(),
				},
			},
			{
				Title:        "CSV",
				Description:  "One column per declared top-level property; undeclared fields go to _airbyte_additional_properties",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"format_type"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"format_type": {PropertyType: stringType(), Const: formatCSV},
					"compression": textCompressionSpec(),
				},
			},
			{
				Title:        "Parquet",
				Description:  "One column per declared top-level property with logical types; undeclared fields go to _airbyte_additional_properties",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"format_type"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"format_type": {PropertyType: stringType(), Const: formatParquet},
					"compression": {
						Title:        "Compression",
						Description:  "snappy, zstd, gzip or none",
						PropertyType: stringType(),
						Default:      compressionSnappy,
						Examples:     []string{compressionSnappy, compressionZstd, compressionGzip, compressionNone},
						Order:        intPtr(0),
					},
					"parquet_number_type": {
						Title:        "Number Type",
						Description:  "decimal writes JSON numbers as DECIMAL(38, decimal_scale); string writes their text",
						PropertyType: stringType(),
						Default:      numberTypeDecimal,
						Examples:     []string{numberTypeDecimal, numberTypeString},
						Order:        intPtr(1),
					},
					"decimal_scale": {
						Title:        "Decimal Scale",
						Description:  "Digits after the decimal point of DECIMAL columns (0-38); a number with more digits fails the sync",
						PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}},
						Default:      defaultDecimalScale,
						Order:        intPtr(2),
					},
				},
			},
		},
	}
}

func textCompressionSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Compression",
		Description:  "none or gzip; gzip appends .gz to the key",
		PropertyType: stringType(),
		Default:      compressionNone,
		Examples:     []string{compressionNone, compressionGzip},
		Order:        intPtr(0),
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
