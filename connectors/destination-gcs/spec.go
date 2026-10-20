package main

import synclet "github.com/synclet-io/synclet-go-sdk"

func gcsConnectionSpec() synclet.ConnectionSpecification {
	return synclet.ConnectionSpecification{
		Title:       "GCS Destination Spec",
		Description: "Writes streams as JSONL, CSV or Parquet files into a Google Cloud Storage bucket",
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
					Description:  "Object name prefix every object is written under, without leading or trailing slash",
					PropertyType: stringType(),
					Examples:     []string{"synclet", "exports/raw"},
					Order:        intPtr(1),
				},
				"credentials": credentialsSpec(),
				"format":      formatSpec(),
				"path_template": {
					Title: "Path Template",
					Description: "Object name below bucket_path. Tokens: {namespace}, {stream}, {yyyy}, {mm}, {dd}, {hh} (sync start, UTC), " +
						"{field:<name>} (a top-level record field; null or missing is __null__, an empty string __empty__) and {part} " +
						"(required in the last segment). {stream} is required in a directory segment, and only {namespace} and {stream} " +
						"may come before the end of that segment. Empty segments are dropped; the format's extension is appended.",
					PropertyType: stringType(),
					Default:      defaultPathTemplate,
					Examples:     []string{defaultPathTemplate, "{stream}/country={field:country}/{yyyy}/{mm}/{dd}/{part}"},
					Order:        intPtr(4),
				},
				"file_size_mb": {
					Title:        "File Size (MB)",
					Description:  "A file is uploaded and a new one started once this many megabytes are written before compression",
					PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}},
					Default:      defaultFileSizeMB,
					Order:        intPtr(5),
				},
				"max_records_per_file": {
					Title:        "Max Records per File",
					Description:  "A file is uploaded and a new one started once it holds this many records; 0 is no limit",
					PropertyType: synclet.PropertyType{Type: []synclet.PropType{synclet.Integer}},
					Default:      0,
					Order:        intPtr(6),
				},
			},
		},
	}
}

func credentialsSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Authentication",
		Description:  "How the connector authenticates to Cloud Storage",
		PropertyType: objectType(),
		Order:        intPtr(2),
		OneOf: []synclet.PropertySpec{
			{
				Title:        "Service Account Key",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"auth_type", "service_account_info"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: stringType(), Const: authServiceAccount},
					"service_account_info": {
						Title:        "Service Account Key JSON",
						Description:  "The JSON key of a service account with roles/storage.objectAdmin on the bucket",
						PropertyType: stringType(),
						IsSecret:     true,
						Order:        intPtr(0),
					},
				},
			},
			{
				Title:        "Application Default Credentials",
				Description:  "GOOGLE_APPLICATION_CREDENTIALS, workload identity or the metadata server of the machine running the connector",
				PropertyType: objectType(),
				Required:     []synclet.PropertyName{"auth_type"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"auth_type": {PropertyType: stringType(), Const: authApplicationDefault},
				},
			},
		},
	}
}

func formatSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:        "Output Format",
		PropertyType: objectType(),
		Order:        intPtr(3),
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
		Description:  "none or gzip; gzip appends .gz to the object name",
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
