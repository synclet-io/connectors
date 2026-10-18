package main

import synclet "github.com/synclet-io/synclet-go-sdk"

const booleanType synclet.PropType = "boolean"

// bigqueryConnectionSpec returns the full BigQuery connection specification
// matching the Airbyte reference BigquerySpecification.
func bigqueryConnectionSpec() synclet.ConnectionSpecification {
	return synclet.ConnectionSpecification{
		Title:       "BigQuery Destination Spec",
		Description: "Writes data to a Google BigQuery dataset",
		Type:        "object",
		Required:    []synclet.PropertyName{"project_id", "dataset_id"},
		Properties: synclet.Properties{
			Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"project_id": {
					Title:       "Project ID",
					Description: "Your Google Cloud project ID",
					PropertyType: synclet.PropertyType{
						Type: []synclet.PropType{synclet.String},
					},
					Order: intPtr(0),
				},
				"dataset_location": {
					Title:       "Dataset Location",
					Description: "The location of the dataset. Supported values: US, EU, us-central1, us-east1, us-west1, europe-west1, europe-west2, asia-east1, asia-northeast1, asia-southeast1, australia-southeast1. Warning: Changes to this after initial setup may cause data loss.",
					PropertyType: synclet.PropertyType{
						Type: []synclet.PropType{synclet.String},
					},
					Default:  "US",
					Examples: []string{"US", "EU", "us-central1", "us-east1", "us-west1", "europe-west1", "europe-west2", "asia-east1", "asia-northeast1", "asia-southeast1", "australia-southeast1"},
					Order:    intPtr(1),
				},
				"dataset_id": {
					Title:       "Default Dataset ID",
					Description: "The default BigQuery dataset ID for writing data. Created if it does not exist.",
					PropertyType: synclet.PropertyType{
						Type: []synclet.PropType{synclet.String},
					},
					Order: intPtr(2),
				},
				"loading_method": loadingMethodSpec(),
				"credentials":    credentialsSpec(),
				"credentials_json": {
					Title:       "Service Account Key JSON (Alternative)",
					Description: "The contents of the JSON service account key. Alternative to the credentials field.",
					PropertyType: synclet.PropertyType{
						Type: []synclet.PropType{synclet.String},
					},
					IsSecret: true,
					Order:    intPtr(5),
				},
				"disable_type_dedupe": {
					Title:       "Disable Typed Tables and Deduplication",
					Description: "Disable the creation of typed tables and deduplication of data. This can be useful for debugging or performance testing.",
					PropertyType: synclet.PropertyType{
						Type: []synclet.PropType{booleanType},
					},
					Default: false,
					Order:   intPtr(6),
				},
				"raw_data_dataset": {
					Title:       "Raw Table Dataset",
					Description: "The dataset to write raw tables into. Defaults to airbyte_internal.",
					PropertyType: synclet.PropertyType{
						Type: []synclet.PropType{synclet.String},
					},
					Default: "airbyte_internal",
					Order:   intPtr(7),
				},
				"cdc_deletion_mode": cdcDeletionModeSpec(),
			},
		},
	}
}

// loadingMethodSpec returns the oneOf spec for loading method selection.
func loadingMethodSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:       "Loading Method",
		Description: "The method to use for loading data into BigQuery",
		PropertyType: synclet.PropertyType{
			Type: []synclet.PropType{synclet.Object},
		},
		Order: intPtr(3),
		OneOf: []synclet.PropertySpec{
			{
				Title:       "Standard Inserts",
				Description: "Direct loading using BigQuery standard inserts",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"method"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"method": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "Standard",
					},
				},
			},
			{
				Title:       "GCS Staging",
				Description: "Staging data in Google Cloud Storage before loading into BigQuery",
				PropertyType: synclet.PropertyType{
					Type: []synclet.PropType{synclet.Object},
				},
				Required: []synclet.PropertyName{"method", "gcs_bucket_name", "credential"},
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"method": {
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Const: "GCS Staging",
					},
					"credential": {
						Title:       "GCS Credential",
						Description: "HMAC key credential for accessing GCS bucket",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.Object},
						},
						Required: []synclet.PropertyName{"credential_type", "hmac_key_access_id", "hmac_key_secret"},
						Properties: map[synclet.PropertyName]synclet.PropertySpec{
							"credential_type": {
								PropertyType: synclet.PropertyType{
									Type: []synclet.PropType{synclet.String},
								},
								Const: "HMAC_KEY",
							},
							"hmac_key_access_id": {
								Title:       "HMAC Key Access ID",
								Description: "HMAC key access ID for the GCS bucket",
								PropertyType: synclet.PropertyType{
									Type: []synclet.PropType{synclet.String},
								},
								IsSecret: true,
							},
							"hmac_key_secret": {
								Title:       "HMAC Key Secret",
								Description: "HMAC key secret for the GCS bucket",
								PropertyType: synclet.PropertyType{
									Type: []synclet.PropType{synclet.String},
								},
								IsSecret: true,
							},
						},
					},
					"gcs_bucket_name": {
						Title:       "GCS Bucket Name",
						Description: "The name of the GCS bucket for staging",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
					},
					"gcs_bucket_path": {
						Title:       "GCS Bucket Path",
						Description: "Directory within the GCS bucket for staging files",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Default: "",
					},
					"keep_files_in_gcs-bucket": {
						Title:       "Keep Files in GCS Bucket",
						Description: "Whether to keep staging files in the GCS bucket after loading. Options: 'Delete all tmp files from GCS' or 'Keep all tmp files in GCS'",
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{synclet.String},
						},
						Default:  "Delete all tmp files from GCS",
						Examples: []string{"Delete all tmp files from GCS", "Keep all tmp files in GCS"},
					},
				},
			},
		},
	}
}

// cdcDeletionModeSpec returns the spec for CDC deletion mode.
func cdcDeletionModeSpec() synclet.PropertySpec {
	return synclet.PropertySpec{
		Title:       "CDC Deletion Mode",
		Description: "Determines how deletions are handled in CDC mode. Options: hard_delete (permanently remove rows) or soft_delete (mark rows as deleted).",
		PropertyType: synclet.PropertyType{
			Type: []synclet.PropType{synclet.String},
		},
		Default:  "hard_delete",
		Examples: []string{"hard_delete", "soft_delete"},
		Order:    intPtr(8),
	}
}

// intPtr returns a pointer to an int (helper for optional Order fields).
func intPtr(i int) *int {
	return &i
}
