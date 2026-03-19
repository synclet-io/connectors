package main

import airbyte "github.com/saturn4er/airbyte-go-sdk"

const booleanType airbyte.PropType = "boolean"

// bigqueryConnectionSpec returns the full BigQuery connection specification
// matching the Airbyte reference BigquerySpecification.
func bigqueryConnectionSpec() airbyte.ConnectionSpecification {
	return airbyte.ConnectionSpecification{
		Title:       "BigQuery Destination Spec",
		Description: "Writes data to a Google BigQuery dataset",
		Type:        "object",
		Required:    []airbyte.PropertyName{"project_id", "dataset_id"},
		Properties: airbyte.Properties{
			Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
				"project_id": {
					Title:       "Project ID",
					Description: "Your Google Cloud project ID",
					PropertyType: airbyte.PropertyType{
						Type: []airbyte.PropType{airbyte.String},
					},
					Order: intPtr(0),
				},
				"dataset_location": {
					Title:       "Dataset Location",
					Description: "The location of the dataset. Supported values: US, EU, us-central1, us-east1, us-west1, europe-west1, europe-west2, asia-east1, asia-northeast1, asia-southeast1, australia-southeast1. Warning: Changes to this after initial setup may cause data loss.",
					PropertyType: airbyte.PropertyType{
						Type: []airbyte.PropType{airbyte.String},
					},
					Default:  "US",
					Examples: []string{"US", "EU", "us-central1", "us-east1", "us-west1", "europe-west1", "europe-west2", "asia-east1", "asia-northeast1", "asia-southeast1", "australia-southeast1"},
					Order:    intPtr(1),
				},
				"dataset_id": {
					Title:       "Default Dataset ID",
					Description: "The default BigQuery dataset ID for writing data. Created if it does not exist.",
					PropertyType: airbyte.PropertyType{
						Type: []airbyte.PropType{airbyte.String},
					},
					Order: intPtr(2),
				},
				"loading_method": loadingMethodSpec(),
				"credentials":    credentialsSpec(),
				"credentials_json": {
					Title:       "Service Account Key JSON (Alternative)",
					Description: "The contents of the JSON service account key. Alternative to the credentials field.",
					PropertyType: airbyte.PropertyType{
						Type: []airbyte.PropType{airbyte.String},
					},
					IsSecret: true,
					Order:    intPtr(5),
				},
				"disable_type_dedupe": {
					Title:       "Disable Typed Tables and Deduplication",
					Description: "Disable the creation of typed tables and deduplication of data. This can be useful for debugging or performance testing.",
					PropertyType: airbyte.PropertyType{
						Type: []airbyte.PropType{booleanType},
					},
					Default: false,
					Order:   intPtr(6),
				},
				"raw_data_dataset": {
					Title:       "Raw Table Dataset",
					Description: "The dataset to write raw tables into. Defaults to airbyte_internal.",
					PropertyType: airbyte.PropertyType{
						Type: []airbyte.PropType{airbyte.String},
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
func loadingMethodSpec() airbyte.PropertySpec {
	return airbyte.PropertySpec{
		Title:       "Loading Method",
		Description: "The method to use for loading data into BigQuery",
		PropertyType: airbyte.PropertyType{
			Type: []airbyte.PropType{airbyte.Object},
		},
		Order: intPtr(3),
		OneOf: []airbyte.PropertySpec{
			{
				Title:       "Standard Inserts",
				Description: "Direct loading using BigQuery standard inserts",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"method"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"method": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "Standard",
					},
				},
			},
			{
				Title:       "GCS Staging",
				Description: "Staging data in Google Cloud Storage before loading into BigQuery",
				PropertyType: airbyte.PropertyType{
					Type: []airbyte.PropType{airbyte.Object},
				},
				Required: []airbyte.PropertyName{"method", "gcs_bucket_name", "credential"},
				Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
					"method": {
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Const: "GCS Staging",
					},
					"credential": {
						Title:       "GCS Credential",
						Description: "HMAC key credential for accessing GCS bucket",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.Object},
						},
						Required: []airbyte.PropertyName{"credential_type", "hmac_key_access_id", "hmac_key_secret"},
						Properties: map[airbyte.PropertyName]airbyte.PropertySpec{
							"credential_type": {
								PropertyType: airbyte.PropertyType{
									Type: []airbyte.PropType{airbyte.String},
								},
								Const: "HMAC_KEY",
							},
							"hmac_key_access_id": {
								Title:       "HMAC Key Access ID",
								Description: "HMAC key access ID for the GCS bucket",
								PropertyType: airbyte.PropertyType{
									Type: []airbyte.PropType{airbyte.String},
								},
								IsSecret: true,
							},
							"hmac_key_secret": {
								Title:       "HMAC Key Secret",
								Description: "HMAC key secret for the GCS bucket",
								PropertyType: airbyte.PropertyType{
									Type: []airbyte.PropType{airbyte.String},
								},
								IsSecret: true,
							},
						},
					},
					"gcs_bucket_name": {
						Title:       "GCS Bucket Name",
						Description: "The name of the GCS bucket for staging",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
					},
					"gcs_bucket_path": {
						Title:       "GCS Bucket Path",
						Description: "Directory within the GCS bucket for staging files",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
						},
						Default: "",
					},
					"keep_files_in_gcs-bucket": {
						Title:       "Keep Files in GCS Bucket",
						Description: "Whether to keep staging files in the GCS bucket after loading. Options: 'Delete all tmp files from GCS' or 'Keep all tmp files in GCS'",
						PropertyType: airbyte.PropertyType{
							Type: []airbyte.PropType{airbyte.String},
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
func cdcDeletionModeSpec() airbyte.PropertySpec {
	return airbyte.PropertySpec{
		Title:       "CDC Deletion Mode",
		Description: "Determines how deletions are handled in CDC mode. Options: hard_delete (permanently remove rows) or soft_delete (mark rows as deleted).",
		PropertyType: airbyte.PropertyType{
			Type: []airbyte.PropType{airbyte.String},
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
