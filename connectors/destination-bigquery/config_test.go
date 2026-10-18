package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigParsing(t *testing.T) {
	raw := `{
		"project_id": "my-project",
		"dataset_location": "EU",
		"dataset_id": "my_dataset",
		"loading_method": {"method": "Standard"},
		"credentials": {"auth_type": "ApplicationDefault"},
		"cdc_deletion_mode": "soft_delete",
		"disable_type_dedupe": true,
		"raw_data_dataset": "custom_raw"
	}`

	var cfg Config
	err := json.Unmarshal([]byte(raw), &cfg)
	require.NoError(t, err)

	assert.Equal(t, "my-project", cfg.ProjectID)
	assert.Equal(t, "EU", cfg.DatasetLocation)
	assert.Equal(t, "my_dataset", cfg.DatasetID)
	assert.Equal(t, "ApplicationDefault", cfg.Credentials.AuthType)
	assert.Equal(t, "soft_delete", cfg.CDCDeletionMode)
	assert.True(t, cfg.DisableTypeDedupe)
	assert.Equal(t, "custom_raw", cfg.RawDataDataset)
	assert.NotEmpty(t, cfg.LoadingMethod)
}

func TestConfigDefaults(t *testing.T) {
	raw := `{
		"project_id": "p",
		"dataset_id": "d",
		"credentials": {"auth_type": "ApplicationDefault"}
	}`

	var cfg Config
	err := json.Unmarshal([]byte(raw), &cfg)
	require.NoError(t, err)

	assert.Equal(t, "airbyte_internal", cfg.RawDataDataset)
	assert.Equal(t, "US", cfg.DatasetLocation)
	assert.Equal(t, "hard_delete", cfg.CDCDeletionMode)
	assert.False(t, cfg.DisableTypeDedupe)
}

func TestConfigLoadingMethodStandard(t *testing.T) {
	raw := `{
		"project_id": "p",
		"dataset_id": "d",
		"credentials": {"auth_type": "ApplicationDefault"},
		"loading_method": {"method": "Standard"}
	}`

	var cfg Config
	err := json.Unmarshal([]byte(raw), &cfg)
	require.NoError(t, err)

	lm, err := cfg.loadingMethod()
	require.NoError(t, err)

	std, ok := lm.(*LoadingMethodStandard)
	require.True(t, ok, "expected *LoadingMethodStandard")
	assert.Equal(t, "Standard", std.Method)
}

func TestConfigLoadingMethodStandardDefault(t *testing.T) {
	// When loading_method is omitted, default to Standard.
	raw := `{
		"project_id": "p",
		"dataset_id": "d",
		"credentials": {"auth_type": "ApplicationDefault"}
	}`

	var cfg Config
	err := json.Unmarshal([]byte(raw), &cfg)
	require.NoError(t, err)

	lm, err := cfg.loadingMethod()
	require.NoError(t, err)

	std, ok := lm.(*LoadingMethodStandard)
	require.True(t, ok, "expected *LoadingMethodStandard when omitted")
	assert.Equal(t, "Standard", std.Method)
}

func TestConfigLoadingMethodGCS(t *testing.T) {
	raw := `{
		"project_id": "p",
		"dataset_id": "d",
		"credentials": {"auth_type": "ApplicationDefault"},
		"loading_method": {
			"method": "GCS Staging",
			"credential": {
				"credential_type": "HMAC_KEY",
				"hmac_key_access_id": "access123",
				"hmac_key_secret": "secret456"
			},
			"gcs_bucket_name": "my-bucket",
			"gcs_bucket_path": "staging/data",
			"keep_files_in_gcs-bucket": "Delete all tmp files from GCS"
		}
	}`

	var cfg Config
	err := json.Unmarshal([]byte(raw), &cfg)
	require.NoError(t, err)

	lm, err := cfg.loadingMethod()
	require.NoError(t, err)

	gcs, ok := lm.(*LoadingMethodGCS)
	require.True(t, ok, "expected *LoadingMethodGCS")
	assert.Equal(t, "GCS Staging", gcs.Method)
	assert.Equal(t, "my-bucket", gcs.GCSBucketName)
	assert.Equal(t, "staging/data", gcs.GCSBucketPath)
	assert.Equal(t, "Delete all tmp files from GCS", gcs.KeepFiles)
	assert.Equal(t, "HMAC_KEY", gcs.Credential.CredentialType)
	assert.Equal(t, "access123", gcs.Credential.HMACKeyAccessID)
	assert.Equal(t, "secret456", gcs.Credential.HMACKeySecret)
}

func TestConfigCredentialsJSON(t *testing.T) {
	raw := `{
		"project_id": "p",
		"dataset_id": "d",
		"credentials": {"auth_type": "ApplicationDefault"},
		"credentials_json": "{\"type\":\"service_account\",\"project_id\":\"test\"}"
	}`

	var cfg Config
	err := json.Unmarshal([]byte(raw), &cfg)
	require.NoError(t, err)

	assert.Equal(t, "{\"type\":\"service_account\",\"project_id\":\"test\"}", cfg.CredentialsJSON)
}
