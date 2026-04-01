package main

import (
	"encoding/json"
	"fmt"
)

// Config represents the BigQuery destination connector configuration.
type Config struct {
	ProjectID         string            `json:"project_id"`
	DatasetLocation   string            `json:"dataset_location"`
	DatasetID         string            `json:"dataset_id"`
	LoadingMethod     json.RawMessage   `json:"loading_method"`
	Credentials       CredentialsConfig `json:"credentials"`
	CredentialsJSON   string            `json:"credentials_json"`
	CDCDeletionMode   string            `json:"cdc_deletion_mode"`
	DisableTypeDedupe bool              `json:"disable_type_dedupe"`
	RawDataDataset    string            `json:"raw_data_dataset"`
}

// CredentialsConfig holds authentication configuration.
// AuthType determines which fields are used:
//   - "Client": OAuth (ClientID, ClientSecret, RefreshToken)
//   - "Service": Service Account (ServiceAccountInfo JSON key)
//   - "ApplicationDefault": GCP ADC (no extra fields)
type CredentialsConfig struct {
	AuthType           string `json:"auth_type"`
	ClientID           string `json:"client_id"`
	ClientSecret       string `json:"client_secret"`
	RefreshToken       string `json:"refresh_token"`
	ServiceAccountInfo string `json:"service_account_info"`
}

// LoadingMethodStandard represents the standard (non-GCS) loading method.
type LoadingMethodStandard struct {
	Method string `json:"method"` // "Standard"
}

// LoadingMethodGCS represents GCS staging loading method.
type LoadingMethodGCS struct {
	Method        string        `json:"method"` // "GCS Staging"
	Credential    GCSCredential `json:"credential"`
	GCSBucketName string        `json:"gcs_bucket_name"`
	GCSBucketPath string        `json:"gcs_bucket_path"`
	KeepFiles     string        `json:"keep_files_in_gcs-bucket"`
}

// GCSCredential holds HMAC key credentials for GCS access.
type GCSCredential struct {
	CredentialType  string `json:"credential_type"` // "HMAC_KEY"
	HMACKeyAccessID string `json:"hmac_key_access_id"`
	HMACKeySecret   string `json:"hmac_key_secret"`
}

// UnmarshalJSON handles default values for BigQuery config fields.
func (c *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	raw := &struct {
		*Alias
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, raw); err != nil {
		return err
	}

	if c.RawDataDataset == "" {
		c.RawDataDataset = "airbyte_internal"
	}
	if c.DatasetLocation == "" {
		c.DatasetLocation = "US"
	}
	if c.CDCDeletionMode == "" {
		c.CDCDeletionMode = "hard_delete"
	}

	return nil
}

// loadingMethod parses the raw loading method JSON into the appropriate typed struct.
// Returns a LoadingMethodStandard or LoadingMethodGCS depending on the "method" field.
func (c *Config) loadingMethod() (interface{}, error) {
	if len(c.LoadingMethod) == 0 {
		return &LoadingMethodStandard{Method: "Standard"}, nil
	}

	var probe struct {
		Method string `json:"method"`
	}
	if err := json.Unmarshal(c.LoadingMethod, &probe); err != nil {
		return nil, fmt.Errorf("parsing loading_method: %w", err)
	}

	switch probe.Method {
	case "GCS Staging":
		var gcs LoadingMethodGCS
		if err := json.Unmarshal(c.LoadingMethod, &gcs); err != nil {
			return nil, fmt.Errorf("parsing GCS loading method: %w", err)
		}
		return &gcs, nil
	default:
		var std LoadingMethodStandard
		if err := json.Unmarshal(c.LoadingMethod, &std); err != nil {
			return nil, fmt.Errorf("parsing standard loading method: %w", err)
		}

		return &std, nil
	}
}
