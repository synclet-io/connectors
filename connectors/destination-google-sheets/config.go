package main

import (
	"encoding/json"
	"regexp"
)

// Config represents the Google Sheets destination connector configuration.
type Config struct {
	SpreadsheetID string            `json:"spreadsheet_id"`
	Credentials   CredentialsConfig `json:"credentials"`
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

// spreadsheetIDRegex extracts the spreadsheet ID from a full Google Sheets URL.
var spreadsheetIDRegex = regexp.MustCompile(`/d/([-\w]+)`)

// parseSpreadsheetID extracts the spreadsheet ID from a full URL or returns the raw value.
func parseSpreadsheetID(raw string) string {
	matches := spreadsheetIDRegex.FindStringSubmatch(raw)
	if len(matches) >= 2 {
		return matches[1]
	}
	return raw
}

// UnmarshalJSON handles spreadsheet_id extraction from URLs.
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

	c.SpreadsheetID = parseSpreadsheetID(c.SpreadsheetID)
	return nil
}
