package main

import (
	"encoding/json"
	"regexp"
	"strconv"
)

// Config represents the Google Sheets source connector configuration.
type Config struct {
	SpreadsheetID   string            `json:"spreadsheet_id"`
	Credentials     CredentialsConfig `json:"credentials"`
	BatchSize       int               `json:"-"` // custom unmarshal: string or number
	NamesConversion bool              `json:"names_conversion"`
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

const defaultBatchSize = 1000000

// UnmarshalJSON handles batch_size as string-or-number and defaults to 1000000.
func (c *Config) UnmarshalJSON(data []byte) error {
	type Alias Config
	raw := &struct {
		*Alias
		BatchSize json.RawMessage `json:"batch_size"`
	}{
		Alias: (*Alias)(c),
	}

	if err := json.Unmarshal(data, raw); err != nil {
		return err
	}

	c.BatchSize = parseBatchSize(raw.BatchSize)
	return nil
}

// parseBatchSize parses a JSON value that may be a number or string, defaulting to defaultBatchSize.
func parseBatchSize(raw json.RawMessage) int {
	if len(raw) == 0 {
		return defaultBatchSize
	}

	// Try as number first
	var n int
	if err := json.Unmarshal(raw, &n); err == nil {
		return n
	}

	// Try as string
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, err := strconv.Atoi(s); err == nil {
			return v
		}
	}

	return defaultBatchSize
}

// spreadsheetIDRegex extracts the spreadsheet ID from a full Google Sheets URL.
// Matches IDs that are at least 20 characters of word characters and hyphens.
var spreadsheetIDRegex = regexp.MustCompile(`/d/([-\w]+)`)

// parseSpreadsheetID extracts the spreadsheet ID from a full URL or returns the raw value.
func parseSpreadsheetID(raw string) string {
	matches := spreadsheetIDRegex.FindStringSubmatch(raw)
	if len(matches) >= 2 {
		return matches[1]
	}
	return raw
}
