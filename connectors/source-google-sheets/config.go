package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const (
	authClient             = "Client"
	authService            = "Service"
	authApplicationDefault = "ApplicationDefault"

	columnNamingLegacy = "legacy"
	columnNamingStable = "stable"

	valueRenderFormatted   = "formatted"
	valueRenderUnformatted = "unformatted"

	defaultBatchSize         = 10000
	defaultRequestsPerMinute = 60
)

// Config is the Google Sheets source configuration. Every key besides
// spreadsheet_id, credentials, batch_size and names_conversion is optional,
// and its default keeps the stream names, column names and types of a
// configuration that does not set it.
type Config struct {
	SpreadsheetID            string            `json:"spreadsheet_id"`
	Credentials              CredentialsConfig `json:"credentials"`
	BatchSize                int               `json:"-"`
	NamesConversion          bool              `json:"-"`
	ColumnNaming             string            `json:"column_naming,omitempty"`
	ValueRender              string            `json:"value_render,omitempty"`
	Incremental              bool              `json:"incremental,omitempty"`
	AdditionalSpreadsheetIDs []string          `json:"additional_spreadsheet_ids,omitempty"`
	RequestsPerMinute        *int              `json:"requests_per_minute,omitempty"`
}

// CredentialsConfig holds one of three ways to authenticate, chosen by
// AuthType:
//   - "Client": an OAuth client with a refresh token of a Google user;
//   - "Service": a Google credential JSON, usually a service account key;
//   - "ApplicationDefault": Google Application Default Credentials.
type CredentialsConfig struct {
	AuthType           string `json:"auth_type"`
	ClientID           string `json:"client_id,omitempty"`
	ClientSecret       string `json:"client_secret,omitempty"`
	RefreshToken       string `json:"refresh_token,omitempty"`
	ServiceAccountInfo string `json:"service_account_info,omitempty"`
}

// UnmarshalJSON reads batch_size as a number or a numeric string and
// names_conversion as a boolean or "true"/"false".
func (c *Config) UnmarshalJSON(data []byte) error {
	type alias Config
	raw := &struct {
		*alias
		BatchSize       json.RawMessage `json:"batch_size"`
		NamesConversion json.RawMessage `json:"names_conversion"`
	}{alias: (*alias)(c)}
	if err := json.Unmarshal(data, raw); err != nil {
		return err
	}
	c.BatchSize = parseBatchSize(raw.BatchSize)
	nc, err := parseBool("names_conversion", raw.NamesConversion)
	if err != nil {
		return err
	}
	c.NamesConversion = nc
	return nil
}

// MarshalJSON writes the configuration in the form UnmarshalJSON reads.
func (c Config) MarshalJSON() ([]byte, error) {
	type alias Config
	return json.Marshal(struct {
		alias
		BatchSize       int  `json:"batch_size,omitempty"`
		NamesConversion bool `json:"names_conversion,omitempty"`
	}{alias(c), c.BatchSize, c.NamesConversion})
}

// parseBatchSize reads a number or a numeric string; anything else, zero or
// a negative value is defaultBatchSize.
func parseBatchSize(raw json.RawMessage) int {
	var n int
	if err := json.Unmarshal(raw, &n); err == nil && n > 0 {
		return n
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && v > 0 {
			return v
		}
	}
	return defaultBatchSize
}

func parseBool(key string, raw json.RawMessage) (bool, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return false, nil
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		return b, nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		switch strings.ToLower(strings.TrimSpace(s)) {
		case "", "false":
			return false, nil
		case "true":
			return true, nil
		}
	}
	return false, fmt.Errorf("%s must be true or false, got %s", key, raw)
}

var spreadsheetIDRegex = regexp.MustCompile(`/d/([-\w]+)`)

// parseSpreadsheetID returns the ID of a spreadsheet URL, or raw as it is.
func parseSpreadsheetID(raw string) string {
	raw = strings.TrimSpace(raw)
	if m := spreadsheetIDRegex.FindStringSubmatch(raw); len(m) >= 2 {
		return m[1]
	}
	return raw
}

// normalize fills the defaults and turns spreadsheet URLs into IDs.
func (c *Config) normalize() {
	c.SpreadsheetID = parseSpreadsheetID(c.SpreadsheetID)
	ids := make([]string, len(c.AdditionalSpreadsheetIDs))
	for i, id := range c.AdditionalSpreadsheetIDs {
		ids[i] = parseSpreadsheetID(id)
	}
	c.AdditionalSpreadsheetIDs = ids
	if c.ColumnNaming == "" {
		c.ColumnNaming = columnNamingLegacy
	}
	if c.ValueRender == "" {
		c.ValueRender = valueRenderFormatted
	}
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
}

func (c Config) validate() error {
	var errs []error
	if c.SpreadsheetID == "" {
		errs = append(errs, errors.New("spreadsheet_id is empty"))
	}
	seen := map[string]bool{c.SpreadsheetID: true}
	for _, id := range c.AdditionalSpreadsheetIDs {
		switch {
		case id == "":
			errs = append(errs, errors.New("additional_spreadsheet_ids has an empty entry"))
		case seen[id]:
			errs = append(errs, fmt.Errorf("spreadsheet %s is configured twice", id))
		}
		seen[id] = true
	}
	switch c.ColumnNaming {
	case columnNamingLegacy, columnNamingStable:
	default:
		errs = append(errs, fmt.Errorf("column_naming must be %s or %s, got %q", columnNamingLegacy, columnNamingStable, c.ColumnNaming))
	}
	switch c.ValueRender {
	case valueRenderFormatted, valueRenderUnformatted:
	default:
		errs = append(errs, fmt.Errorf("value_render must be %s or %s, got %q", valueRenderFormatted, valueRenderUnformatted, c.ValueRender))
	}
	if c.RequestsPerMinute != nil && *c.RequestsPerMinute <= 0 {
		errs = append(errs, fmt.Errorf("requests_per_minute must be positive, got %d", *c.RequestsPerMinute))
	}
	switch c.Credentials.AuthType {
	case authClient, authService, authApplicationDefault:
	default:
		errs = append(errs, fmt.Errorf("unsupported auth_type: %q", c.Credentials.AuthType))
	}
	return errors.Join(errs...)
}

func (c Config) requestsPerMinute() int {
	if c.RequestsPerMinute == nil {
		return defaultRequestsPerMinute
	}
	return *c.RequestsPerMinute
}

// spreadsheetRef is a configured spreadsheet and the namespace of its
// streams: none for spreadsheet_id, the spreadsheet ID for each of
// additional_spreadsheet_ids.
type spreadsheetRef struct {
	id        string
	namespace string
}

func (c Config) spreadsheets() []spreadsheetRef {
	refs := []spreadsheetRef{{id: c.SpreadsheetID}}
	for _, id := range c.AdditionalSpreadsheetIDs {
		refs = append(refs, spreadsheetRef{id: id, namespace: id})
	}
	return refs
}
