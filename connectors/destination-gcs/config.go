package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	authServiceAccount     = "service_account"
	authApplicationDefault = "application_default"

	formatJSONL   = "jsonl"
	formatCSV     = "csv"
	formatParquet = "parquet"

	compressionNone   = "none"
	compressionGzip   = "gzip"
	compressionSnappy = "snappy"
	compressionZstd   = "zstd"

	numberTypeDecimal = "decimal"
	numberTypeString  = "string"

	defaultPathTemplate  = "{namespace}/{stream}/{yyyy}_{mm}_{dd}/{part}"
	defaultFileSizeMB    = 100
	defaultDecimalScale  = 9
	maxDecimalPrecision  = 38
	maxFileSizeMB        = 4096
	maxOpenFilesPerWrite = 64
)

// Config is the destination-gcs configuration.
type Config struct {
	Bucket            string      `json:"bucket"`
	BucketPath        string      `json:"bucket_path"`
	Credentials       Credentials `json:"credentials"`
	Format            Format      `json:"format"`
	PathTemplate      string      `json:"path_template"`
	FileSizeMB        int         `json:"file_size_mb"`
	MaxRecordsPerFile int         `json:"max_records_per_file"`
}

// Credentials selects how the connector authenticates: a service account
// JSON key, or Application Default Credentials.
type Credentials struct {
	AuthType           string `json:"auth_type"`
	ServiceAccountInfo string `json:"service_account_info"`
}

// Format is the output file format and its options.
type Format struct {
	FormatType        string `json:"format_type"`
	Compression       string `json:"compression"`
	ParquetNumberType string `json:"parquet_number_type"`
	DecimalScale      *int   `json:"decimal_scale"`
}

// UnmarshalJSON fills the defaults of every optional field.
func (c *Config) UnmarshalJSON(data []byte) error {
	type alias Config
	if err := json.Unmarshal(data, (*alias)(c)); err != nil {
		return err
	}
	if c.PathTemplate == "" {
		c.PathTemplate = defaultPathTemplate
	}
	if c.FileSizeMB == 0 {
		c.FileSizeMB = defaultFileSizeMB
	}
	if c.Credentials.AuthType == "" {
		c.Credentials.AuthType = authServiceAccount
	}
	f := &c.Format
	if f.FormatType == "" {
		f.FormatType = formatJSONL
	}
	if f.Compression == "" {
		if f.FormatType == formatParquet {
			f.Compression = compressionSnappy
		} else {
			f.Compression = compressionNone
		}
	}
	if f.ParquetNumberType == "" {
		f.ParquetNumberType = numberTypeDecimal
	}
	if f.DecimalScale == nil {
		scale := defaultDecimalScale
		f.DecimalScale = &scale
	}
	return nil
}

// validate reports the first setting that cannot work.
func (c *Config) validate() error {
	if c.Bucket == "" {
		return errors.New("bucket is required")
	}
	switch c.Credentials.AuthType {
	case authServiceAccount:
		if strings.TrimSpace(c.Credentials.ServiceAccountInfo) == "" {
			return errors.New("credentials: service_account_info is required")
		}
		if !json.Valid([]byte(c.Credentials.ServiceAccountInfo)) {
			return errors.New("credentials: service_account_info is not valid JSON")
		}
	case authApplicationDefault:
	default:
		return fmt.Errorf("credentials: unsupported auth_type %q", c.Credentials.AuthType)
	}
	if err := c.Format.validate(); err != nil {
		return fmt.Errorf("format: %w", err)
	}
	if c.FileSizeMB < 1 || c.FileSizeMB > maxFileSizeMB {
		return fmt.Errorf("file_size_mb must be between 1 and %d, got %d", maxFileSizeMB, c.FileSizeMB)
	}
	if c.MaxRecordsPerFile < 0 {
		return fmt.Errorf("max_records_per_file must not be negative, got %d", c.MaxRecordsPerFile)
	}
	if _, err := parsePathTemplate(c.PathTemplate); err != nil {
		return fmt.Errorf("path_template: %w", err)
	}
	return nil
}

func (f *Format) validate() error {
	var allowed []string
	switch f.FormatType {
	case formatJSONL, formatCSV:
		allowed = []string{compressionNone, compressionGzip}
	case formatParquet:
		allowed = []string{compressionSnappy, compressionZstd, compressionGzip, compressionNone}
		if f.ParquetNumberType != numberTypeDecimal && f.ParquetNumberType != numberTypeString {
			return fmt.Errorf("parquet_number_type must be %q or %q, got %q", numberTypeDecimal, numberTypeString, f.ParquetNumberType)
		}
		if *f.DecimalScale < 0 || *f.DecimalScale > maxDecimalPrecision {
			return fmt.Errorf("decimal_scale must be between 0 and %d, got %d", maxDecimalPrecision, *f.DecimalScale)
		}
	default:
		return fmt.Errorf("unsupported format_type %q", f.FormatType)
	}
	for _, a := range allowed {
		if f.Compression == a {
			return nil
		}
	}
	return fmt.Errorf("compression of %s must be one of %s, got %q", f.FormatType, strings.Join(allowed, ", "), f.Compression)
}

// keyPrefix is bucket_path without leading and trailing slashes.
func (c *Config) keyPrefix() string {
	return strings.Trim(c.BucketPath, "/")
}

// extension is the object key suffix of the configured format.
func (f *Format) extension() string {
	ext := "." + f.FormatType
	if f.FormatType != formatParquet && f.Compression == compressionGzip {
		ext += ".gz"
	}
	return ext
}
