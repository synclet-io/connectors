package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"unicode/utf8"

	"github.com/bmatcuk/doublestar/v4"
)

// Config is the connector configuration.
type Config struct {
	Bucket      string         `json:"bucket"`
	Credentials Credentials    `json:"credentials"`
	Streams     []StreamConfig `json:"streams"`
}

// Credentials is how the connector authenticates to Google Cloud: a service
// account JSON key, or Application Default Credentials.
type Credentials struct {
	AuthType           string `json:"auth_type"`
	ServiceAccountInfo string `json:"service_account_info"`
}

// StreamConfig is one stream: the objects of the bucket whose key matches
// Glob, read in Format.
type StreamConfig struct {
	Name              string `json:"name"`
	Glob              string `json:"glob"`
	Format            Format `json:"format"`
	Compression       string `json:"compression"`
	Schema            string `json:"schema"`
	SchemaSampleFiles int    `json:"schema_sample_files"`
	SkipBadRows       bool   `json:"skip_bad_rows"`
}

// Format is the file format of a stream with its options. The delimited
// options apply to csv and tsv only.
type Format struct {
	Filetype    string    `json:"filetype"`
	Delimiter   *string   `json:"delimiter"`
	QuoteChar   *string   `json:"quote_char"`
	EscapeChar  string    `json:"escape_char"`
	Header      *bool     `json:"header"`
	ColumnNames []string  `json:"column_names"`
	SkipRows    int       `json:"skip_rows"`
	NullValues  *[]string `json:"null_values"`
}

const (
	authService            = "Service"
	authApplicationDefault = "ApplicationDefault"

	formatCSV     = "csv"
	formatTSV     = "tsv"
	formatJSONL   = "jsonl"
	formatParquet = "parquet"

	compressionAuto = "auto"
	compressionNone = "none"
	compressionGzip = "gzip"
	compressionZstd = "zstd"

	defaultSampleFiles = 10
)

var (
	authTypes    = []string{authService, authApplicationDefault}
	formats      = []string{formatCSV, formatTSV, formatJSONL, formatParquet}
	compressions = []string{compressionAuto, compressionNone, compressionGzip, compressionZstd}
)

func ptr[T any](v T) *T { return &v }

func (c *Config) applyDefaults() {
	if c.Credentials.AuthType == "" {
		c.Credentials.AuthType = authApplicationDefault
	}
	for i := range c.Streams {
		s := &c.Streams[i]
		if s.Compression == "" {
			s.Compression = compressionAuto
		}
		if s.SchemaSampleFiles == 0 {
			s.SchemaSampleFiles = defaultSampleFiles
		}
		f := &s.Format
		switch f.Filetype {
		case formatCSV:
			if f.Delimiter == nil {
				f.Delimiter = ptr(",")
			}
			if f.QuoteChar == nil {
				f.QuoteChar = ptr(`"`)
			}
		case formatTSV:
			if f.Delimiter == nil {
				f.Delimiter = ptr("\t")
			}
			if f.QuoteChar == nil {
				f.QuoteChar = ptr("")
			}
		}
		if f.Header == nil {
			f.Header = ptr(true)
		}
		if f.NullValues == nil {
			f.NullValues = &[]string{""}
		}
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.Bucket == "" {
		errs = append(errs, errors.New("bucket is required"))
	}
	switch cr := c.Credentials; cr.AuthType {
	case authService:
		if err := validServiceAccountKey(cr.ServiceAccountInfo); err != nil {
			errs = append(errs, err)
		}
	case authApplicationDefault:
	default:
		errs = append(errs, fmt.Errorf("credentials auth_type %q is not one of %v", cr.AuthType, authTypes))
	}
	if len(c.Streams) == 0 {
		errs = append(errs, errors.New("streams needs at least one stream"))
	}
	names := map[string]bool{}
	for i, s := range c.Streams {
		if err := s.validate(); err != nil {
			errs = append(errs, fmt.Errorf("streams[%d]: %w", i, err))
		}
		if names[s.Name] {
			errs = append(errs, fmt.Errorf("streams[%d]: name %q is used twice", i, s.Name))
		}
		names[s.Name] = true
	}
	return errors.Join(errs...)
}

func (s StreamConfig) validate() error {
	var errs []error
	if s.Name == "" {
		errs = append(errs, errors.New("name is required"))
	}
	switch {
	case s.Glob == "":
		errs = append(errs, errors.New("glob is required"))
	case !doublestar.ValidatePattern(s.Glob):
		errs = append(errs, fmt.Errorf("glob %q is not a valid pattern", s.Glob))
	}
	if !slices.Contains(compressions, s.Compression) {
		errs = append(errs, fmt.Errorf("compression %q is not one of %v", s.Compression, compressions))
	}
	if s.SchemaSampleFiles < 1 {
		errs = append(errs, fmt.Errorf("schema_sample_files %d must be at least 1", s.SchemaSampleFiles))
	}
	if s.Schema != "" {
		if _, err := parseSchema(json.RawMessage(s.Schema)); err != nil {
			errs = append(errs, fmt.Errorf("schema: %w", err))
		}
	}
	f := s.Format
	switch f.Filetype {
	case formatCSV, formatTSV:
		if utf8.RuneCountInString(*f.Delimiter) != 1 {
			errs = append(errs, fmt.Errorf("delimiter %q must be one character", *f.Delimiter))
		}
		if utf8.RuneCountInString(*f.QuoteChar) > 1 {
			errs = append(errs, fmt.Errorf("quote_char %q must be one character or empty", *f.QuoteChar))
		}
		if utf8.RuneCountInString(f.EscapeChar) > 1 {
			errs = append(errs, fmt.Errorf("escape_char %q must be one character or empty", f.EscapeChar))
		}
		if *f.Delimiter == *f.QuoteChar || (f.EscapeChar != "" && f.EscapeChar == *f.Delimiter) {
			errs = append(errs, errors.New("delimiter, quote_char and escape_char must differ"))
		}
		if *f.Delimiter == "\n" || *f.Delimiter == "\r" {
			errs = append(errs, errors.New("delimiter cannot be a line break"))
		}
		if !*f.Header && len(f.ColumnNames) == 0 {
			errs = append(errs, errors.New("header false needs column_names"))
		}
		if f.SkipRows < 0 {
			errs = append(errs, fmt.Errorf("skip_rows %d must not be negative", f.SkipRows))
		}
		seen := map[string]bool{}
		for _, n := range f.ColumnNames {
			if n == "" || seen[n] {
				errs = append(errs, fmt.Errorf("column_names must be unique and non-empty, got %q", n))
			}
			seen[n] = true
		}
	case formatJSONL, formatParquet:
	default:
		errs = append(errs, fmt.Errorf("format filetype %q is not one of %v", f.Filetype, formats))
	}
	return errors.Join(errs...)
}

// validServiceAccountKey reports whether info is the JSON key of a service
// account, as the Google Cloud console downloads it.
func validServiceAccountKey(info string) error {
	if info == "" {
		return errors.New("service_account_info is required for auth_type Service")
	}
	var key struct {
		Type        string `json:"type"`
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
	}
	if err := json.Unmarshal([]byte(info), &key); err != nil {
		return fmt.Errorf("service_account_info is not a JSON service account key: %w", err)
	}
	if key.Type != "service_account" {
		return fmt.Errorf("service_account_info has type %q, a service account key has type service_account", key.Type)
	}
	if key.ClientEmail == "" || key.PrivateKey == "" {
		return errors.New("service_account_info needs client_email and private_key")
	}
	return nil
}
