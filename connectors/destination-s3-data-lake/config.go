package main

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

const (
	catalogREST = "rest"

	authAccessKey    = "access_key"
	authDefaultChain = "default_chain"
	authAssumeRole   = "assume_role"

	numberTypeDecimal = "decimal"
	numberTypeDouble  = "double"

	defaultRegion       = "us-east-1"
	defaultDecimalScale = 9
	maxDecimalPrecision = 38
)

// Config is the destination-s3-data-lake configuration.
type Config struct {
	Catalog      CatalogConfig `json:"catalog"`
	Namespace    string        `json:"namespace"`
	S3           S3Config      `json:"s3"`
	NumberType   string        `json:"number_type,omitempty"`
	DecimalScale *int          `json:"decimal_scale,omitempty"`
}

// CatalogConfig is how the connector reaches the Iceberg REST catalog.
type CatalogConfig struct {
	CatalogType string `json:"catalog_type"`
	URI         string `json:"uri"`
	Warehouse   string `json:"warehouse,omitempty"`
	Credential  string `json:"credential,omitempty"`
	Token       string `json:"token,omitempty"`
	Scope       string `json:"scope,omitempty"`
}

// S3Config is where the tables' files are written and how the connector
// authenticates to the bucket.
type S3Config struct {
	WarehouseLocation string      `json:"warehouse_location"`
	Region            string      `json:"region,omitempty"`
	Endpoint          string      `json:"endpoint,omitempty"`
	ForcePathStyle    bool        `json:"force_path_style,omitempty"`
	Credentials       Credentials `json:"credentials"`
}

// Credentials selects static keys, the AWS default credential chain, or a
// role assumed on top of the default chain.
type Credentials struct {
	AuthType        string `json:"auth_type"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	RoleARN         string `json:"role_arn,omitempty"`
	ExternalID      string `json:"external_id,omitempty"`
}

// applyDefaults fills every optional field left empty.
func (c *Config) applyDefaults() {
	if c.Catalog.CatalogType == "" {
		c.Catalog.CatalogType = catalogREST
	}
	if c.S3.Region == "" {
		c.S3.Region = defaultRegion
	}
	if c.S3.Credentials.AuthType == "" {
		c.S3.Credentials.AuthType = authAccessKey
	}
	if c.NumberType == "" {
		c.NumberType = numberTypeDecimal
	}
	if c.DecimalScale == nil {
		scale := defaultDecimalScale
		c.DecimalScale = &scale
	}
}

// validate reports the first setting that cannot work.
func (c *Config) validate() error {
	if c.Catalog.CatalogType != catalogREST {
		return fmt.Errorf("catalog: unsupported catalog_type %q; only %q is supported", c.Catalog.CatalogType, catalogREST)
	}
	if c.Catalog.URI == "" {
		return errors.New("catalog: uri is required")
	}
	u, err := url.Parse(c.Catalog.URI)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("catalog: uri %q is not an http or https URL", c.Catalog.URI)
	}
	if c.Catalog.Credential != "" && c.Catalog.Token != "" {
		return errors.New("catalog: set credential or token, not both")
	}
	if _, err := c.namespace(); err != nil {
		return err
	}
	if _, _, err := c.S3.bucketAndPrefix(); err != nil {
		return err
	}
	switch cr := c.S3.Credentials; cr.AuthType {
	case authAccessKey:
		if cr.AccessKeyID == "" || cr.SecretAccessKey == "" {
			return errors.New("s3.credentials: access_key_id and secret_access_key are required")
		}
	case authDefaultChain:
	case authAssumeRole:
		if cr.RoleARN == "" {
			return errors.New("s3.credentials: role_arn is required")
		}
	default:
		return fmt.Errorf("s3.credentials: unsupported auth_type %q", cr.AuthType)
	}
	if c.NumberType != numberTypeDecimal && c.NumberType != numberTypeDouble {
		return fmt.Errorf("number_type must be %q or %q, got %q", numberTypeDecimal, numberTypeDouble, c.NumberType)
	}
	if *c.DecimalScale < 0 || *c.DecimalScale > maxDecimalPrecision {
		return fmt.Errorf("decimal_scale must be between 0 and %d, got %d", maxDecimalPrecision, *c.DecimalScale)
	}
	return nil
}

// namespace splits the configured namespace into its levels at dots.
func (c *Config) namespace() ([]string, error) {
	if c.Namespace == "" {
		return nil, errors.New("namespace is required")
	}
	levels := strings.Split(c.Namespace, ".")
	for _, l := range levels {
		if l == "" {
			return nil, fmt.Errorf("namespace %q has an empty level", c.Namespace)
		}
	}
	return levels, nil
}

// bucketAndPrefix splits warehouse_location, s3://bucket[/prefix], into the
// bucket and the key prefix without leading or trailing slashes.
func (s *S3Config) bucketAndPrefix() (string, string, error) {
	rest, ok := strings.CutPrefix(s.WarehouseLocation, "s3://")
	if !ok {
		return "", "", fmt.Errorf("s3.warehouse_location %q must start with s3://", s.WarehouseLocation)
	}
	bucket, prefix, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", fmt.Errorf("s3.warehouse_location %q has no bucket", s.WarehouseLocation)
	}
	return bucket, strings.Trim(prefix, "/"), nil
}

// tableLocation is the root of a table's files:
// <warehouse_location>/<namespace levels>/<table>.
func (c *Config) tableLocation(namespace []string, table string) string {
	bucket, prefix, _ := c.S3.bucketAndPrefix()
	parts := []string{"s3://" + bucket}
	if prefix != "" {
		parts = append(parts, prefix)
	}
	parts = append(parts, namespace...)
	parts = append(parts, table)
	return strings.Join(parts, "/")
}
